defmodule Bedrock.SystemKeys.Reader do
  @moduledoc """
  The shared readers for the durable `\\xFF/system` mapping families:
  paged range reads and the family decoders. Recovery's materializer
  bootstrap, the Distributor and the exclusion safety check all read
  their families the same way — one reader, so two consumers cannot
  disagree about what the bytes mean.

  Paging resumes each page immediately after the last returned key and
  drains the continuation to exhaustion: a truncated boundary map is not
  a degraded layout, it is a wrong one. Any failure mid-continuation
  fails the whole read — partial success would BE the silent truncation
  this exists to preclude — and an empty page claiming more is a broken
  read contract, surfaced rather than looped on.
  """

  alias Bedrock.Service.Worker
  alias Bedrock.SystemKeys
  alias Bedrock.SystemKeys.Values

  @type range_read_fn ::
          (Bedrock.key() ->
             {:ok, {[{Bedrock.key(), binary()}], more :: boolean()}}
             | {:error, term()}
             | {:failure, term(), term()})

  @doc "Reads a system family's entries to exhaustion, starting at `prefix`."
  @spec read_family(range_read_fn(), prefix :: Bedrock.key(), error_tag :: atom()) ::
          {:ok, [{Bedrock.key(), binary()}]} | {:error, {atom(), term()}}
  def read_family(range_read_fn, prefix, error_tag), do: page_entries(range_read_fn, prefix, error_tag, [])

  defp page_entries(range_read_fn, start_key, error_tag, pages) do
    case range_read_fn.(start_key) do
      {:ok, {entries, false}} ->
        {:ok, [entries | pages] |> Enum.reverse() |> Enum.concat()}

      {:ok, {[], true}} ->
        {:error, {error_tag, :empty_continuation_page}}

      {:ok, {entries, true}} ->
        {last_key, _value} = List.last(entries)
        page_entries(range_read_fn, Bedrock.Key.key_after(last_key), error_tag, [entries | pages])

      {:error, reason} ->
        {:error, {error_tag, reason}}

      {:failure, reason, _ref} ->
        {:error, {error_tag, reason}}
    end
  end

  @doc """
  Decodes `materializers/<tag>/<worker_id>` entries into the membership
  map `%{tag => %{worker_id => node}}` - a shard's members are a set, and
  absence of a key is absence of a member.

  A foreign key, or a member key with an undecodable value, fails the
  whole decode: reading a family we cannot account for as empty would
  re-recruit every shard and orphan the live ones.
  """
  @spec decode_materializer_members([{Bedrock.key(), binary()}]) ::
          {:ok, %{Bedrock.range_tag() => %{Worker.id() => String.t()}}}
          | {:error, {:invalid_materializer_entry, Bedrock.key()}}
  def decode_materializer_members(entries) do
    Enum.reduce_while(entries, {:ok, %{}}, fn {key, value}, {:ok, acc} ->
      case decode_member_entry(SystemKeys.parse_key(key), value) do
        {:ok, tag, worker_id, node} ->
          {:cont, {:ok, Map.update(acc, tag, %{worker_id => node}, &Map.put(&1, worker_id, node))}}

        :error ->
          {:halt, {:error, {:invalid_materializer_entry, key}}}
      end
    end)
  end

  defp decode_member_entry({:materializer_key, tag, worker_id}, value) do
    case Values.decode_materializer_node(value) do
      {:ok, node} -> {:ok, tag, worker_id, node}
      _ -> :error
    end
  end

  defp decode_member_entry(_not_a_member_key, _value), do: :error

  @doc """
  Decodes `logs/<generation>/<log_id>` entries into
  `%{current: %{log_id => node}, old: %{log_id => node}}` - the two
  vectors FDB packs into one `logsKey` value, kept apart because the
  question its reader asks ("is recovery still finished with this
  generation?") is answered differently for each.

  Both generations always appear, empty when the family names none: a
  caller forced to tell "no old generation" apart from "the key was
  missing" would be reading the shape wrong.

  A foreign key, or an entry whose value will not decode, fails the whole
  decode - the same rule the membership decoder follows, and for a
  sharper reason here: an exclusion check that read a family it cannot
  account for as empty would answer "safe".
  """
  @spec decode_log_locations([{Bedrock.key(), binary()}]) ::
          {:ok, %{current: %{Worker.id() => String.t()}, old: %{Worker.id() => String.t()}}}
          | {:error, {:invalid_log_entry, Bedrock.key()}}
  def decode_log_locations(entries) do
    Enum.reduce_while(entries, {:ok, %{current: %{}, old: %{}}}, fn {key, value}, {:ok, acc} ->
      case decode_log_entry(SystemKeys.parse_key(key), value) do
        {:ok, generation, log_id, node} ->
          {:cont, {:ok, Map.update!(acc, generation, &Map.put(&1, log_id, node))}}

        :error ->
          {:halt, {:error, {:invalid_log_entry, key}}}
      end
    end)
  end

  defp decode_log_entry({:log_key, generation, log_id}, value) do
    case Values.decode_log_node(value) do
      {:ok, node} -> {:ok, generation, log_id, node}
      _ -> :error
    end
  end

  defp decode_log_entry(_not_a_log_key, _value), do: :error

  @doc """
  Decodes `shard_keys/<end_key>` entries into the boundary map
  `%{end_key => {tag, start_key}}`, consuming the carried start key
  verbatim — the same meaning `RoutingData.apply_mutation` gives the
  value; two readers of one family must not disagree. Adjacency
  reconstruction (each shard starts where the previous ends) survives
  only for legacy term_to_binary snapshots that predate carried start
  keys; recovery no longer rewrites the family (read-and-heal,
  bedrock-q67.21.2), so a legacy family stays legacy — encoding-uniform,
  covered by the any-legacy-falls-back-whole rule — until
  bedrock-q67.20.7 retires the fallback with an explicit migration.
  """
  @spec shard_layout_from_entries([{Bedrock.key(), binary()}]) ::
          {:ok, %{Bedrock.key() => {Bedrock.range_tag(), Bedrock.key()}}}
          | {:error, {:invalid_shard_value, Bedrock.key()}}
  def shard_layout_from_entries(entries) do
    entries
    |> Enum.reduce_while({:ok, []}, fn {key, value}, {:ok, acc} ->
      case decode_shard_entry(value) do
        {:ok, decoded} -> {:cont, {:ok, [{extract_end_key(key), decoded} | acc]}}
        {:error, _} -> {:halt, {:error, {:invalid_shard_value, key}}}
      end
    end)
    |> case do
      {:ok, decoded_by_end_key} -> {:ok, build_shard_layout(decoded_by_end_key)}
      error -> error
    end
  end

  defp build_shard_layout(decoded_by_end_key) do
    if Enum.any?(decoded_by_end_key, &match?({_end_key, {:legacy, _tag}}, &1)) do
      rebuild_start_keys_by_adjacency(decoded_by_end_key)
    else
      Map.new(decoded_by_end_key, fn {end_key, {tag, start_key}} -> {end_key, {tag, start_key}} end)
    end
  end

  defp rebuild_start_keys_by_adjacency(decoded_by_end_key) do
    decoded_by_end_key
    |> Enum.sort_by(fn {end_key, _decoded} -> end_key end)
    |> Enum.map_reduce(<<>>, fn {end_key, decoded}, start_key ->
      tag =
        case decoded do
          {:legacy, tag} -> tag
          {tag, _carried_start} -> tag
        end

      {{end_key, {tag, start_key}}, end_key}
    end)
    |> elem(0)
    |> Map.new()
  end

  defp extract_end_key(key) do
    prefix = SystemKeys.shard_keys_prefix()
    prefix_len = byte_size(prefix)
    binary_part(key, prefix_len, byte_size(key) - prefix_len)
  end

  defp decode_shard_entry(value) do
    case Values.decode_shard_key_entry(value) do
      {:ok, {tag, start_key}} -> {:ok, {tag, start_key}}
      {:error, _} -> decode_legacy_shard_tag(value)
    end
  end

  # Clusters created before Bedrock.SystemKeys.Values wrote shard_key
  # values with term_to_binary. Recovery no longer rewrites the family,
  # so these values persist AS-IS until bedrock-q67.20.7's explicit
  # migration retires them along with this fallback.
  defp decode_legacy_shard_tag(value) do
    case :erlang.binary_to_term(value, [:safe]) do
      tag when is_integer(tag) -> {:ok, {:legacy, tag}}
      {tag, _start_key} when is_integer(tag) -> {:ok, {:legacy, tag}}
      _ -> {:error, :invalid_encoding}
    end
  rescue
    ArgumentError -> {:error, :invalid_encoding}
  end
end
