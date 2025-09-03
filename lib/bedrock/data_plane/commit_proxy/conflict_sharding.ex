defmodule Bedrock.DataPlane.CommitProxy.ConflictSharding do
  @moduledoc """
  Shards transaction conflicts across resolvers by key range.

  Takes transaction conflict sections and creates resolver-specific transaction
  binaries containing only conflicts within each resolver's key range.

  ## Process

  1. Extract read/write conflicts from transaction sections
  2. Distribute conflict ranges across resolvers using sweep algorithm
  3. Split ranges that span multiple resolver boundaries
  4. Build Transaction-format binaries for each resolver

  ## Usage

  ```elixir
  sharded_transactions = ConflictSharding.shard_conflicts_across_resolvers(
    transaction_sections,
    resolver_ends,  # [{end_key, resolver_pid}]
    resolver_refs   # [resolver_pid]
  )
  # Returns %{resolver_pid => transaction_binary}
  ```
  """

  alias Bedrock.DataPlane.Transaction

  @end_marker <<0xFF, 0xFF, 0xFF>>

  @spec shard_conflicts_across_resolvers(binary(), [{binary(), pid()}], [pid()]) :: %{pid() => binary()}
  def shard_conflicts_across_resolvers(sections, resolver_ends, resolver_refs) do
    {:ok, {read_conflicts, write_conflicts}} = Transaction.read_write_conflicts(sections)

    sharded_reads = shard_read_conflicts(read_conflicts, resolver_ends)
    sharded_writes = shard_write_conflicts(write_conflicts, resolver_ends)

    Map.new(resolver_refs, fn resolver_ref ->
      resolver_reads = Map.get(sharded_reads, resolver_ref, {nil, []})
      resolver_writes = Map.get(sharded_writes, resolver_ref, [])
      sections_binary = build_transaction_sections(resolver_reads, resolver_writes)
      {resolver_ref, sections_binary}
    end)
  end

  defp shard_read_conflicts({nil, []}, _resolver_ends), do: %{}
  defp shard_read_conflicts({_read_version, []}, _resolver_ends), do: %{}

  defp shard_read_conflicts({read_version, read_ranges}, resolver_ends) do
    read_ranges
    |> shard_conflict_ranges(resolver_ends)
    |> Map.new(fn {resolver_ref, read_ranges} -> {resolver_ref, {read_version, read_ranges}} end)
  end

  defp shard_write_conflicts([], _), do: %{}
  defp shard_write_conflicts(write_ranges, resolver_ends), do: shard_conflict_ranges(write_ranges, resolver_ends)

  defp shard_conflict_ranges(ranges, resolver_ends) do
    %{}
    |> sweep(ranges, <<>>, resolver_ends)
    |> Map.new(fn {k, v} -> {k, Enum.reverse(v)} end)
  end

  defp sweep(map, [], _, _), do: map

  defp sweep(map, [{r_min, _} | _] = ranges, _, [{max_key_ex, _} | remaining_ends]) when r_min >= max_key_ex,
    do: sweep(map, ranges, max_key_ex, remaining_ends)

  defp sweep(map, [{r_min, r_max} | remaining_ranges], min_key, resolver_ends) do
    map
    |> distribute(r_min, r_max, resolver_ends)
    |> sweep(remaining_ranges, min_key, resolver_ends)
  end

  defp distribute(map, r_min, r_max, [{max_key_ex, resolver_ref} | _]) when r_max <= max_key_ex,
    do: Map.update(map, resolver_ref, [{r_min, r_max}], fn existing -> [{r_min, r_max} | existing] end)

  defp distribute(map, r_min, r_max, [{max_key_ex, resolver_ref} | remaining_ends]) do
    map
    |> distribute(max_key_ex, r_max, remaining_ends)
    |> Map.update(resolver_ref, [{r_min, max_key_ex}], fn existing -> [{r_min, max_key_ex} | existing] end)
  end

  defp distribute(_map, r_min, r_max, []) do
    raise(
      "Conflict range #{inspect({r_min, r_max})} extends beyond all resolvers - resolver_ends must end with #{inspect(@end_marker)}"
    )
  end

  defp build_transaction_sections({nil, []}, []), do: Transaction.empty_transaction()

  defp build_transaction_sections(read_conflicts, write_conflicts) do
    []
    |> add_read_conflicts(read_conflicts)
    |> add_write_conflicts(write_conflicts)
    |> build_complete_transaction()
  end

  defp add_read_conflicts(sections, {_read_version, []}), do: sections

  defp add_read_conflicts(sections, {read_version, read_ranges}) do
    payload = encode_read_conflicts_payload(read_ranges, read_version)
    [Transaction.encode_section(0x02, payload) | sections]
  end

  # Mirror Transaction module's private function for read conflicts encoding
  # read_version is guaranteed to be non-nil when this function is called
  defp encode_read_conflicts_payload(read_conflicts, read_version) when is_binary(read_version) do
    conflict_count = length(read_conflicts)
    conflicts_data = Enum.map(read_conflicts, &Transaction.encode_conflict_range/1)

    IO.iodata_to_binary([
      read_version,
      <<conflict_count::unsigned-big-32>>,
      conflicts_data
    ])
  end

  defp add_write_conflicts(sections, write_ranges) do
    payload = encode_write_conflicts_payload(write_ranges)
    [Transaction.encode_section(0x03, payload) | sections]
  end

  # Mirror Transaction module's private function for write conflicts encoding
  defp encode_write_conflicts_payload(write_conflicts) do
    conflict_count = length(write_conflicts)
    conflicts_data = Enum.map(write_conflicts, &Transaction.encode_conflict_range/1)

    IO.iodata_to_binary([
      <<conflict_count::unsigned-big-32>>,
      conflicts_data
    ])
  end

  defp build_complete_transaction(sections) do
    section_count = length(sections)
    header = Transaction.encode_overall_header(section_count)
    IO.iodata_to_binary([header | sections])
  end
end
