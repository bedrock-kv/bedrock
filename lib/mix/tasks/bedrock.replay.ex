defmodule Mix.Tasks.Bedrock.Replay do
  @shortdoc "Rebuild a keyspace from the chunks in an object store"

  @moduledoc """
  Replays the transaction chunks in a Bedrock object store and prints the
  keyspace they describe.

  Reads the store directly. No cluster needs to be running, and nothing the
  cluster reports about itself is consulted — the chunks either fold into a
  keyspace or they do not.

  This is the offline half of the replay-and-diff oracle. The other half,
  comparing the result against what a running cluster serves, needs a live
  client and so lives in-process: see `Bedrock.ObjectStorage.Replay.diff/3`
  and `compare/3`, which a chaos harness calls at quiesce and reads
  structured results from rather than parsing this output.

  ## Usage

      mix bedrock.replay --path /path/to/object/store [options]

  ## Options

    * `--path PATH` - Root of the object store (required)
    * `--shard TAG` - Only replay this shard; may be given more than once
    * `--through-version N` - Stop the replay at this version, for an image
      that is consistent across shards as of it rather than one that is as
      far along as each shard happens to be
    * `--verbose` - List the reconstructed keys, not just the summary
    * `--limit N` - How many keys to list under `--verbose` (default: 50)
    * `--format FORMAT` - `text` (default) or `json`

  ## Examples

      # Summarize what the store says the keyspace is
      mix bedrock.replay --path /var/lib/bedrock/objects

      # One shard's keys, machine-readable
      mix bedrock.replay --path /var/lib/bedrock/objects --shard 1 \\
        --verbose --format json

  ## What the result is worth

  A mutation fold gives point-lookup equivalence: these are the keys a
  point read should find and the values it should return. It is not a page
  index, so it says nothing about what a range read would return.

  The replay needs the shard layout, which is itself replayed out of the
  system shard's chunks, so a store whose system shard is missing or
  structurally faulted has no reconstruction at all. Run `mix bedrock.fsck`
  first: every fault it reports is a reason a replay silently returns the
  wrong answer.
  """

  use Mix.Task

  alias Bedrock.ObjectStorage
  alias Bedrock.ObjectStorage.LocalFilesystem
  alias Bedrock.ObjectStorage.Replay

  @switches [
    path: :string,
    shard: :keep,
    through_version: :integer,
    verbose: :boolean,
    limit: :integer,
    format: :string,
    help: :boolean
  ]
  @aliases [p: :path, s: :shard, v: :verbose, l: :limit, f: :format, h: :help]

  @impl Mix.Task
  @spec run([String.t()]) :: :ok
  def run(args) do
    {opts, _rest, _invalid} = OptionParser.parse(args, strict: @switches, aliases: @aliases)

    if opts[:help] do
      Mix.shell().info(@moduledoc)
    else
      opts |> reconstruct!() |> report(opts)
    end
  end

  defp reconstruct!(opts) do
    case Replay.reconstruct(backend!(opts), shards: shards(opts), through_version: opts[:through_version]) do
      {:ok, image} -> image
      {:error, reason} -> Mix.raise("could not reconstruct the keyspace: #{describe(reason)}")
    end
  rescue
    e in ObjectStorage.ListError ->
      # A short listing would reconstruct a keyspace out of the chunks that
      # happened to be returned, and present it as the whole one.
      Mix.raise("could not list the store: #{Exception.message(e)}")
  end

  defp describe({:layout_unavailable, reason}),
    do: "the shard layout could not be recovered from the system shard (#{reason})"

  defp describe({:unreadable_chunk, key, reason}), do: "chunk #{key} could not be read (#{inspect(reason)})"

  defp describe({:undecodable_transaction, shard_tag, version, reason}),
    do: "shard #{shard_tag} version #{version} would not decode (#{inspect(reason)})"

  # The only backend the argv surface builds today, exactly as
  # `mix bedrock.fsck` builds it.
  defp backend!(opts) do
    path = opts[:path] || Mix.raise("--path is required")

    if !File.dir?(path) do
      Mix.raise("not a directory: #{path}")
    end

    ObjectStorage.backend(LocalFilesystem, root: path)
  end

  defp shards(opts) do
    case Keyword.get_values(opts, :shard) do
      [] -> nil
      tags -> tags
    end
  end

  defp report(image, opts) do
    case opts[:format] || "text" do
      "text" -> image |> text(opts) |> Mix.shell().info()
      "json" -> image |> json(opts) |> Mix.shell().info()
      other -> Mix.raise("unknown format: #{other} (expected text or json)")
    end
  end

  # ------------------------------------------------------------------- text

  @doc false
  @spec text(Replay.Image.t(), keyword()) :: String.t()
  def text(image, opts) do
    Enum.join(
      ["layout: #{length(image.layout.shards)} shard(s) recovered from the system shard"] ++
        Enum.map(image.shards, &shard_line/1) ++
        [""] ++
        caveat_lines(image) ++
        key_lines(image, opts) ++
        [summary(image)],
      "\n"
    )
  end

  defp shard_line(shard) do
    "shard #{shard.shard_tag}: #{shard.chunk_count} chunk(s), #{shard.transaction_count} txn replayed" <>
      ", through version #{shard.max_replayed_version || "nothing"}" <>
      if(shard.max_chunk_version && shard.max_chunk_version != shard.max_replayed_version,
        do: " (chunks reach #{shard.max_chunk_version})",
        else: ""
      )
  end

  defp caveat_lines(image) do
    [
      caveat_line("shard(s) the layout names with no chunks at all", image.shards_without_chunks),
      caveat_line("shard(s) with chunks the layout does not name (not replayed)", image.orphan_shards),
      caveat_line("key(s) claimed by more than one shard (resolved to the newest write)", image.multiply_claimed_keys)
    ]
    |> Enum.reject(&is_nil/1)
    |> case do
      [] -> []
      lines -> lines ++ [""]
    end
  end

  defp caveat_line(_description, []), do: nil
  defp caveat_line(description, values), do: "note: #{description}: #{Enum.map_join(values, ", ", &inspect/1)}"

  defp key_lines(image, opts) do
    if opts[:verbose] do
      limit = opts[:limit] || 50

      image.keys
      |> Enum.sort()
      |> Enum.take(limit)
      |> Enum.map(fn {key, entry} ->
        "  #{inspect(key)} = #{inspect(entry.value)}  @#{entry.version} from shard #{entry.shard_tag}"
      end)
      |> Kernel.++([""])
    else
      []
    end
  end

  defp summary(image) do
    "#{map_size(image.keys)} key(s) across #{length(image.shards)} shard(s); " <>
      "frontier #{image.frontier || "none"}, anchor #{image.anchor || "none"}" <>
      if(image.through_version, do: ", truncated at #{image.through_version}", else: "")
  end

  # ------------------------------------------------------------------- json

  @doc false
  @spec json(Replay.Image.t(), keyword()) :: String.t()
  def json(image, opts) do
    Jason.encode!(
      %{
        "key_count" => map_size(image.keys),
        "frontier" => image.frontier,
        "anchor" => image.anchor,
        "through_version" => image.through_version,
        "shards" => Enum.map(image.shards, &json_shard/1),
        "shards_without_chunks" => image.shards_without_chunks,
        "orphan_shards" => image.orphan_shards,
        "multiply_claimed_keys" => Enum.map(image.multiply_claimed_keys, &printable/1),
        "keys" => json_keys(image, opts)
      },
      pretty: true
    )
  end

  defp json_shard(shard) do
    %{
      "shard_tag" => shard.shard_tag,
      "chunk_count" => shard.chunk_count,
      "transaction_count" => shard.transaction_count,
      "max_chunk_version" => shard.max_chunk_version,
      "max_replayed_version" => shard.max_replayed_version
    }
  end

  defp json_keys(image, opts) do
    if opts[:verbose] do
      image
      |> Map.fetch!(:keys)
      |> Enum.sort()
      |> Enum.take(opts[:limit] || 50)
      |> Enum.map(fn {key, entry} ->
        %{
          "key" => printable(key),
          "value" => printable(entry.value),
          "version" => entry.version,
          "shard_tag" => entry.shard_tag
        }
      end)
    end
  end

  # Keys and values are arbitrary binaries, and the interesting ones live
  # up at \\xFF. Jason raises on invalid UTF-8, which would turn a
  # successful replay into a crashed task.
  defp printable(binary) do
    if String.valid?(binary), do: binary, else: inspect(binary)
  end
end
