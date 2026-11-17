defmodule Mix.Tasks.Bedrock.Fsck do
  @shortdoc "Check the structure of the transaction chunks in an object store"

  @moduledoc """
  Checks the transaction chunks in a Bedrock object store, offline.

  Reads the store directly. No cluster needs to be running, and nothing the
  cluster reports about itself is trusted or consulted — the store either
  describes something coherent or it does not.

  Exits 0 when the store is clean and 1 when any fault was found. Notes are
  informational and never change the exit status.

  ## Usage

      mix bedrock.fsck --path /path/to/object/store [options]

  ## Options

    * `--path PATH` - Root of the object store (required)
    * `--shard TAG` - Only check this shard; may be given more than once
    * `--skip-transactions` - Check chunk structure only, without decoding
      each transaction slice. Much faster on a large store, and enough to
      catch every framing fault. Takes key containment with it.
    * `--skip-layout` - Do not replay the system shard to recover the shard
      layout, and skip the checks that need it
    * `--skip-snapshots` - Do not open the newest snapshot bundle under each
      `s/` prefix. That read has no ranged form, so it downloads a whole
      bundle — the shard's entire materialized state — per shard
    * `--verbose` - List version gaps and every chunk, not just the faults
    * `--format FORMAT` - `text` (default) or `json`

  ## Examples

      # Check everything
      mix bedrock.fsck --path /var/lib/bedrock/objects

      # One shard, structure only, machine-readable
      mix bedrock.fsck --path /var/lib/bedrock/objects --shard a \\
        --skip-transactions --format json

  ## What a fault means

  See `Bedrock.ObjectStorage.Fsck` for what each check proves — and, just
  as importantly, for what a version gap does NOT prove. Gaps between
  chunks are normal: they are reported, never faulted. The same goes for a
  shard the layout names but no chunks back, and for chunks under a tag the
  layout has forgotten.

  ## The shard layout

  The layout every key-containment check is measured against is not in the
  store as a map. It is replayed out of the system shard's own chunks, so
  a store whose system shard is missing or structurally faulted reports
  `layout: unavailable` and skips those checks rather than guessing.

  ## Snapshots

  Each shard's NEWEST bundle is opened and its index record validated,
  because that is the only bundle a cold start will ever read. A bundle
  that fails here means the shard cannot be restored at all, or — worse,
  and silently — restores as a nearly empty one.

  What is NOT checked is whether the shard's chunks still cover everything
  above the snapshot. Nothing in the store can answer that; see
  `Bedrock.ObjectStorage.Fsck` for why, and for why a snapshot AHEAD of
  its chunks is ordinary rather than alarming.

  A clean run says the persisted bytes are coherent with each other. It
  says nothing about whether the cluster kept a commit it acknowledged:
  durable versions live in RAM and are never written to object storage.
  """

  use Mix.Task

  alias Bedrock.ObjectStorage
  alias Bedrock.ObjectStorage.Fsck
  alias Bedrock.ObjectStorage.LocalFilesystem

  @switches [
    path: :string,
    shard: :keep,
    skip_transactions: :boolean,
    skip_layout: :boolean,
    skip_snapshots: :boolean,
    verbose: :boolean,
    format: :string,
    help: :boolean
  ]
  @aliases [p: :path, s: :shard, v: :verbose, f: :format, h: :help]

  @impl Mix.Task
  @spec run([String.t()]) :: :ok
  def run(args) do
    {opts, _rest, _invalid} = OptionParser.parse(args, strict: @switches, aliases: @aliases)

    if opts[:help] do
      Mix.shell().info(@moduledoc)
    else
      opts |> check!() |> report(opts)
    end
  end

  defp check!(opts) do
    backend = backend!(opts)

    Fsck.check(backend,
      shards: shards(opts),
      check_transactions: !opts[:skip_transactions],
      check_layout: !opts[:skip_layout],
      check_snapshots: !opts[:skip_snapshots]
    )
  rescue
    e in ObjectStorage.ListError ->
      # The store could not be enumerated, so "no faults" would be a claim
      # about chunks nobody looked at. That is the one answer this tool
      # must never give.
      abort("could not list the store: #{Exception.message(e)}")
  end

  # The only backend the argv surface builds today. An `--url` option for
  # S3/MinIO belongs here, alongside it: everything past this point works
  # against `ObjectStorage.backend/2` and does not care which one it got.
  defp backend!(opts) do
    path = opts[:path] || abort("--path is required")

    if !File.dir?(path) do
      abort("not a directory: #{path}")
    end

    ObjectStorage.backend(LocalFilesystem, root: path)
  end

  defp shards(opts) do
    case Keyword.get_values(opts, :shard) do
      [] -> nil
      tags -> tags
    end
  end

  defp report(report, opts) do
    case opts[:format] || "text" do
      "text" -> report |> text(opts[:verbose]) |> Mix.shell().info()
      "json" -> report |> json() |> Mix.shell().info()
      other -> abort("unknown format: #{other} (expected text or json)")
    end

    if report.clean?, do: :ok, else: System.halt(1)
  end

  # ------------------------------------------------------------------- text

  defp text(report, verbose) do
    Enum.join(
      layout_lines(report.layout, verbose) ++
        shard_lines(report, verbose) ++
        snapshot_lines(report, verbose) ++
        section("FAULTS", Enum.map(report.faults, &finding_line/1)) ++
        section("NOTES", note_lines(report, verbose)) ++
        [summary(report)],
      "\n"
    )
  end

  defp layout_lines(nil, _verbose), do: []

  defp layout_lines(%{status: :unavailable, reason: reason}, _verbose),
    do: ["layout: unavailable (#{reason}) — key containment and shard correspondence not checked", ""]

  defp layout_lines(layout, verbose) do
    header =
      "layout: #{length(layout.shards)} shard(s) recovered from the system shard" <>
        if(layout.containment == :undecidable, do: " [containment not checked: legacy encoding]", else: "")

    [header | if(verbose, do: Enum.map(layout.shards, &layout_shard_line/1), else: [])] ++ [""]
  end

  defp layout_shard_line(shard), do: "  tag #{shard.tag}  #{inspect(shard.start_key)}..#{inspect(shard.end_key)}"

  defp shard_lines(%{shards: []}, _verbose), do: ["No chunks found."]

  defp shard_lines(report, verbose) do
    Enum.flat_map(report.shards, fn shard ->
      [shard_line(shard) | if(verbose, do: Enum.map(shard.chunks, &chunk_line/1), else: [])]
    end)
  end

  defp shard_line(shard) do
    "shard #{shard.shard_tag}: #{length(shard.chunks)} chunk(s), versions #{range(shard.range)}" <>
      ", #{length(shard.gaps)} gap(s)" <>
      if(shard.range_analysis == :partial, do: " [ranges incomplete: some chunks unreadable]", else: "")
  end

  defp chunk_line(chunk) do
    "  #{chunk.key}  #{range(chunk.range)}  #{chunk.txn_count || "?"} txn, #{chunk.bytes} bytes"
  end

  defp snapshot_lines(%{snapshots: []}, _verbose), do: []

  defp snapshot_lines(report, verbose) do
    ["" | Enum.map(report.snapshots, &snapshot_line(&1, verbose))]
  end

  # The bundle's OWN version is what a restored shard comes up at, so it
  # is what gets printed; the key's is only interesting when they differ,
  # and a fault already says so when they do.
  defp snapshot_line(snapshot, verbose) do
    "snapshot #{snapshot.shard_tag}: #{snapshot.count} bundle(s), newest at version " <>
      "#{snapshot.durable_version || "?"}, chunks through #{snapshot.chunk_max_version || "none"}" <>
      if(verbose, do: "\n  #{snapshot.key}  #{snapshot.bytes} bytes", else: "")
  end

  defp note_lines(report, verbose) do
    report.notes
    |> Enum.reject(&(!verbose and &1.kind == :version_gap))
    |> Enum.map(&finding_line/1)
  end

  defp finding_line(finding) do
    "  #{finding.key || "shard " <> to_string(finding.shard_tag)}  #{finding.kind}  #{inspect(finding.detail)}"
  end

  defp section(_title, []), do: []
  defp section(title, lines), do: ["", "#{title} (#{length(lines)})" | lines]

  defp summary(report) do
    gaps = report.shards |> Enum.map(&length(&1.gaps)) |> Enum.sum()

    "\n#{report.chunk_count} chunk(s) across #{length(report.shards)} shard(s): " <>
      "#{length(report.faults)} fault(s), #{length(report.notes) - gaps} note(s), #{gaps} gap(s)"
  end

  defp range(nil), do: "unknown"
  defp range({min, max}), do: "#{min}..#{max}"

  # ------------------------------------------------------------------- json

  defp json(report) do
    Jason.encode!(
      %{
        "clean" => report.clean?,
        "chunk_count" => report.chunk_count,
        "layout" => json_layout(report.layout),
        "shards" => Enum.map(report.shards, &json_shard/1),
        "snapshots" => Enum.map(report.snapshots, &json_snapshot/1),
        "faults" => Enum.map(report.faults, &json_finding/1),
        "notes" => Enum.map(report.notes, &json_finding/1)
      },
      pretty: true
    )
  end

  defp json_layout(nil), do: nil

  defp json_layout(%{status: :unavailable, reason: reason}),
    do: %{"status" => "unavailable", "reason" => to_string(reason)}

  # Shard boundaries are arbitrary binaries — mostly unprintable, since
  # the interesting ones live up at \xFF — so they are inspected rather
  # than pretended into strings.
  defp json_layout(layout) do
    %{
      "status" => "recovered",
      "containment" => to_string(layout.containment),
      "shards" =>
        Enum.map(layout.shards, fn shard ->
          %{"tag" => shard.tag, "start_key" => inspect(shard.start_key), "end_key" => inspect(shard.end_key)}
        end)
    }
  end

  defp json_shard(shard) do
    %{
      "shard_tag" => shard.shard_tag,
      "range" => json_range(shard.range),
      "range_analysis" => to_string(shard.range_analysis),
      "chunk_count" => length(shard.chunks),
      "gaps" => Enum.map(shard.gaps, &json_detail/1),
      "chunks" =>
        Enum.map(shard.chunks, fn chunk ->
          %{
            "key" => chunk.key,
            "range" => json_range(chunk.range),
            "txn_count" => chunk.txn_count,
            "bytes" => chunk.bytes,
            "fault_count" => length(chunk.faults)
          }
        end)
    }
  end

  defp json_snapshot(snapshot) do
    %{
      "shard_tag" => snapshot.shard_tag,
      "key" => snapshot.key,
      "count" => snapshot.count,
      "version" => snapshot.version,
      "durable_version" => snapshot.durable_version,
      "chunk_max_version" => snapshot.chunk_max_version,
      "bytes" => snapshot.bytes,
      "fault_count" => length(snapshot.faults)
    }
  end

  defp json_finding(finding) do
    %{
      "kind" => to_string(finding.kind),
      "shard_tag" => finding.shard_tag,
      "key" => finding.key,
      "detail" => json_detail(finding.detail)
    }
  end

  # Details hold raw terms — version ranges as tuples, error reasons as
  # atoms — which JSON has no way to carry. Ranges become pairs; anything
  # else that is not already a JSON scalar is inspected.
  defp json_detail(detail) do
    Map.new(detail, fn {key, value} -> {to_string(key), json_value(value)} end)
  end

  defp json_range(nil), do: nil
  defp json_range({min, max}), do: [min, max]

  defp json_value({min, max}) when is_integer(min) and is_integer(max), do: [min, max]
  defp json_value(value) when is_number(value) or is_nil(value), do: value
  defp json_value(value) when is_atom(value), do: to_string(value)

  # Keys are arbitrary binaries, and the ones a containment fault names
  # are the ones with a \xFF in them. Jason refuses invalid UTF-8 by
  # raising, which would turn a found fault into a crashed fsck.
  defp json_value(value) when is_binary(value) do
    if String.valid?(value), do: value, else: inspect(value)
  end

  defp json_value(value), do: inspect(value)

  @spec abort(String.t()) :: no_return()
  defp abort(message) do
    Mix.shell().error("Error: #{message}")
    System.halt(2)
  end
end
