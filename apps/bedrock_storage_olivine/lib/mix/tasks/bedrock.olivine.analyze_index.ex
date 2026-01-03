defmodule Mix.Tasks.Bedrock.Olivine.AnalyzeIndex do
  @shortdoc "Analyzes an Olivine index file"

  @moduledoc """
  Analyzes an Olivine index file and provides detailed statistics.

  ## Usage

      mix bedrock.olivine.analyze_index --path /path/to/storage [options]

  ## Options

    * `--path PATH` - Path to the storage directory (required)
    * `--verbose` - Show detailed page-by-page analysis
    * `--histogram` - Show visual histogram of key distribution
    * `--limit N` - Limit verbose output to first N pages

  ## Examples

      # Basic analysis
      mix bedrock.olivine.analyze_index --path /tmp/bedrock_storage

      # Detailed analysis with histogram
      mix bedrock.olivine.analyze_index --path /tmp/bedrock_storage --histogram

      # Verbose output showing first 100 pages
      mix bedrock.olivine.analyze_index --path /tmp/bedrock_storage --verbose --limit 100
  """

  use Mix.Task

  alias Bedrock.DataPlane.Storage.Olivine.Database
  alias Bedrock.DataPlane.Storage.Olivine.Index
  alias Bedrock.DataPlane.Storage.Olivine.IndexDatabase

  require Logger

  @impl Mix.Task
  def run(args) do
    Mix.Task.run("app.start", [])

    config = parse_args(args)
    analyze_storage(config)
  end

  defp parse_args(args) do
    {opts, _, _} =
      OptionParser.parse(args,
        strict: [
          path: :string,
          verbose: :boolean,
          histogram: :boolean,
          limit: :integer
        ]
      )

    path = Keyword.get(opts, :path)

    if !path do
      Mix.shell().error("Error: --path is required")
      Mix.shell().info(@moduledoc)
      System.halt(1)
    end

    %{
      path: path,
      verbose: Keyword.get(opts, :verbose, false),
      histogram: Keyword.get(opts, :histogram, false),
      limit: Keyword.get(opts, :limit, 100)
    }
  end

  defp analyze_storage(config) do
    Mix.shell().info("Analyzing Olivine index at: #{config.path}")
    Mix.shell().info("")

    # Check if this is a directory with dets.idx/dets.data files
    dets_base = Path.join(config.path, "dets")

    base_path =
      cond do
        File.exists?("#{dets_base}.idx") and File.exists?("#{dets_base}.data") ->
          Mix.shell().info("Found dets.idx and dets.data files in directory")
          dets_base

        File.exists?("#{config.path}.idx") and File.exists?("#{config.path}.data") ->
          config.path

        true ->
          config.path
      end

    # Open the database with the correct base path
    case Database.open(:analyzer, base_path) do
      {:ok, db_tuple} ->
        analyze_index(db_tuple, config)

      {:error, reason} ->
        Mix.shell().error("Failed to open storage: #{inspect(reason)}")
        System.halt(1)
    end
  end

  defp analyze_index({_data_db, index_db} = db_tuple, config) do
    # Load durable version
    {:ok, version} = IndexDatabase.load_durable_version(index_db)

    version_str =
      version
      |> :binary.bin_to_list()
      |> Enum.map_join(",", &to_string/1)

    Mix.shell().info("Loading index from durable version: <#{version_str}>")

    # Use Index.load_from to properly load the index from disk
    case Index.load_from(db_tuple) do
      {:ok, index, _version, _new_pages, _loaded_pages} ->
        analyze_index_structure(index, config)

      {:error, :missing_pages} ->
        Mix.shell().info("Index has missing pages")
        System.halt(1)
    end
  end

  defp analyze_index_structure(index, config) do
    page_count = map_size(index.page_map)

    # Collect page statistics
    page_stats =
      index.page_map
      |> Enum.map(fn {id, {page, next_id}} ->
        <<_id::32, key_count::16, right_key_offset::32, _reserved::48, _rest::binary>> = page

        # Calculate payload size
        header_size = 16
        payload_size = byte_size(page) - header_size

        %{
          id: id,
          key_count: key_count,
          next_id: next_id,
          page_size: byte_size(page),
          payload_size: payload_size,
          right_key_offset: right_key_offset,
          avg_entry_size: if(key_count > 0, do: payload_size / key_count, else: 0)
        }
      end)
      |> Enum.sort_by(& &1.id)

    # Calculate statistics
    key_counts = Enum.map(page_stats, & &1.key_count)
    page_sizes = Enum.map(page_stats, & &1.page_size)
    payload_sizes = Enum.map(page_stats, & &1.payload_size)

    total_keys = Enum.sum(key_counts)
    total_size = Enum.sum(page_sizes)
    total_payload = Enum.sum(payload_sizes)

    # Find pages with extra keys (>244)
    extra_key_pages = Enum.filter(page_stats, fn stat -> stat.key_count > 244 end)

    # Key count statistics
    min_keys = Enum.min(key_counts, fn -> 0 end)
    max_keys = Enum.max(key_counts, fn -> 0 end)
    avg_keys = if page_count > 0, do: total_keys / page_count, else: 0
    median_keys = median(key_counts)

    # Page size statistics
    min_size = Enum.min(page_sizes, fn -> 0 end)
    max_size = Enum.max(page_sizes, fn -> 0 end)
    avg_size = if page_count > 0, do: total_size / page_count, else: 0
    median_size = median(page_sizes)

    # Display results
    Mix.shell().info("""
    ================================================================================
    Index Statistics
    ================================================================================

    Pages:
      Total pages:                    #{format_number(page_count)}
      Pages with extra keys (>244):   #{format_number(length(extra_key_pages))} (#{format_percent(length(extra_key_pages), page_count)})

    Keys:
      Total keys:                     #{format_number(total_keys)}
      Min keys per page:              #{min_keys}
      Max keys per page:              #{max_keys}
      Average keys per page:          #{format_float(avg_keys, 2)}
      Median keys per page:           #{median_keys}

    Memory:
      Total index size:               #{format_bytes(total_size)}
      Total payload size:             #{format_bytes(total_payload)}
      Overhead:                       #{format_bytes(total_size - total_payload)} (#{format_percent(total_size - total_payload, total_size)})

      Min page size:                  #{format_bytes(min_size)}
      Max page size:                  #{format_bytes(max_size)}
      Average page size:              #{format_bytes(round(avg_size))}
      Median page size:               #{format_bytes(median_size)}

      Average bytes per key:          #{format_float(total_size / max(1, total_keys), 2)} bytes
      Average payload per key:        #{format_float(total_payload / max(1, total_keys), 2)} bytes
    """)

    if config.histogram do
      display_histogram(key_counts, page_count)
    end

    if length(extra_key_pages) > 0 do
      display_extra_keys_analysis(extra_key_pages)
    end

    if config.verbose do
      display_verbose_pages(page_stats, config.limit)
    end

    Mix.shell().info("================================================================================")
  end

  defp display_histogram(key_counts, total_pages) do
    Mix.shell().info("""

    Key Distribution Histogram:
    ================================================================================
    """)

    frequencies = key_counts |> Enum.frequencies() |> Enum.sort()

    Enum.each(frequencies, fn {count, pages} ->
      percentage = pages * 100.0 / total_pages
      bar_length = min(60, round(pages * 60.0 / total_pages))
      bar = String.duplicate("█", max(1, bar_length))

      label =
        cond do
          count < 244 -> ""
          count == 244 -> " [standard]"
          count > 244 -> " [EXTRA]"
        end

      Mix.shell().info(
        "  #{String.pad_leading(to_string(count), 3)} keys#{label}: " <>
          "#{String.pad_leading(to_string(pages), 6)} pages " <>
          "(#{String.pad_leading(format_float(percentage, 1), 5)}%) #{bar}"
      )
    end)
  end

  defp display_extra_keys_analysis(extra_key_pages) do
    total_extra_keys =
      extra_key_pages
      |> Enum.map(fn page -> page.key_count - 244 end)
      |> Enum.sum()

    avg_extra = total_extra_keys / max(1, length(extra_key_pages))

    Mix.shell().info("""

    Extra Keys Analysis (Pages with >244 keys):
    ================================================================================
      Total pages with extra keys:   #{length(extra_key_pages)}
      Total extra keys:               #{total_extra_keys}
      Average extra keys per page:    #{format_float(avg_extra, 2)}

      Distribution:
    """)

    extra_key_pages
    |> Enum.map(& &1.key_count)
    |> Enum.frequencies()
    |> Enum.sort()
    |> Enum.each(fn {count, pages} ->
      extra = count - 244
      Mix.shell().info("    #{count} keys (+#{extra}): #{pages} pages")
    end)
  end

  defp display_verbose_pages(page_stats, limit) do
    Mix.shell().info("""

    Page Details (First #{limit} pages):
    ================================================================================
    Page ID   | Keys | Size (bytes) | Payload | Avg Entry | Next ID
    ----------+------+--------------+---------+-----------+----------
    """)

    page_stats
    |> Enum.take(limit)
    |> Enum.each(fn stat ->
      extra_marker = if stat.key_count > 244, do: "*", else: " "

      Mix.shell().info(
        "#{String.pad_leading(to_string(stat.id), 8)}  | " <>
          "#{String.pad_leading(to_string(stat.key_count), 3)}#{extra_marker} | " <>
          "#{String.pad_leading(to_string(stat.page_size), 12)} | " <>
          "#{String.pad_leading(to_string(stat.payload_size), 7)} | " <>
          "#{String.pad_leading(format_float(stat.avg_entry_size, 1), 9)} | " <>
          "#{String.pad_leading(to_string(stat.next_id), 8)}"
      )
    end)

    if length(page_stats) > limit do
      Mix.shell().info("... and #{length(page_stats) - limit} more pages")
    end
  end

  # Helper functions

  defp median([]), do: 0

  defp median(list) do
    sorted = Enum.sort(list)
    mid = div(length(sorted), 2)

    if rem(length(sorted), 2) == 1 do
      Enum.at(sorted, mid)
    else
      (Enum.at(sorted, mid - 1) + Enum.at(sorted, mid)) / 2
    end
  end

  defp format_number(n) when n >= 1_000_000 do
    "#{format_float(n / 1_000_000, 2)}M"
  end

  defp format_number(n) when n >= 1_000 do
    "#{format_float(n / 1_000, 1)}K"
  end

  defp format_number(n), do: to_string(n)

  defp format_bytes(bytes) when bytes >= 1_073_741_824 do
    "#{format_float(bytes / 1_073_741_824, 2)} GB"
  end

  defp format_bytes(bytes) when bytes >= 1_048_576 do
    "#{format_float(bytes / 1_048_576, 2)} MB"
  end

  defp format_bytes(bytes) when bytes >= 1024 do
    "#{format_float(bytes / 1024, 2)} KB"
  end

  defp format_bytes(bytes), do: "#{bytes} B"

  defp format_float(value, decimals) do
    (value / 1)
    |> Float.round(decimals)
    |> :erlang.float_to_binary(decimals: decimals)
    |> to_string()
  end

  defp format_percent(part, whole) when whole > 0 do
    "#{format_float(part * 100.0 / whole, 1)}%"
  end

  defp format_percent(_, _), do: "0.0%"
end
