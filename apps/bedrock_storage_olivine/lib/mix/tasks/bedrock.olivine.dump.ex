defmodule Mix.Tasks.Bedrock.Olivine.Dump do
  @shortdoc "Dump keys and values from an Olivine storage instance"

  @moduledoc """
  Dump all keys and values from an Olivine storage instance.

  ## Usage

      mix bedrock.olivine.dump --path /path/to/storage [options]

  ## Options

  * `--path PATH` - Path to the storage directory (required)
  * `--format FORMAT` - Output format: `table`, `csv`, or `json` (default: table)
  * `--key-filter PATTERN` - Only show keys matching the hex pattern (e.g., "fe" for directory keys)
  * `--limit N` - Limit output to first N keys (default: no limit)
  * `--hex-keys` - Always show keys in hex format (default: auto-detect)
  * `--hex-values` - Always show values in hex format (default: auto-detect)
  * `--include-empty` - Include keys with empty values (default: true)
  * `--output FILE` - Write output to file instead of stdout

  ## Examples

      # Basic dump in table format
      mix bedrock.olivine.dump --path /tmp/bedrock_storage

      # Dump to CSV file
      mix bedrock.olivine.dump --path /tmp/bedrock_storage --format csv --output dump.csv

      # Show only directory-related keys (starting with 0xFE)
      mix bedrock.olivine.dump --path /tmp/bedrock_storage --key-filter fe

      # Show first 100 keys in JSON format
      mix bedrock.olivine.dump --path /tmp/bedrock_storage --format json --limit 100

  """

  use Mix.Task

  alias Bedrock.DataPlane.Storage
  alias Bedrock.DataPlane.Storage.Olivine
  alias Bedrock.DataPlane.Version
  alias Bedrock.Service.Worker

  @switches [
    path: :string,
    format: :string,
    key_filter: :string,
    limit: :integer,
    hex_keys: :boolean,
    hex_values: :boolean,
    include_empty: :boolean,
    output: :string,
    help: :boolean
  ]

  @aliases [
    p: :path,
    f: :format,
    k: :key_filter,
    l: :limit,
    o: :output,
    h: :help
  ]

  def run(args) do
    {opts, _args, _invalid} = OptionParser.parse(args, switches: @switches, aliases: @aliases)

    if opts[:help] do
      Mix.shell().info(@moduledoc)
    else
      run_dump(opts)
    end
  end

  defp run_dump(opts) do
    if !opts[:path] do
      Mix.shell().error("Error: --path is required")
      Mix.shell().info(@moduledoc)
      System.halt(1)
    end

    config = %{
      path: opts[:path],
      format: String.to_atom(opts[:format] || "table"),
      key_filter: opts[:key_filter],
      limit: opts[:limit],
      hex_keys: opts[:hex_keys] || false,
      hex_values: opts[:hex_values] || false,
      # default true
      include_empty: opts[:include_empty] != false,
      output: opts[:output]
    }

    dump_storage(config)
  end

  defp dump_storage(config) do
    storage_path = config.path

    if !File.exists?(storage_path) do
      Mix.shell().error("Error: Storage path does not exist: #{storage_path}")
      System.halt(1)
    end

    Mix.shell().info("Opening storage at: #{storage_path}")

    # Generate unique identifiers for the storage worker
    worker_id = Worker.random_id()
    unique_int = System.unique_integer([:positive])
    otp_name = String.to_atom("dump_storage_#{unique_int}")

    # Create child spec for the Olivine storage worker
    child_spec =
      Olivine.child_spec(
        otp_name: otp_name,
        foreman: self(),
        id: worker_id,
        path: storage_path
      )

    # Start the storage worker directly using the child spec
    {GenServer, :start_link, [module, init_arg, gen_opts]} = child_spec.start
    {:ok, storage_pid} = GenServer.start_link(module, init_arg, gen_opts)

    # Wait for health report from the storage worker
    receive do
      {:"$gen_cast", {:worker_health, ^worker_id, {:ok, ^storage_pid}}} -> :ok
    after
      5000 ->
        Mix.shell().error("Error: Storage worker failed to start within timeout")
        System.halt(1)
    end

    try do
      keys_values = fetch_all_keys_values(storage_pid, config)
      output_data(keys_values, config)
    after
      GenServer.stop(storage_pid)
    end
  end

  defp fetch_all_keys_values(storage_pid, config) do
    # First, get the current durable version from the storage
    version =
      case Storage.info(storage_pid, [:durable_version]) do
        {:ok, info_map} ->
          case Map.get(info_map, :durable_version) do
            nil ->
              Mix.shell().info("No durable version in storage info, using zero version")
              Version.zero()

            durable_version ->
              Mix.shell().info("Using durable version: #{Version.to_string(durable_version)}")
              durable_version
          end

        {:error, reason} ->
          Mix.shell().error("Error getting storage version: #{inspect(reason)}")
          # Fall back to zero version for empty storage
          Mix.shell().info("Falling back to zero version")
          Version.zero()
      end

    # Get all keys using range query from empty key to maximum key
    # Note: Using 0xFF repeated to get maximum possible key
    max_key = <<0xFF, 0xFF>>

    case Storage.get_range(storage_pid, <<>>, max_key, version, []) do
      {:ok, {entries, _has_more}} ->
        Mix.shell().info("Found #{length(entries)} key-value pairs")

        entries
        |> filter_entries(config)
        |> limit_entries(config)
        |> Enum.map(fn {key, value} ->
          {format_key(key, config), format_value(value, config), key, value}
        end)

      {:error, reason} ->
        Mix.shell().error("Error reading storage: #{inspect(reason)}")
        System.halt(1)

      {:failure, failure_reason, _storage_ref} ->
        Mix.shell().error("Storage failure: #{inspect(failure_reason)}")
        System.halt(1)
    end
  end

  defp filter_entries(entries, %{key_filter: nil, include_empty: include_empty}) do
    if include_empty do
      entries
    else
      Enum.filter(entries, fn {_key, value} -> value != <<>> end)
    end
  end

  defp filter_entries(entries, %{key_filter: filter, include_empty: include_empty}) do
    filter_bytes = Base.decode16!(String.upcase(filter))

    filtered =
      Enum.filter(entries, fn {key, _value} ->
        String.starts_with?(key, filter_bytes)
      end)

    if include_empty do
      filtered
    else
      Enum.filter(filtered, fn {_key, value} -> value != <<>> end)
    end
  end

  defp limit_entries(entries, %{limit: nil}), do: entries
  defp limit_entries(entries, %{limit: limit}), do: Enum.take(entries, limit)

  defp format_key(key, %{hex_keys: true}), do: hex_format(key)

  defp format_key(key, _config) do
    if printable?(key) do
      inspect(key)
    else
      hex_format(key)
    end
  end

  defp format_value(value, %{hex_values: true}), do: hex_format(value)

  defp format_value(value, _config) do
    if printable?(value) do
      inspect(value)
    else
      hex_format(value)
    end
  end

  defp hex_format(binary) when binary == <<>>, do: "(empty)"

  defp hex_format(binary) when is_binary(binary) do
    "0x" <> Base.encode16(binary, case: :lower)
  end

  defp printable?(<<>>), do: true

  defp printable?(binary) when is_binary(binary) do
    # Check if all bytes are printable ASCII (32-126) or common whitespace (9, 10, 13)
    binary
    |> :binary.bin_to_list()
    |> Enum.all?(fn byte ->
      (byte >= 32 and byte <= 126) or byte in [9, 10, 13]
    end)
  end

  defp output_data(entries, config) do
    output =
      case config.format do
        :table ->
          format_table(entries)

        :csv ->
          format_csv(entries)

        :json ->
          format_json(entries)

        _ ->
          Mix.shell().error("Error: Invalid format. Use: table, csv, or json")
          System.halt(1)
      end

    case config.output do
      nil ->
        Mix.shell().info(output)

      file_path ->
        File.write!(file_path, output)
        Mix.shell().info("Output written to: #{file_path}")
    end
  end

  defp format_table(entries) do
    if Enum.empty?(entries) do
      "No entries found."
    else
      # Calculate column widths
      key_width =
        entries
        |> Enum.map(fn {formatted_key, _, _, _} -> String.length(formatted_key) end)
        |> Enum.max()
        # Minimum width
        |> max(10)

      value_width =
        entries
        |> Enum.map(fn {_, formatted_value, _, _} -> String.length(formatted_value) end)
        |> Enum.max()
        # Minimum width
        |> max(10)
        # Maximum width for readability
        |> min(80)

      # Header
      header = String.pad_trailing("Key", key_width) <> " | " <> String.pad_trailing("Value", value_width)
      separator = String.duplicate("-", key_width) <> "-+-" <> String.duplicate("-", value_width)

      # Rows
      rows =
        Enum.map(entries, fn {formatted_key, formatted_value, _, _} ->
          key_col = String.pad_trailing(formatted_key, key_width)
          value_col = String.pad_trailing(String.slice(formatted_value, 0, value_width), value_width)
          key_col <> " | " <> value_col
        end)

      Enum.join([header, separator | rows], "\n")
    end
  end

  defp format_csv(entries) do
    header = "Key,Value,KeyHex,ValueHex\n"

    rows =
      Enum.map(entries, fn {formatted_key, formatted_value, raw_key, raw_value} ->
        Enum.join(
          [
            csv_escape(formatted_key),
            csv_escape(formatted_value),
            csv_escape(hex_format(raw_key)),
            csv_escape(hex_format(raw_value))
          ],
          ","
        )
      end)

    header <> Enum.join(rows, "\n")
  end

  defp format_json(entries) do
    json_entries =
      Enum.map(entries, fn {formatted_key, formatted_value, raw_key, raw_value} ->
        %{
          "key" => formatted_key,
          "value" => formatted_value,
          "key_hex" => hex_format(raw_key),
          "value_hex" => hex_format(raw_value),
          "key_size" => byte_size(raw_key),
          "value_size" => byte_size(raw_value)
        }
      end)

    Jason.encode!(
      %{
        "entries" => json_entries,
        "count" => length(json_entries)
      },
      pretty: true
    )
  rescue
    UndefinedFunctionError ->
      Mix.shell().error("Error: Jason library not available. Install with: mix deps.get")
      System.halt(1)
  end

  defp csv_escape(value) do
    escaped = String.replace(value, "\"", "\"\"")

    if String.contains?(value, [",", "\"", "\n", "\r"]) do
      "\"#{escaped}\""
    else
      escaped
    end
  end
end
