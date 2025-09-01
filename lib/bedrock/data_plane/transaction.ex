defmodule Bedrock.DataPlane.Transaction do
  @moduledoc """
  Tagged binary transaction encoding for Bedrock that supports efficient operations
  and extensibility through self-describing sections with embedded CRC validation.

  This module implements a comprehensive tagged binary format that encodes the
  simple map structure from `Tx.commit/1` with an efficient binary format featuring:

  - **Tagged sections**: Self-describing sections with type, size, and embedded CRC
  - **Order independence**: Sections can appear in any order for better extensibility
  - **Section omission**: All sections are optional - empty sections are completely omitted to save space
  - **Efficient operations**: Extract specific sections without full decode
  - **Robust validation**: Self-validating CRCs detect any bit corruption
  - **Simple opcode format**: 5-bit operations + 3-bit variants with size optimization

  ## Transaction Structure

  Input/output transaction map:
  ```elixir
  %{
    commit_version: Bedrock.version()               # assigned by commit proxy
    mutations: [Tx.mutation()],                     # {:set, binary(), binary()} | {:clear_range, binary(), binary()}
    read_conflicts: {read_version, [key_range()]},
    write_conflicts: [key_range()}],
  }
  ```

  ## Binary Format

  ```text
  [OVERALL HEADER - 8 bytes]
    - 4 bytes: Magic number (0x42524454 = "BRDT")
    - 1 byte: Format version (0x01)
    - 1 byte: Flags (reserved, set to 0)
    - 2 bytes: Section count (big-endian)

  [SECTION 1: header + payload]
  [SECTION 2: header + payload]
  ...
  [SECTION N: header + payload]
  ```

  ## Section Format

  Each section has an 8-byte header with CRC validation:
  ```text
  [SECTION HEADER - 8 bytes]
    - 1 byte: Section tag
    - 3 bytes: Section payload size (24-bit big-endian, max 16MB)
    - 4 bytes: Section CRC32 (standard CRC32 over tag + size + payload)

  [SECTION PAYLOAD - variable size]
    - Section-specific data format
  ```

  ## CRC Validation

  Uses standard CRC32 calculation and validation:
  - **Encoding**: Calculate CRC32 over tag + size + payload, store value directly
  - **Validation**: Recalculate CRC32 and compare with stored value
  - **Coverage**: CRC covers tag, size, and payload for robust error detection

  ## Section Types

  - 0x01: MUTATIONS - Nearly always present (may be omitted if empty)
  - 0x02: READ_CONFLICTS - Present when read conflicts exist AND read version available
  - 0x03: WRITE_CONFLICTS - Present when write conflicts exist
  - 0x04: COMMIT_VERSION - Present when stamped by commit proxy

  ## Section Usage by Component

  **Logs:** mutations, commit_version
  **Resolver:** write_conflicts, read_conflicts (if any), commit_version
  **Commit Proxy:** mutations, read_conflicts (if reads performed), write_conflicts

  ## Opcode Format

  Mutations use a 5-bit operation + 3-bit variant structure for optimal size:

  **SET Operation (0x00 << 3):**
  - 0x00: 16-bit key + 32-bit value lengths
  - 0x01: 8-bit key + 16-bit value lengths
  - 0x02: 8-bit key + 8-bit value lengths (most compact)

  **CLEAR Operation (0x01 << 3):**
  - 0x08: Single key, 16-bit length
  - 0x09: Single key, 8-bit length
  - 0x0A: Range, 16-bit lengths
  - 0x0B: Range, 8-bit lengths (most compact)

  Size optimization automatically selects the most compact variant.
  """

  import Bitwise, only: [>>>: 2, &&&: 2, <<<: 2, |||: 2]

  alias Bedrock.Cluster.Gateway.TransactionBuilder.Tx

  @type transaction_map :: Bedrock.transaction()

  @type encoded :: binary()

  @type section_tag :: 0x01..0xFF
  @type opcode :: 0x00..0xFF

  @magic_number 0x42524454
  @format_version 0x01

  @mutations_tag 0x01
  @read_conflicts_tag 0x02
  @write_conflicts_tag 0x03
  @commit_version_tag 0x04

  @set_16_32 0x00
  @set_8_16 0x01
  @set_8_8 0x02

  @clear_single_16 0x08
  @clear_single_8 0x09
  @clear_range_16 0x0A
  @clear_range_8 0x0B

  @set_operation 0x00
  @clear_operation 0x01

  @doc """
  Encodes a transaction map into the tagged binary format.

  Automatically selects the most compact opcode variants based on data sizes
  and omits empty sections for optimal space efficiency.

  ## Options
  - `:include_commit_version` - Include placeholder commit version section
  - `:include_transaction_id` - DEPRECATED: Use `:include_commit_version` instead
  """
  @spec encode(transaction_map()) :: binary()
  def encode(transaction) do
    sections =
      []
      |> add_mutations_section(transaction)
      |> add_read_conflicts_section(transaction)
      |> add_write_conflicts_section(transaction)
      |> add_commit_version_section(transaction)

    overall_header = encode_overall_header(length(sections))

    IO.iodata_to_binary([overall_header | sections])
  end

  defp add_mutations_section(sections, %{mutations: mutations}) when is_list(mutations) do
    payload = encode_mutations_payload(mutations)
    section = encode_section(@mutations_tag, payload)
    [section | sections]
  end

  defp add_mutations_section(sections, _), do: sections

  defp add_read_conflicts_section(sections, %{read_conflicts: {read_version, read_conflicts}}) do
    case {read_version, read_conflicts} do
      {nil, []} ->
        sections

      {nil, [_ | _]} ->
        raise ArgumentError, "read_version is nil but read_conflicts is non-empty"

      {_non_nil_version, []} ->
        raise ArgumentError, "read_version is non-nil but read_conflicts is empty"

      {_non_nil_version, [_ | _]} ->
        payload = encode_read_conflicts_payload(read_conflicts, read_version)
        section = encode_section(@read_conflicts_tag, payload)
        [section | sections]
    end
  end

  defp add_read_conflicts_section(sections, _), do: sections

  defp add_write_conflicts_section(sections, %{write_conflicts: []}), do: sections

  defp add_write_conflicts_section(sections, %{write_conflicts: write_conflicts}) when is_list(write_conflicts) do
    payload = encode_write_conflicts_payload(write_conflicts)
    section = encode_section(@write_conflicts_tag, payload)
    [section | sections]
  end

  defp add_write_conflicts_section(sections, _), do: sections

  defp add_commit_version_section(sections, %{commit_version: <<commit_version::binary-size(8)>>}) do
    section = encode_section(@commit_version_tag, commit_version)
    [section | sections]
  end

  defp add_commit_version_section(sections, _), do: sections

  @doc """
  Decodes a tagged binary transaction back to the transaction map format.

  Validates all section CRCs and handles missing sections appropriately.
  """
  @spec decode(binary()) :: {:ok, transaction_map()} | {:error, reason :: term()}
  def decode(
        <<@magic_number::unsigned-big-32, @format_version, _flags, section_count::unsigned-big-16,
          sections_data::binary>>
      ) do
    with {:ok, sections} <- parse_sections(sections_data, section_count) do
      build_transaction_from_sections(sections)
    end
  end

  def decode(_), do: {:error, :invalid_format}

  @doc """
  Validates the binary format integrity using section CRCs.

  Each section validates independently using standard CRC validation
  by recalculating CRC32 over tag + size + payload and comparing with stored CRC.
  """
  @spec validate(binary()) :: {:ok, binary()} | {:error, reason :: term()}
  def validate(
        <<@magic_number::unsigned-big-32, @format_version, _flags, section_count::unsigned-big-16,
          sections_data::binary>> = transaction
      ) do
    case validate_all_sections(sections_data, section_count) do
      :ok -> {:ok, transaction}
      error -> error
    end
  end

  def validate(_), do: {:error, :invalid_format}

  @doc """
  Extracts a specific section payload by tag without full transaction decode.
  """
  @spec extract_section(binary(), section_tag()) :: {:ok, binary()} | {:error, reason :: term()}
  def extract_section(encoded_transaction, target_tag) do
    extract_and_process_section(
      encoded_transaction,
      target_tag,
      &{:ok, &1},
      {:error, :section_not_found}
    )
  end

  @doc """
  Adds a new section to the transaction.

  Returns error if section already exists. Use for adding COMMIT_VERSION section
  after commit proxy processing.
  """
  @spec add_section(binary(), section_tag(), binary()) ::
          {:ok, binary()} | {:error, reason :: term()}
  def add_section(encoded_transaction, section_tag, payload) do
    callback = fn
      tag, _offset, _size, section_payload, sections_map ->
        {:ok, Map.put(sections_map, tag, section_payload)}
    end

    case iterate_sections(encoded_transaction, callback, %{}) do
      {:ok, sections_map} when is_map_key(sections_map, section_tag) ->
        {:error, :section_already_exists}

      {:ok, sections_map} ->
        new_sections_map = Map.put(sections_map, section_tag, payload)

        sections =
          Enum.map(new_sections_map, fn {tag, section_payload} ->
            encode_section(tag, section_payload)
          end)

        section_count = length(sections)
        overall_header = encode_overall_header(section_count)
        transaction = IO.iodata_to_binary([overall_header | sections])
        {:ok, transaction}

      error ->
        error
    end
  end

  @doc """
  Efficiently extracts specific sections from a transaction and reassembles them into a new transaction.

  This avoids the decode-encode cycle by working directly with binary sections.
  Much faster than extracting data and calling encode/1 again.

  ## Parameters
    - `encoded_transaction`: The source transaction
    - `sections_to_keep`: List of section names to keep in the new transaction (`:mutations`, `:read_conflicts`, `:write_conflicts`, `:commit_version`)
    - `new_sections`: Map of additional sections to add (e.g. %{commit_version: version_binary})

  ## Examples
      # Extract just mutations and add commit version
      {:ok, log_transaction} = reassemble_sections(transaction, [:mutations], %{commit_version: version})

      # Extract read conflicts and write conflicts for resolver
      {:ok, conflict_data} = reassemble_sections(transaction, [:read_conflicts, :write_conflicts], %{})
  """
  @spec reassemble_sections(binary(), [atom()], %{atom() => binary()}) ::
          {:ok, binary()} | {:error, reason :: term()}
  def reassemble_sections(encoded_transaction, sections_to_keep, new_sections \\ %{}),
    do: extract_and_add_sections_optimized(encoded_transaction, sections_to_keep, new_sections)

  defp extract_and_add_sections_optimized(encoded_transaction, sections_to_keep, new_sections) do
    case parse_section_offsets(encoded_transaction) do
      {:ok, section_offsets} ->
        section_tags_to_keep = Enum.map(sections_to_keep, &atom_to_tag/1)

        existing_offsets =
          section_offsets
          |> Enum.filter(fn {tag, _offset, _size} -> tag in section_tags_to_keep end)
          |> Enum.sort_by(fn {_tag, offset, _size} -> offset end)

        new_section_binaries =
          Enum.map(new_sections, fn {atom_name, payload} ->
            tag = atom_to_tag(atom_name)
            encode_section(tag, payload)
          end)

        combine_existing_and_new_sections(encoded_transaction, existing_offsets, new_section_binaries)

      error ->
        error
    end
  end

  defp combine_existing_and_new_sections(encoded_transaction, existing_offsets, new_section_binaries) do
    case parse_transaction_header(encoded_transaction) do
      {:ok, {_section_count, sections_data}} ->
        existing_section_binaries =
          Enum.map(existing_offsets, fn {_tag, offset, size} ->
            binary_part(sections_data, offset, size)
          end)

        all_section_binaries = existing_section_binaries ++ new_section_binaries
        total_section_count = length(all_section_binaries)

        case total_section_count do
          0 ->
            {:ok, encode_overall_header(0)}

          _ ->
            overall_header = encode_overall_header(total_section_count)
            {:ok, IO.iodata_to_binary([overall_header | all_section_binaries])}
        end

      error ->
        error
    end
  end

  # Unified section iterator - handles offsets, decoding, and searching
  defp iterate_sections(encoded_transaction, callback_fn, initial_acc) do
    case parse_transaction_header(encoded_transaction) do
      {:ok, {section_count, sections_data}} ->
        iterate_sections_data(sections_data, section_count, callback_fn, initial_acc, 0)

      error ->
        error
    end
  end

  defp iterate_sections_data(_data, 0, _callback_fn, acc, _offset), do: {:ok, acc}

  defp iterate_sections_data(
         <<tag, payload_size::unsigned-big-24, _stored_crc::unsigned-big-32, payload::binary-size(payload_size),
           rest::binary>>,
         remaining_count,
         callback_fn,
         acc,
         current_offset
       ) do
    section_header_size = 8
    total_section_size = section_header_size + payload_size

    case callback_fn.(tag, current_offset, total_section_size, payload, acc) do
      {:ok, new_acc} ->
        next_offset = current_offset + total_section_size
        iterate_sections_data(rest, remaining_count - 1, callback_fn, new_acc, next_offset)

      {:found, result} ->
        {:found, result}
    end
  end

  defp iterate_sections_data(_, _, _, _, _), do: {:error, :truncated_sections}

  defp parse_section_offsets(encoded_transaction) do
    case parse_transaction_header(encoded_transaction) do
      {:ok, {section_count, sections_data}} ->
        parse_offsets_only(sections_data, section_count, [], 0)

      error ->
        error
    end
  end

  defp parse_offsets_only(_data, 0, offsets, _offset), do: {:ok, Enum.reverse(offsets)}

  defp parse_offsets_only(
         <<tag, payload_size::unsigned-big-24, _stored_crc::unsigned-big-32, _payload::binary-size(payload_size),
           rest::binary>>,
         remaining_count,
         offsets,
         current_offset
       ) do
    section_header_size = 8
    total_section_size = section_header_size + payload_size
    next_offset = current_offset + total_section_size

    parse_offsets_only(rest, remaining_count - 1, [{tag, current_offset, total_section_size} | offsets], next_offset)
  end

  defp parse_offsets_only(_, _, _, _), do: {:error, :truncated_sections}

  defp extract_and_process_section(encoded_transaction, target_tag, process_fn, not_found_result) do
    callback = fn
      ^target_tag, _offset, _size, payload, _acc ->
        case process_fn.(payload) do
          {:ok, result} -> {:found, {:ok, result}}
          {:error, reason} -> {:found, {:error, reason}}
        end

      _other_tag, _offset, _size, _payload, _acc ->
        {:ok, nil}
    end

    case iterate_sections(encoded_transaction, callback, nil) do
      {:found, result} -> result
      {:ok, nil} -> not_found_result
      {:error, reason} -> {:error, reason}
    end
  end

  @spec atom_to_tag(atom()) :: section_tag()
  defp atom_to_tag(:mutations), do: @mutations_tag
  defp atom_to_tag(:read_conflicts), do: @read_conflicts_tag
  defp atom_to_tag(:write_conflicts), do: @write_conflicts_tag
  defp atom_to_tag(:commit_version), do: @commit_version_tag

  @doc """
  Efficiently extracts specific sections from a transaction into a smaller binary transaction.

  This is a convenience wrapper around reassemble_sections/3 for the common case
  of extracting sections without adding new ones.

  ## Parameters
    - `encoded_transaction`: The source transaction
    - `sections_to_keep`: List of section names to keep (`:mutations`, `:read_conflicts`, `:write_conflicts`, `:commit_version`)

  ## Examples
      # Extract just conflict sections for resolver
      {:ok, conflict_binary} = extract_sections(transaction, [:read_conflicts, :write_conflicts])

      # Extract mutations for log processing
      {:ok, mutations_binary} = extract_sections(transaction, [:mutations])
  """
  @spec extract_sections(binary(), [atom()]) :: {:ok, binary()} | {:error, reason :: term()}
  def extract_sections(encoded_transaction, sections_to_keep),
    do: reassemble_sections(encoded_transaction, sections_to_keep, %{})

  @doc """
  Efficiently extracts specific sections from a transaction into a smaller binary transaction.

  Same as extract_sections/2 but raises on error instead of returning {:error, reason}.

  ## Parameters
    - `encoded_transaction`: The source transaction
    - `sections_to_keep`: List of section names to keep (`:mutations`, `:read_conflicts`, `:write_conflicts`, `:commit_version`)

  ## Examples
      # Extract just conflict sections for resolver
      conflict_binary = extract_sections!(transaction, [:read_conflicts, :write_conflicts])

      # Extract mutations for log processing
      mutations_binary = extract_sections!(transaction, [:mutations])
  """
  @spec extract_sections!(binary(), [atom()]) :: binary()
  def extract_sections!(encoded_transaction, sections_to_keep) do
    case extract_sections(encoded_transaction, sections_to_keep) do
      {:ok, result} -> result
      {:error, reason} -> raise "Failed to extract sections #{inspect(sections_to_keep)}: #{inspect(reason)}"
    end
  end

  # ============================================================================
  # CONVENIENCE FUNCTIONS
  # ============================================================================

  @doc """
  Extracts read conflicts and read version from READ_CONFLICTS section.

  Returns {nil, []} if no READ_CONFLICTS section exists.
  """
  @spec read_conflicts(binary()) ::
          {:ok, {Bedrock.version() | nil, [{binary(), binary()}]}} | {:error, reason :: term()}
  def read_conflicts(encoded_transaction) do
    extract_and_process_section(
      encoded_transaction,
      @read_conflicts_tag,
      &decode_read_conflicts_payload/1,
      {:ok, {nil, []}}
    )
  end

  @doc """
  Extracts write conflicts from WRITE_CONFLICTS section.

  Returns empty list if no WRITE_CONFLICTS section exists.
  """
  @spec write_conflicts(binary()) ::
          {:ok, [{binary(), binary()}]} | {:error, reason :: term()}
  def write_conflicts(encoded_transaction),
    do:
      extract_and_process_section(
        encoded_transaction,
        @write_conflicts_tag,
        &decode_write_conflicts_payload/1,
        {:ok, []}
      )

  @doc """
  Extracts both read and write conflicts in a single pass for optimal performance.

  This is more efficient than calling read_conflicts/1 and write_conflicts/1
  separately as it only parses the transaction once.

  Returns a tuple with read info and write conflicts.
  """
  @spec read_write_conflicts(binary()) ::
          {:ok, {{Bedrock.version() | nil, [Bedrock.key_range()]}, [Bedrock.key_range()]}}
          | {:error, reason :: term()}
  def read_write_conflicts(encoded_transaction) do
    callback = fn
      @read_conflicts_tag, _offset, _size, payload, {_read_info, write_conflicts} ->
        case decode_read_conflicts_payload(payload) do
          {:ok, read_conflicts} -> {:ok, {read_conflicts, write_conflicts}}
          {:error, reason} -> {:error, reason}
        end

      @write_conflicts_tag, _offset, _size, payload, {read_info, _write_conflicts} ->
        case decode_write_conflicts_payload(payload) do
          {:ok, write_conflicts} -> {:ok, {read_info, write_conflicts}}
          {:error, reason} -> {:error, reason}
        end

      _other_tag, _offset, _size, _payload, acc ->
        {:ok, acc}
    end

    case iterate_sections(encoded_transaction, callback, {{nil, []}, []}) do
      {:ok, result} -> {:ok, result}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Creates a stream of mutations from the MUTATIONS section.

  Enables processing large transactions without loading all mutations into memory.
  """
  @spec mutations(binary()) :: {:ok, Enumerable.t(Tx.mutation())} | {:error, reason :: term()}
  def mutations(encoded_transaction) do
    process_fn = fn payload ->
      stream =
        Stream.resource(
          fn -> payload end,
          &stream_next_mutation/1,
          fn _ -> :ok end
        )

      {:ok, stream}
    end

    extract_and_process_section(encoded_transaction, @mutations_tag, process_fn, {:error, :section_not_found})
  end

  @doc """
  Streams mutations from the transaction, raising if the transaction is invalid.

  Returns a stream of mutations. Use this when you're confident the transaction
  is valid or want to fail fast on invalid data.
  """
  @spec mutations!(binary()) :: Enumerable.t(Tx.mutation())
  def mutations!(encoded_transaction) do
    case mutations(encoded_transaction) do
      {:ok, stream} -> stream
      {:error, reason} -> raise "Failed to stream mutations: #{inspect(reason)}"
    end
  end

  @doc """
  Adds a commit version to an existing transaction.
  """
  @spec add_commit_version(binary(), binary()) :: {:ok, binary()} | {:error, reason :: term()}
  def add_commit_version(encoded_transaction, commit_version)
      when is_binary(commit_version) and byte_size(commit_version) == 8 do
    add_section(encoded_transaction, @commit_version_tag, commit_version)
  end

  @doc """
  Extracts the commit version if present.

  Returns nil if no COMMIT_VERSION section exists.
  """
  @spec commit_version(binary()) :: {:ok, binary() | nil} | {:error, reason :: term()}
  def commit_version(encoded_transaction) do
    process_fn = fn payload ->
      case payload do
        <<_::unsigned-big-64>> = version -> {:ok, version}
        _ -> {:error, :invalid_commit_version_format}
      end
    end

    extract_and_process_section(encoded_transaction, @commit_version_tag, process_fn, {:ok, nil})
  end

  @doc """
  Extracts the commit version from an encoded transaction, raising on error.

  This is the bang version of `commit_version/1` that raises an exception
  instead of returning an error tuple.

  Returns the commit version binary or nil if no commit version is present.
  Raises an exception if the transaction is malformed.
  """
  @spec commit_version!(binary()) :: binary() | nil
  def commit_version!(encoded_transaction) do
    case commit_version(encoded_transaction) do
      {:ok, version} -> version
      {:error, reason} -> raise "Failed to extract commit version: #{inspect(reason)}"
    end
  end

  # ============================================================================
  # DYNAMIC OPCODE CONSTRUCTION
  # ============================================================================

  @spec build_opcode(operation :: 0..31, variant :: 0..7) :: opcode()
  defp build_opcode(operation, variant) when operation <= 31 and variant <= 7, do: operation <<< 3 ||| variant

  @spec extract_variant(opcode()) :: 0..7
  defp extract_variant(opcode) when opcode <= 255, do: opcode &&& 0x07

  @spec optimize_set_opcode(key_size :: non_neg_integer(), value_size :: non_neg_integer()) ::
          opcode()
  defp optimize_set_opcode(key_size, value_size) do
    cond do
      key_size <= 255 and value_size <= 255 ->
        build_opcode(@set_operation, 2)

      key_size <= 255 and value_size <= 65_535 ->
        build_opcode(@set_operation, 1)

      true ->
        build_opcode(@set_operation, 0)
    end
  end

  @spec optimize_clear_opcode(key_size :: non_neg_integer(), is_range :: boolean()) :: opcode()
  defp optimize_clear_opcode(key_size, false = _is_range) when key_size <= 255, do: build_opcode(@clear_operation, 1)

  defp optimize_clear_opcode(_key_size, false = _is_range), do: build_opcode(@clear_operation, 0)

  defp optimize_clear_opcode(max_key_size, true = _is_range) when max_key_size <= 255,
    do: build_opcode(@clear_operation, 3)

  defp optimize_clear_opcode(_max_key_size, true = _is_range), do: build_opcode(@clear_operation, 2)

  # ============================================================================
  # ENCODING IMPLEMENTATION
  # ============================================================================

  defp encode_overall_header(section_count) do
    <<
      @magic_number::unsigned-big-32,
      @format_version,
      0x00,
      section_count::unsigned-big-16
    >>
  end

  defp encode_section(tag, payload) do
    payload_size = byte_size(payload)
    section_content = <<tag, payload_size::unsigned-big-24, payload::binary>>
    section_crc = :erlang.crc32(section_content)

    <<
      tag,
      payload_size::unsigned-big-24,
      section_crc::unsigned-big-32,
      payload::binary
    >>
  end

  defp encode_mutations_payload(mutations) do
    mutations_data = Enum.map(mutations, &encode_mutation_opcode/1)
    IO.iodata_to_binary(mutations_data)
  end

  defp encode_mutation_opcode({:set, key, value}) do
    key_len = byte_size(key)
    value_len = byte_size(value)
    opcode = optimize_set_opcode(key_len, value_len)

    case extract_variant(opcode) do
      2 ->
        <<opcode, key_len::unsigned-8, key::binary, value_len::unsigned-8, value::binary>>

      1 ->
        <<opcode, key_len::unsigned-8, key::binary, value_len::unsigned-big-16, value::binary>>

      0 ->
        <<opcode, key_len::unsigned-big-16, key::binary, value_len::unsigned-big-32, value::binary>>
    end
  end

  defp encode_mutation_opcode({:clear, key}) do
    key_len = byte_size(key)
    opcode = optimize_clear_opcode(key_len, false)

    case extract_variant(opcode) do
      1 ->
        <<opcode, key_len::unsigned-8, key::binary>>

      0 ->
        <<opcode, key_len::unsigned-big-16, key::binary>>
    end
  end

  defp encode_mutation_opcode({:clear_range, start_key, end_key}) do
    start_len = byte_size(start_key)
    end_len = byte_size(end_key)
    max_key_len = max(start_len, end_len)
    opcode = optimize_clear_opcode(max_key_len, true)

    case extract_variant(opcode) do
      3 ->
        <<opcode, start_len::unsigned-8, start_key::binary, end_len::unsigned-8, end_key::binary>>

      2 ->
        <<opcode, start_len::unsigned-big-16, start_key::binary, end_len::unsigned-big-16, end_key::binary>>
    end
  end

  defp encode_read_conflicts_payload(read_conflicts, read_version) do
    conflict_count = length(read_conflicts)
    conflicts_data = Enum.map(read_conflicts, &encode_conflict_range/1)

    read_version_value =
      case read_version do
        nil -> -1
        version when is_integer(version) -> version
        version -> Bedrock.DataPlane.Version.to_integer(version)
      end

    IO.iodata_to_binary([
      <<read_version_value::signed-big-64, conflict_count::unsigned-big-32>>,
      conflicts_data
    ])
  end

  defp encode_write_conflicts_payload(write_conflicts) do
    conflict_count = length(write_conflicts)
    conflicts_data = Enum.map(write_conflicts, &encode_conflict_range/1)

    IO.iodata_to_binary([
      <<conflict_count::unsigned-big-32>>,
      conflicts_data
    ])
  end

  defp encode_conflict_range({start_key, end_key}) do
    start_len = byte_size(start_key)
    end_len = byte_size(end_key)
    <<start_len::unsigned-big-16, start_key::binary, end_len::unsigned-big-16, end_key::binary>>
  end

  # ============================================================================
  # DECODING IMPLEMENTATION
  # ============================================================================

  # Parse all sections into map using unified iterator
  defp parse_sections(sections_data, section_count) do
    # This function needs to extract all payloads for decode/1, so keep the old efficient approach
    parse_sections_old(sections_data, section_count, %{})
  end

  defp parse_sections_old(_data, 0, sections_map), do: {:ok, sections_map}

  defp parse_sections_old(
         <<tag, payload_size::unsigned-big-24, _stored_crc::unsigned-big-32, payload::binary-size(payload_size),
           rest::binary>>,
         remaining_count,
         sections_map
       ) do
    parse_sections_old(rest, remaining_count - 1, Map.put(sections_map, tag, payload))
  end

  defp parse_sections_old(_, _, _), do: {:error, :truncated_sections}

  defp build_transaction_from_sections(sections) do
    transaction = %{
      mutations: [],
      write_conflicts: [],
      read_conflicts: {nil, []}
    }

    with {:ok, transaction} <-
           maybe_decode_mutations(transaction, Map.get(sections, @mutations_tag)),
         {:ok, transaction} <-
           maybe_decode_read_conflicts(transaction, Map.get(sections, @read_conflicts_tag)) do
      maybe_decode_write_conflicts(transaction, Map.get(sections, @write_conflicts_tag))
    end
  end

  defp maybe_decode_mutations(transaction, nil), do: {:ok, transaction}

  defp maybe_decode_mutations(transaction, payload) do
    with {:ok, mutations} <- decode_mutations_payload(payload) do
      {:ok, %{transaction | mutations: mutations}}
    end
  end

  defp decode_mutations_payload(payload) do
    payload
    |> mutations_opcodes([])
    |> case do
      {:ok, mutations} -> {:ok, Enum.reverse(mutations)}
      error -> error
    end
  end

  defp mutations_opcodes(<<>>, mutations), do: {:ok, mutations}

  defp mutations_opcodes(
         <<@set_16_32, key_len::unsigned-big-16, key::binary-size(key_len), value_len::unsigned-big-32,
           value::binary-size(value_len), rest::binary>>,
         mutations
       ) do
    mutation = {:set, key, value}
    mutations_opcodes(rest, [mutation | mutations])
  end

  defp mutations_opcodes(
         <<@set_8_16, key_len::unsigned-8, key::binary-size(key_len), value_len::unsigned-big-16,
           value::binary-size(value_len), rest::binary>>,
         mutations
       ) do
    mutation = {:set, key, value}
    mutations_opcodes(rest, [mutation | mutations])
  end

  defp mutations_opcodes(
         <<@set_8_8, key_len::unsigned-8, key::binary-size(key_len), value_len::unsigned-8,
           value::binary-size(value_len), rest::binary>>,
         mutations
       ) do
    mutation = {:set, key, value}
    mutations_opcodes(rest, [mutation | mutations])
  end

  defp mutations_opcodes(
         <<@clear_single_16, key_len::unsigned-big-16, key::binary-size(key_len), rest::binary>>,
         mutations
       ) do
    mutation = {:clear, key}
    mutations_opcodes(rest, [mutation | mutations])
  end

  defp mutations_opcodes(<<@clear_single_8, key_len::unsigned-8, key::binary-size(key_len), rest::binary>>, mutations) do
    mutation = {:clear, key}
    mutations_opcodes(rest, [mutation | mutations])
  end

  defp mutations_opcodes(
         <<@clear_range_16, start_len::unsigned-big-16, start_key::binary-size(start_len), end_len::unsigned-big-16,
           end_key::binary-size(end_len), rest::binary>>,
         mutations
       ) do
    mutation = {:clear_range, start_key, end_key}
    mutations_opcodes(rest, [mutation | mutations])
  end

  defp mutations_opcodes(
         <<@clear_range_8, start_len::unsigned-8, start_key::binary-size(start_len), end_len::unsigned-8,
           end_key::binary-size(end_len), rest::binary>>,
         mutations
       ) do
    mutation = {:clear_range, start_key, end_key}
    mutations_opcodes(rest, [mutation | mutations])
  end

  defp mutations_opcodes(<<opcode, _rest::binary>>, _mutations) when opcode in 0x03..0x07 do
    {:error, {:reserved_set_variant, opcode}}
  end

  defp mutations_opcodes(<<opcode, _rest::binary>>, _mutations) when opcode in 0x0C..0x0F do
    {:error, {:reserved_clear_variant, opcode}}
  end

  defp mutations_opcodes(<<opcode, _rest::binary>>, _mutations) when opcode in 0x10..0xFF do
    operation_type = opcode >>> 3
    variant = opcode &&& 0x07
    {:error, {:unsupported_operation, operation_type, variant}}
  end

  defp mutations_opcodes(<<opcode, _rest::binary>>, _mutations) do
    {:error, {:invalid_opcode, opcode}}
  end

  defp mutations_opcodes(_truncated_data, _mutations) do
    {:error, :truncated_mutation_data}
  end

  defp maybe_decode_read_conflicts(transaction, nil) do
    {:ok, %{transaction | read_conflicts: {nil, []}}}
  end

  defp maybe_decode_read_conflicts(transaction, payload) do
    case decode_read_conflicts_payload(payload) do
      {:ok, read_conflicts} ->
        {:ok, %{transaction | read_conflicts: read_conflicts}}

      error ->
        error
    end
  end

  defp decode_read_conflicts_payload(
         <<read_version::binary-size(8), conflict_count::unsigned-big-32, conflicts_data::binary>>
       ) do
    case decode_conflict_ranges(conflicts_data, conflict_count, []) do
      {:ok, conflicts} -> {:ok, {read_version, Enum.reverse(conflicts)}}
      error -> error
    end
  end

  defp maybe_decode_write_conflicts(transaction, nil) do
    {:ok, %{transaction | write_conflicts: []}}
  end

  defp maybe_decode_write_conflicts(transaction, payload) do
    with {:ok, write_conflicts} <- decode_write_conflicts_payload(payload) do
      {:ok, %{transaction | write_conflicts: write_conflicts}}
    end
  end

  defp decode_write_conflicts_payload(<<conflict_count::unsigned-big-32, conflicts_data::binary>>) do
    conflicts_data
    |> decode_conflict_ranges(conflict_count, [])
    |> case do
      {:ok, conflicts} -> {:ok, Enum.reverse(conflicts)}
      error -> error
    end
  end

  defp decode_conflict_ranges(_data, 0, conflicts), do: {:ok, conflicts}

  defp decode_conflict_ranges(
         <<start_len::unsigned-big-16, start_key::binary-size(start_len), end_len::unsigned-big-16,
           end_key::binary-size(end_len), rest::binary>>,
         remaining_count,
         conflicts
       ) do
    decode_conflict_ranges(rest, remaining_count - 1, [{start_key, end_key} | conflicts])
  end

  defp decode_conflict_ranges(_, _, _), do: {:error, :truncated_conflict_data}

  # ============================================================================
  # VALIDATION IMPLEMENTATION
  # ============================================================================

  defp validate_all_sections(_data, 0), do: :ok

  defp validate_all_sections(
         <<tag, payload_size::unsigned-big-24, stored_crc::unsigned-big-32, _payload::binary-size(payload_size),
           rest::binary>> = data,
         remaining_count
       ) do
    tag_and_size = binary_part(data, 0, 4)
    payload_part = binary_part(data, 8, payload_size)
    calculated_crc = :erlang.crc32([tag_and_size, payload_part])

    if calculated_crc == stored_crc do
      validate_all_sections(rest, remaining_count - 1)
    else
      {:error, {:section_checksum_mismatch, tag}}
    end
  end

  defp validate_all_sections(_, _), do: {:error, :truncated_sections}

  # ============================================================================
  # SECTION OPERATIONS IMPLEMENTATION
  # ============================================================================

  defp parse_transaction_header(
         <<@magic_number::unsigned-big-32, @format_version, _flags, section_count::unsigned-big-16,
           sections_data::binary>>
       ) do
    {:ok, {section_count, sections_data}}
  end

  defp parse_transaction_header(_), do: {:error, :invalid_format}

  # ============================================================================
  # STREAMING IMPLEMENTATION
  # ============================================================================

  defp stream_next_mutation(<<>>), do: {:halt, <<>>}

  defp stream_next_mutation(
         <<@set_16_32, key_len::unsigned-big-16, key::binary-size(key_len), value_len::unsigned-big-32,
           value::binary-size(value_len), rest::binary>>
       ) do
    mutation = {:set, key, value}
    {[mutation], rest}
  end

  defp stream_next_mutation(
         <<@set_8_16, key_len::unsigned-8, key::binary-size(key_len), value_len::unsigned-big-16,
           value::binary-size(value_len), rest::binary>>
       ) do
    mutation = {:set, key, value}
    {[mutation], rest}
  end

  defp stream_next_mutation(
         <<@set_8_8, key_len::unsigned-8, key::binary-size(key_len), value_len::unsigned-8,
           value::binary-size(value_len), rest::binary>>
       ) do
    mutation = {:set, key, value}
    {[mutation], rest}
  end

  defp stream_next_mutation(<<@clear_single_16, key_len::unsigned-big-16, key::binary-size(key_len), rest::binary>>) do
    mutation = {:clear, key}
    {[mutation], rest}
  end

  defp stream_next_mutation(<<@clear_single_8, key_len::unsigned-8, key::binary-size(key_len), rest::binary>>) do
    mutation = {:clear, key}
    {[mutation], rest}
  end

  defp stream_next_mutation(
         <<@clear_range_16, start_len::unsigned-big-16, start_key::binary-size(start_len), end_len::unsigned-big-16,
           end_key::binary-size(end_len), rest::binary>>
       ) do
    mutation = {:clear_range, start_key, end_key}
    {[mutation], rest}
  end

  defp stream_next_mutation(
         <<@clear_range_8, start_len::unsigned-8, start_key::binary-size(start_len), end_len::unsigned-8,
           end_key::binary-size(end_len), rest::binary>>
       ) do
    mutation = {:clear_range, start_key, end_key}
    {[mutation], rest}
  end

  defp stream_next_mutation(<<opcode, _rest::binary>>) when opcode >= 0x10 do
    operation_type = opcode >>> 3
    variant = opcode &&& 0x07
    {:halt, {:unknown_opcode, operation_type, variant}}
  end

  defp stream_next_mutation(_invalid_data), do: {:halt, <<>>}

  # ============================================================================
  # BINARY OPERATIONS IMPLEMENTATION
  # ============================================================================
end
