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
  - **16-bit instruction format**: 5-bit operations + 3-bit reserved + 8-bit parameters with optimized length encoding

  ## Transaction Structure

  Input/output transaction map:
  ```elixir
  %{
    commit_version: Bedrock.version()               # assigned by commit proxy
    mutations: [Tx.mutation()],                     # {:set, binary(), binary()} | {:clear_range, binary(), binary()} | atomic operations
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

  ## 16-bit Header Mutation Format

  All mutations use a 16-bit header format with 4-tier variable-length encoding:

  **Header Structure (16 bits):**
  - **5 bits: opcode** (0-31 operations)
  - **3 bits: reserved** (future extensibility)
  - **8 bits: parameters** (format encoding - see below)

  **Operation Codes:**
  - 0x00: SET - Set key to value
  - 0x01: CLEAR - Clear single key
  - 0x02: CLEAR_RANGE - Clear key range
  - 0x03: ATOMIC_ADD - Atomic add operation
  - 0x04: ATOMIC_MIN - Atomic minimum operation
  - 0x05: ATOMIC_MAX - Atomic maximum operation

  **Parameters Field Encoding:**

  For two-parameter mutations (SET, CLEAR_RANGE):
  - **Upper 4 bits (f1::4)**: Format for first parameter (key/start_key)
  - **Lower 4 bits (f2::4)**: Format for second parameter (value/end_key)

  For single-parameter mutations (CLEAR, atomics):
  - **Upper 4 bits (f::4)**: Format for parameter (key)
  - **Lower 4 bits**: Zero (reserved)

  **4-Tier Variable-Length Format Values:**
  - **Direct encoding (0-11)**: Values 0-11 encoded directly as format value
  - **1-byte extended (1-256)**: Format 12 (0b1100) + (len-1) in next byte
  - **1-byte extended (257-512)**: Format 13 (0b1101) + (len-1 & 0xFF) in next byte, 9th bit encoded in format
  - **2-byte extended (1-65536)**: Format 14 (0b1110) + (len-1) in next 2 bytes
  - **2-byte extended (65537-131072)**: Format 15 (0b1111) + (len-1 & 0xFFFF) in next 2 bytes, 17th bit encoded in format

  The system automatically selects the most compact format for each parameter.
  """

  import Bitwise
  import Bitwise, only: [>>>: 2, &&&: 2]

  alias Bedrock.Cluster.Gateway.TransactionBuilder.Tx

  @type transaction_map :: Bedrock.transaction()

  @type encoded :: binary()

  @type section_tag :: 0x01..0xFF

  @magic_number 0x42524454
  @format_version 0x01

  @mutations_tag 0x01
  @read_conflicts_tag 0x02
  @write_conflicts_tag 0x03
  @commit_version_tag 0x04

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

  # Constant header-only transaction for version advancement (no sections at all)
  @empty_transaction <<
    @magic_number::unsigned-big-32,
    @format_version,
    0x00,
    0::unsigned-big-16
  >>

  @doc """
  Returns a minimal header-only transaction with no sections for version advancement.

  This is more efficient than encoding an empty transaction map as it contains
  only the transaction header with zero sections, making it the smallest possible
  valid transaction binary.
  """
  @spec empty_transaction() :: encoded()
  def empty_transaction, do: @empty_transaction

  # ============================================================================
  # ENCODING IMPLEMENTATION
  # ============================================================================

  @doc false
  def encode_overall_header(section_count) do
    <<
      @magic_number::unsigned-big-32,
      @format_version,
      0x00,
      section_count::unsigned-big-16
    >>
  end

  @doc false
  def encode_section(tag, payload) do
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
    mutations_data = Enum.map(mutations, &encode_mutation/1)
    IO.iodata_to_binary(mutations_data)
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

  @doc false
  def encode_conflict_range({start_key, end_key}) do
    start_len = byte_size(start_key)

    # Handle :end case for ConflictSharding
    {end_key_binary, end_len} =
      case end_key do
        :end -> {"\xFF\xFF\xFF", 3}
        key -> {key, byte_size(key)}
      end

    <<start_len::unsigned-big-16, start_key::binary, end_len::unsigned-big-16, end_key_binary::binary>>
  end

  # ============================================================================
  # DECODING IMPLEMENTATION
  # ============================================================================

  # Parse all sections into map using unified iterator
  defp parse_sections(sections_data, section_count), do: parse_sections_old(sections_data, section_count, %{})

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
           maybe_decode_read_conflicts(transaction, Map.get(sections, @read_conflicts_tag)),
         {:ok, transaction} <-
           maybe_decode_write_conflicts(transaction, Map.get(sections, @write_conflicts_tag)) do
      maybe_decode_commit_version(transaction, Map.get(sections, @commit_version_tag))
    end
  end

  defp maybe_decode_mutations(transaction, nil), do: {:ok, transaction}

  defp maybe_decode_mutations(transaction, payload) do
    with {:ok, mutations} <- decode_mutations_payload(payload) do
      {:ok, %{transaction | mutations: mutations}}
    end
  end

  defp decode_mutations_payload(payload) do
    stream =
      Stream.resource(
        fn -> payload end,
        &stream_next_mutation/1,
        fn _ -> :ok end
      )

    {:ok, Enum.to_list(stream)}
  rescue
    e -> {:error, {:decode_exception, Exception.message(e)}}
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

  defp maybe_decode_commit_version(transaction, nil), do: {:ok, transaction}

  defp maybe_decode_commit_version(transaction, payload) do
    case payload do
      <<commit_version::binary-size(8)>> ->
        # Include commit_version in the transaction structure when present
        {:ok, Map.put(transaction, :commit_version, commit_version)}

      _ ->
        {:error, :invalid_commit_version_format}
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

  defp stream_next_mutation(<<opcode::5, _reserved::3, params::8, rest::binary>>) do
    {mutation, remaining_data} = decode_mutation(opcode, params, rest)
    {[mutation], remaining_data}
  end

  defp stream_next_mutation(<<>>), do: {:halt, <<>>}

  # ============================================================================
  # 16-BIT MUTATION FORMAT WITH OPTIMIZED LENGTH ENCODING
  # ============================================================================

  # 4-Tier Variable-Length Encoding Constants
  # Direct encoding: 0-11 encoded as format value
  @length_direct_max 11
  # Maximum length boundaries for 4-tier encoding
  # 2^9 - Maximum for 1-byte extended encoding
  @length_1b_max 512
  # 2^17 - Maximum for 2-byte extended encoding
  @length_2b_max 131_072

  # 16-bit header operation codes
  @op16_set 0x00
  @op16_clear 0x01
  @op16_clear_range 0x02
  @op16_add 0x03
  @op16_min 0x04
  @op16_max 0x05
  @op16_bit_and 0x06
  @op16_bit_or 0x07
  @op16_bit_xor 0x08
  @op16_byte_min 0x09
  @op16_byte_max 0x0A
  @op16_append_if_fits 0x0B
  @op16_compare_and_clear 0x0C

  # 16-bit format mutation encoding using pattern matching for clean separation
  defp encode_mutation({:set, key, value}), do: encode_op2vb(@op16_set, key, value)
  defp encode_mutation({:clear, key}), do: encode_op1vb(@op16_clear, key)
  defp encode_mutation({:clear_range, start_key, end_key}), do: encode_op2vb(@op16_clear_range, start_key, end_key)
  defp encode_mutation({:atomic, :add, key, value}), do: encode_op2vb(@op16_add, key, value)
  defp encode_mutation({:atomic, :min, key, value}), do: encode_op2vb(@op16_min, key, value)
  defp encode_mutation({:atomic, :max, key, value}), do: encode_op2vb(@op16_max, key, value)
  defp encode_mutation({:atomic, :bit_and, key, value}), do: encode_op2vb(@op16_bit_and, key, value)
  defp encode_mutation({:atomic, :bit_or, key, value}), do: encode_op2vb(@op16_bit_or, key, value)
  defp encode_mutation({:atomic, :bit_xor, key, value}), do: encode_op2vb(@op16_bit_xor, key, value)
  defp encode_mutation({:atomic, :byte_min, key, value}), do: encode_op2vb(@op16_byte_min, key, value)
  defp encode_mutation({:atomic, :byte_max, key, value}), do: encode_op2vb(@op16_byte_max, key, value)
  defp encode_mutation({:atomic, :append_if_fits, key, value}), do: encode_op2vb(@op16_append_if_fits, key, value)

  defp encode_mutation({:atomic, :compare_and_clear, key, expected}),
    do: encode_op2vb(@op16_compare_and_clear, key, expected)

  # operation with one varbinary parameters
  defp encode_op1vb(op, p) do
    {f, v} = encode_varbinary_param(p)
    [<<op::5, 0::3, f::4, 0::4>>, v]
  end

  # operation with two varbinary parameters
  defp encode_op2vb(op, p1, p2) do
    {f1, v1} = encode_varbinary_param(p1)
    {f2, v2} = encode_varbinary_param(p2)
    [<<op::5, 0::3, f1::4, f2::4>>, v1, v2]
  end

  defp encode_varbinary_param(p) when byte_size(p) <= @length_direct_max, do: {byte_size(p), p}

  defp encode_varbinary_param(p) when byte_size(p) <= @length_1b_max / 2,
    do: {0b1100, [<<byte_size(p) - 1 &&& 0xFF::8>>, p]}

  defp encode_varbinary_param(p) when byte_size(p) <= @length_1b_max,
    do: {0b1101, [<<byte_size(p) - 1 &&& 0xFF::8>>, p]}

  defp encode_varbinary_param(p) when byte_size(p) <= @length_2b_max / 2,
    do: {0b1110, [<<byte_size(p) - 1 &&& 0xFFFF::big-16>>, p]}

  defp encode_varbinary_param(p) when byte_size(p) <= @length_2b_max,
    do: {0b1111, [<<byte_size(p) - 1 &&& 0xFFFF::big-16>>, p]}

  # Pattern matching for clean shape-based decoding (matches encoding structure)
  defp decode_mutation(@op16_clear, params, data), do: decode_op1vb(:clear, params, data)
  defp decode_mutation(@op16_set, params, data), do: decode_op2vb(:set, params, data)
  defp decode_mutation(@op16_clear_range, params, data), do: decode_op2vb(:clear_range, params, data)
  defp decode_mutation(@op16_add, params, data), do: decode_atomic_op2vb(:add, params, data)
  defp decode_mutation(@op16_min, params, data), do: decode_atomic_op2vb(:min, params, data)
  defp decode_mutation(@op16_max, params, data), do: decode_atomic_op2vb(:max, params, data)
  defp decode_mutation(@op16_bit_and, params, data), do: decode_atomic_op2vb(:bit_and, params, data)
  defp decode_mutation(@op16_bit_or, params, data), do: decode_atomic_op2vb(:bit_or, params, data)
  defp decode_mutation(@op16_bit_xor, params, data), do: decode_atomic_op2vb(:bit_xor, params, data)
  defp decode_mutation(@op16_byte_min, params, data), do: decode_atomic_op2vb(:byte_min, params, data)
  defp decode_mutation(@op16_byte_max, params, data), do: decode_atomic_op2vb(:byte_max, params, data)
  defp decode_mutation(@op16_append_if_fits, params, data), do: decode_atomic_op2vb(:append_if_fits, params, data)
  defp decode_mutation(@op16_compare_and_clear, params, data), do: decode_atomic_op2vb(:compare_and_clear, params, data)
  defp decode_mutation(opcode, _params, _data), do: raise("Unsupported 16-bit opcode: #{opcode}")

  # decode operation with one varbinary parameter
  defp decode_op1vb(operation, params, data) do
    # upper 4 bits contain format, lower 4 bits are zero
    f = params >>> 4
    {key, remaining} = decode_varbinary_param(f, data)
    {{operation, key}, remaining}
  end

  # decode operation with two varbinary parameters
  defp decode_op2vb(operation, params, data) do
    f1 = params >>> 4
    f2 = params &&& 0b1111

    {param1, data_after_param1} = decode_varbinary_param(f1, data)
    {param2, remaining} = decode_varbinary_param(f2, data_after_param1)
    {{operation, param1, param2}, remaining}
  end

  # decode atomic operation with two varbinary parameters
  defp decode_atomic_op2vb(operation, params, data) do
    f1 = params >>> 4
    f2 = params &&& 0b1111

    {param1, data_after_param1} = decode_varbinary_param(f1, data)
    {param2, remaining} = decode_varbinary_param(f2, data_after_param1)
    {{:atomic, operation, param1, param2}, remaining}
  end

  # Helper function to decode varbinary parameters from the new format
  defp decode_varbinary_param(f, data) when f <= @length_direct_max do
    <<param::binary-size(f), rest::binary>> = data
    {param, rest}
  end

  # 1-byte extended - range 12 to 256
  defp decode_varbinary_param(0b1100, <<length::8, param::binary-size(length + 1), rest::binary>>), do: {param, rest}

  # 1-byte extended - range 257 to 512 (9th bit encoded in format)
  defp decode_varbinary_param(0b1101, <<length::8, param::binary-size(length + 1 + 256), rest::binary>>),
    do: {param, rest}

  # 2-byte extended - range 513 to 65,536
  defp decode_varbinary_param(0b1110, <<length::big-16, param::binary-size(length + 1), rest::binary>>),
    do: {param, rest}

  # 2-byte extended - range 65,537 to 131,072 (17th bit encoded in format)
  defp decode_varbinary_param(0b1111, <<length::big-16, param::binary-size(length + 1 + 65_536), rest::binary>>),
    do: {param, rest}

  defp decode_varbinary_param(f, _data), do: raise("Invalid varbinary format: #{f}")
end
