alias Bedrock.DataPlane.EncodedTransaction, as: V2
alias Bedrock.DataPlane.Version

# V0 Row-based Original Implementation (inline for comparison)
defmodule V0 do
  def encode(transaction) do
    transaction
    |> iodata_encode()
    |> IO.iodata_to_binary()
  end

  def iodata_encode({version, writes}) do
    sorted_pairs = writes |> Enum.sort_by(fn {key, _value} -> key end)
    iodata_encode_presorted({version, sorted_pairs})
  end

  def encode_presorted(presorted_transaction) do
    presorted_transaction
    |> iodata_encode_presorted()
    |> IO.iodata_to_binary()
  end

  def iodata_encode_presorted({version, sorted_pairs}) do
    sorted_pairs
    |> encode_key_value_frames_from_pairs()
    |> wrap_with_version_and_crc32(version)
  end

  defp encode_key_value_frames_from_pairs([]), do: []

  defp encode_key_value_frames_from_pairs([{key, value} | pairs]) do
    [encode_key_value_frame(key, value) | encode_key_value_frames_from_pairs(pairs)]
  end

  defp encode_key_value_frame(key, value) do
    n_key_bytes = byte_size(key)
    n_value_bytes = byte_size(value)
    n_bytes = n_value_bytes + n_key_bytes + 2
    [<<n_bytes::unsigned-big-32, n_key_bytes::unsigned-big-16>>, key, value]
  end

  def decode!(
        <<version::binary-size(8), size_in_bytes::unsigned-big-32,
          payload::binary-size(size_in_bytes), crc32::unsigned-big-32>>
      ) do
    if crc32 != :erlang.crc32(payload) do
      raise("CRC32 mismatch")
    else
      {version, decode_key_value_frames(payload)}
    end
  end

  defp decode_key_value_frames(<<>>), do: %{}

  defp decode_key_value_frames(
         <<n_bytes::unsigned-big-32, n_key_bytes::unsigned-big-16, key::binary-size(n_key_bytes),
           value::binary-size(n_bytes - n_key_bytes - 2), remainder::binary>>
       ) do
    remainder
    |> decode_key_value_frames()
    |> Map.put(key, value)
  end

  def keys(
        <<_version::unsigned-big-64, size_in_bytes::unsigned-big-32,
          payload::binary-size(size_in_bytes), _crc32::unsigned-big-32>>
      ) do
    extract_keys_from_payload(payload, [])
  end

  defp extract_keys_from_payload(<<>>, acc), do: Enum.reverse(acc)

  defp extract_keys_from_payload(
         <<n_bytes::unsigned-big-32, n_key_bytes::unsigned-big-16, key::binary-size(n_key_bytes),
           _value::binary-size(n_bytes - n_key_bytes - 2), remainder::binary>>,
         acc
       ) do
    extract_keys_from_payload(remainder, [key | acc])
  end

  defp wrap_with_version_and_crc32(kv_frames, version),
    do: [
      [version, <<IO.iodata_length(kv_frames)::unsigned-big-32>>],
      kv_frames,
      <<:erlang.crc32(kv_frames)::unsigned-big-32>>
    ]
end

defmodule RandomTransactionGenerator do
  def generate(num_items \\ 10, key_length \\ 5, value_length \\ 10) do
    version = Version.from_integer(:rand.uniform(1_000_000))

    key_values =
      1..num_items
      |> Map.new(fn _ ->
        {generate_random_binary(key_length), generate_random_binary(value_length)}
      end)

    {version, key_values}
  end

  def generate_presorted(num_items \\ 10, key_length \\ 5, value_length \\ 10) do
    version = Version.from_integer(:rand.uniform(1_000_000))

    sorted_pairs =
      1..num_items
      |> Enum.map(fn _ ->
        {generate_random_binary(key_length), generate_random_binary(value_length)}
      end)
      |> Enum.sort_by(fn {key, _value} -> key end)

    {version, sorted_pairs}
  end

  def generate_v0_encoded(num_items \\ 10, key_length \\ 5, value_length \\ 10),
    do: V0.encode(generate(num_items, key_length, value_length))

  def generate_columnar_encoded(num_items \\ 10, key_length \\ 5, value_length \\ 10),
    do: V2.encode(generate(num_items, key_length, value_length))

  defp generate_random_binary(length), do: :crypto.strong_rand_bytes(length)
end

IO.puts("=== V0 vs V2 Encoding Benchmark ===")

# Generate shared raw transaction data once
shared_raw_transactions = %{
  "Small (10 keys)" => RandomTransactionGenerator.generate_presorted(10),
  "Medium (100 keys)" => RandomTransactionGenerator.generate_presorted(100),
  "Large (1,000 keys)" => RandomTransactionGenerator.generate_presorted(1_000),
  "XLarge (10,000 keys)" => RandomTransactionGenerator.generate_presorted(10_000)
}

# Encoding benchmark - V0 vs V2 using presorted data (no sorting overhead)
Benchee.run(
  %{
    "V0 encode (row-based)" => fn input -> V0.encode_presorted(input) end,
    "V2 encode (columnar)" => fn input -> V2.encode_presorted(input) end
  },
  inputs: shared_raw_transactions,
  time: 3
)

IO.puts("")
IO.puts("=== Sorting Overhead Comparison ===")

# Compare with and without sorting for V2 (using Medium size)
medium_raw_tx = shared_raw_transactions["Medium (100 keys)"]
medium_map_tx = {elem(medium_raw_tx, 0), Map.new(elem(medium_raw_tx, 1))}

Benchee.run(
  %{
    "V2 encode with sorting" => fn _ -> V2.encode(medium_map_tx) end,
    "V2 encode presorted" => fn _ -> V2.encode_presorted(medium_raw_tx) end
  },
  inputs: %{"Medium (100 keys)" => :dummy},
  time: 2,
  print: [configuration: false]
)

IO.puts("")
IO.puts("=== Decoding Performance - V0 vs V2 Direct Comparison ===")

# Convert to map format for encoding
base_transactions_for_encoding =
  Map.new(shared_raw_transactions, fn {name, {version, sorted_pairs}} ->
    {name, {version, Map.new(sorted_pairs)}}
  end)

# Create tagged inputs with both V0 and V2 encoded data
tagged_decode_inputs =
  Map.new(base_transactions_for_encoding, fn {name, tx} ->
    {name,
     %{
       "V0" => V0.encode(tx),
       "V2" => V2.encode(tx)
     }}
  end)

Benchee.run(
  %{
    "V0 decode (row-based)" => fn tagged_data -> V0.decode!(tagged_data["V0"]) end,
    "V2 decode (columnar)" => fn tagged_data -> V2.decode!(tagged_data["V2"]) end
  },
  inputs: tagged_decode_inputs,
  time: 2
)

IO.puts("")
IO.puts("=== Key Extraction Performance - V0 vs V2 Direct Comparison ===")

Benchee.run(
  %{
    "V0 keys (row-based)" => fn tagged_data -> V0.keys(tagged_data["V0"]) end,
    "V2 keys (columnar)" => fn tagged_data -> V2.keys(tagged_data["V2"]) end
  },
  inputs: tagged_decode_inputs,
  time: 2
)

IO.puts("")
IO.puts("=== Format Size Comparison ===")

IO.puts("| Keys | V0 Size | V2 Size | Overhead |")
IO.puts("|------|---------|---------|----------|")

size_transactions = [
  {10, shared_raw_transactions["Small (10 keys)"]},
  {100, shared_raw_transactions["Medium (100 keys)"]},
  {1000, shared_raw_transactions["Large (1,000 keys)"]},
  {10000, shared_raw_transactions["XLarge (10,000 keys)"]}
]

for {size, raw_tx} <- size_transactions do
  v0_encoded = V0.encode_presorted(raw_tx)
  v2_encoded = V2.encode_presorted(raw_tx)

  overhead_bytes = byte_size(v2_encoded) - byte_size(v0_encoded)
  overhead_pct = Float.round(overhead_bytes / byte_size(v0_encoded) * 100, 1)

  IO.puts(
    "| #{size} | #{byte_size(v0_encoded)} | #{byte_size(v2_encoded)} | +#{overhead_bytes} (#{overhead_pct}%) |"
  )
end
