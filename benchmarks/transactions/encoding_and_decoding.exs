alias Bedrock.DataPlane.Log.EncodedTransaction

defmodule RandomTransactionGenerator do
  def generate(num_items \\ 10, key_length \\ 5, value_length \\ 10) do
    version = :rand.uniform(1_000_000)

    key_values =
      1..num_items
      |> Map.new(fn _ ->
        {generate_random_binary(key_length), generate_random_binary(value_length)}
      end)

    {version, key_values}
  end

  def generate_encoded(num_items \\ 10, key_length \\ 5, value_length \\ 10),
    do: EncodedTransaction.encode(generate(num_items, key_length, value_length))

  defp generate_random_binary(length), do: :crypto.strong_rand_bytes(length)
end

Benchee.run(
  %{
    "encode" => fn input -> EncodedTransaction.encode(input) end
  },
  inputs: %{
    "Small" => RandomTransactionGenerator.generate(10),
    "Medium" => RandomTransactionGenerator.generate(100),
    "Bigger" => RandomTransactionGenerator.generate(1_000)
  }
)

Benchee.run(
  %{
    "decode" => fn input -> EncodedTransaction.decode!(input) end
  },
  inputs: %{
    "Small" => RandomTransactionGenerator.generate_encoded(10),
    "Medium" => RandomTransactionGenerator.generate_encoded(100),
    "Bigger" => RandomTransactionGenerator.generate_encoded(1_000)
  }
)
