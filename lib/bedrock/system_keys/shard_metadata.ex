defmodule Bedrock.SystemKeys.ShardMetadata do
  @moduledoc """
  FlatBuffer schema for shard metadata.

  The shard tag is encoded in the key path (`\\xff/system/shards/{tag}`),
  not in this value. Genealogy fields (parents, children) will be added
  later when implementing splits.

  ## Fields

    * `start_key` - Start of key range (inclusive)
    * `end_key` - End of key range (exclusive)
    * `born_at` - Version when shard was created
    * `ended_at` - Version when shard was split/merged (0 = active)

  ## Example

      # Encode
      binary = ShardMetadata.to_binary(%{
        start_key: "",
        end_key: "m",
        born_at: 100,
        ended_at: 0
      })

      # Decode
      {:ok, metadata} = ShardMetadata.read(binary)
  """

  use Flatbuffer, file: "priv/schemas/shard_metadata.fbs"

  @doc "Creates a new shard metadata binary"
  @spec new(start_key :: binary(), end_key :: binary(), born_at :: non_neg_integer()) :: binary()
  def new(start_key, end_key, born_at) do
    to_binary(%{
      start_key: :binary.bin_to_list(start_key),
      end_key: :binary.bin_to_list(end_key),
      born_at: born_at,
      ended_at: 0
    })
  end

  @doc "Checks if the shard is still active (not split/merged)"
  @spec active?(binary()) :: boolean()
  def active?(binary) do
    case read(binary) do
      {:ok, %{ended_at: 0}} -> true
      {:ok, %{ended_at: _}} -> false
      _ -> false
    end
  end
end
