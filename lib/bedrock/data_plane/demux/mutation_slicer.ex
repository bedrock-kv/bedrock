defmodule Bedrock.DataPlane.Demux.MutationSlicer do
  @moduledoc """
  Slices a transaction's mutations by shard using the SHARD_INDEX section.

  Given a transaction with mutations sorted by shard and a SHARD_INDEX section
  indicating how many mutations belong to each shard, this module produces
  per-shard "slices" - mini-transactions containing only that shard's mutations.

  ## Slice Format

  Each slice is a valid BRDT (transaction) binary containing:
  - Transaction header
  - MUTATIONS section with just that shard's mutations
  - COMMIT_VERSION section

  This format allows materializers to decode slices using standard Transaction functions.
  """

  alias Bedrock.DataPlane.Transaction

  @type shard_id :: non_neg_integer()
  @type version :: Bedrock.version()
  @type slice :: binary()

  @doc """
  Slices a transaction by shard, returning a list of `{shard_id, slice}` tuples.

  The transaction must have a SHARD_INDEX section indicating how mutations
  are distributed across shards. Each slice is a valid transaction binary
  containing only the mutations for that shard.

  ## Parameters

  - `transaction` - Encoded transaction binary with SHARD_INDEX
  - `commit_version` - The commit version to include in each slice (8-byte binary)

  ## Returns

  - `{:ok, [{shard_id, slice}]}` - List of shard slices, one per shard touched
  - `{:error, reason}` - If transaction is invalid or missing SHARD_INDEX

  ## Example

      {:ok, slices} = MutationSlicer.slice(encoded_txn, commit_version)
      # slices = [{0, slice_binary_0}, {2, slice_binary_2}, ...]
  """
  @spec slice(binary(), binary()) :: {:ok, [{shard_id(), slice()}]} | {:error, term()}
  def slice(transaction, commit_version) when is_binary(commit_version) and byte_size(commit_version) == 8 do
    with {:ok, shard_index} <- get_shard_index(transaction),
         {:ok, mutations_stream} <- Transaction.mutations(transaction) do
      slices = build_slices(shard_index, mutations_stream, commit_version)
      {:ok, slices}
    end
  end

  @doc """
  Same as `slice/2` but raises on error.
  """
  @spec slice!(binary(), binary()) :: [{shard_id(), slice()}]
  def slice!(transaction, commit_version) do
    case slice(transaction, commit_version) do
      {:ok, slices} -> slices
      {:error, reason} -> raise "Failed to slice transaction: #{inspect(reason)}"
    end
  end

  @doc """
  Extracts shard IDs from a transaction without full slicing.

  Useful for determining which shards a transaction touches without
  creating the full slice binaries.

  ## Returns

  - `{:ok, [shard_id]}` - List of shard IDs touched by the transaction
  - `{:error, reason}` - If SHARD_INDEX is missing or invalid
  """
  @spec touched_shards(binary()) :: {:ok, [shard_id()]} | {:error, term()}
  def touched_shards(transaction) do
    case Transaction.shard_index(transaction) do
      # Empty transactions touch no shards
      {:ok, nil} -> {:ok, []}
      {:ok, []} -> {:ok, []}
      {:ok, shard_index} -> {:ok, Enum.map(shard_index, fn {shard_id, _count} -> shard_id end)}
      {:error, reason} -> {:error, reason}
    end
  end

  # Private helpers

  defp get_shard_index(transaction) do
    case Transaction.shard_index(transaction) do
      # Empty transactions (heartbeats) have no shard_index - return empty list
      {:ok, nil} -> {:ok, []}
      {:ok, []} -> {:ok, []}
      {:ok, shard_index} -> {:ok, shard_index}
      {:error, reason} -> {:error, reason}
    end
  end

  defp build_slices(shard_index, mutations_stream, commit_version) do
    mutations_list = Enum.to_list(mutations_stream)
    build_slices_from_list(shard_index, mutations_list, commit_version, [])
  end

  defp build_slices_from_list([], _mutations, _commit_version, acc) do
    Enum.reverse(acc)
  end

  defp build_slices_from_list([{shard_id, count} | rest_index], mutations, commit_version, acc) do
    {shard_mutations, remaining_mutations} = Enum.split(mutations, count)

    slice = encode_slice(shard_mutations, commit_version)
    build_slices_from_list(rest_index, remaining_mutations, commit_version, [{shard_id, slice} | acc])
  end

  defp encode_slice(mutations, commit_version) do
    Transaction.encode(%{
      mutations: mutations,
      commit_version: commit_version
    })
  end
end
