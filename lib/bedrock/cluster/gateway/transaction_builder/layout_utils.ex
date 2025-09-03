defmodule Bedrock.Cluster.Gateway.TransactionBuilder.LayoutUtils do
  @moduledoc """
  Utilities for extracting information from Transaction System Layout.

  This module provides shared functionality for both individual key fetches
  and range queries to avoid duplication between the Fetching and RangeFetching modules.
  """

  alias Bedrock.Cluster.Gateway.TransactionBuilder.LayoutIndex
  alias Bedrock.ControlPlane.Config.TransactionSystemLayout
  alias Bedrock.KeyRange

  @doc """
  Extracts the PID of a storage server from the Transaction System Layout.

  Returns the PID if the storage server is up, nil otherwise.
  """
  @spec get_storage_server_pid(TransactionSystemLayout.t(), String.t()) :: pid() | nil
  def get_storage_server_pid(transaction_system_layout, storage_id) do
    transaction_system_layout.services
    |> Map.get(storage_id)
    |> case do
      %{kind: :storage, status: {:up, pid}} -> pid
      _ -> nil
    end
  end

  @doc """
  Builds a pre-computed index from the Transaction System Layout.

  This should be called once during initialization to build an efficient
  lookup structure for storage server queries.
  """
  @spec build_layout_index(TransactionSystemLayout.t()) :: LayoutIndex.t()
  def build_layout_index(transaction_system_layout) do
    LayoutIndex.build_index(transaction_system_layout)
  end

  @doc """
  Retrieves the set of storage servers responsible for a given key.

  Uses O(log n) indexed lookup with pre-computed segments.

  Returns a {key_range, [pid]} tuple for the storage team that contains the key.
  The tuple contains the key range and a list of server PIDs for that range.
  """
  @spec storage_servers_for_key(LayoutIndex.t(), key :: binary()) :: {Bedrock.key_range(), [pid()]}
  def storage_servers_for_key(index, key) do
    LayoutIndex.lookup_key!(index, key)
  end

  @doc """
  Retrieves storage servers that overlap with a given key range.

  Uses O(log n) indexed lookup with segmented results.

  Returns a list of {key_range, [pid]} tuples for storage teams that overlap
  with the specified range. Each tuple contains the key range and a list of
  server PIDs for that range.
  """
  @spec storage_servers_for_range(LayoutIndex.t(), {binary(), binary()}) :: [{Bedrock.key_range(), [pid()]}]
  def storage_servers_for_range(index, {start_key, end_key}) do
    LayoutIndex.lookup_range(index, start_key, end_key)
  end

  @spec ranges_overlap?(Bedrock.key_range(), Bedrock.key_range()) :: boolean()
  defdelegate ranges_overlap?(range1, range2), to: KeyRange, as: :overlap?
end
