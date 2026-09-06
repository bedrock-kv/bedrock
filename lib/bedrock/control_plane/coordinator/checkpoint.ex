defmodule Bedrock.ControlPlane.Coordinator.Checkpoint do
  @moduledoc false
  alias Bedrock.ControlPlane.Coordinator.DiskRaftLog
  alias Bedrock.ControlPlane.Coordinator.Durability
  alias Bedrock.ControlPlane.Coordinator.State
  alias Bedrock.Raft.Log

  @fields [
    :cluster_id,
    :last_durable_txn_id,
    :generation_floor,
    :last_allocation,
    :service_directory,
    :node_capabilities
  ]

  @spec restore(State.t(), Log.t()) :: State.t()
  def restore(state, %DiskRaftLog{} = log) do
    case :dets.lookup(log.table_name, :coordinator_checkpoint) do
      [] ->
        state

      [{:coordinator_checkpoint, %{format_version: 1} = checkpoint}] ->
        if valid?(checkpoint, state, log) do
          struct!(state, Map.take(checkpoint, @fields))
        else
          exit({:invalid_coordinator_checkpoint, checkpoint})
        end

      _ ->
        exit(:unsupported_coordinator_checkpoint)
    end
  end

  def restore(state, _log), do: state

  @spec persist(State.t(), Log.t()) :: :ok
  def persist(state, %DiskRaftLog{} = log) do
    checkpoint = state |> Map.take(@fields) |> Map.put(:format_version, 1)

    with :ok <- :dets.insert(log.table_name, {:coordinator_checkpoint, checkpoint}),
         :ok <- DiskRaftLog.sync(log) do
      :ok
    else
      error -> exit({:coordinator_checkpoint_failed, error})
    end
  end

  def persist(_state, _log), do: :ok

  defp valid?(checkpoint, state, log) do
    Enum.all?(@fields, &Map.has_key?(checkpoint, &1)) and
      checkpoint.cluster_id == state.cluster_id and
      valid_floor?(checkpoint.generation_floor) and
      checkpoint.last_durable_txn_id <= Log.newest_safe_transaction_id(log) and
      Log.has_transaction_id?(log, checkpoint.last_durable_txn_id) and
      is_map(checkpoint.service_directory) and is_map(checkpoint.node_capabilities) and
      valid_allocation?(checkpoint.last_allocation, checkpoint.generation_floor) and
      matches_retained_prefix?(checkpoint, state, log)
  end

  defp valid_floor?(floor), do: is_integer(floor) and floor >= 0 and floor <= 0xFFFFFFFFFFFFFFFF

  defp matches_retained_prefix?(checkpoint, state, log) do
    replayed = Durability.replay_prefix(state, log, checkpoint.last_durable_txn_id)
    Map.take(replayed, @fields) == Map.take(checkpoint, @fields)
  end

  defp valid_allocation?(nil, 0), do: true

  defp valid_allocation?(%{generation: generation, owner_term: term, request_id: id}, generation),
    do: is_integer(term) and term > 0 and is_binary(id) and byte_size(id) > 0

  defp valid_allocation?(_, _), do: false
end
