defmodule Bedrock.Test.History.Gates do
  @moduledoc "One-shot deterministic pauses at real I/O boundaries; every operation still uses LocalFilesystem."
  @behaviour Bedrock.ObjectStorage

  alias Bedrock.ObjectStorage.LocalFilesystem

  def arm(gates, rule), do: Agent.update(gates, &(&1 |> normalize() |> Map.put(:rule, rule)))

  def disarm(gates) do
    active =
      Agent.get_and_update(gates, fn state ->
        state = normalize(state)
        {state.active, %{rule: nil, active: %{}}}
      end)

    Enum.each(active, fn {token, pid} -> send(pid, {:release_history_gate, token}) end)
    :ok
  end

  def pause(gates, stage, key) do
    token = make_ref()
    pid = self()

    owner =
      Agent.get_and_update(gates, fn state ->
        state = normalize(state)

        case state.rule do
          %{stage: ^stage, match: match, owner: owner} ->
            if match.(key), do: {owner, %{rule: nil, active: Map.put(state.active, token, pid)}}, else: {nil, state}

          _ ->
            {nil, state}
        end
      end)

    if owner do
      send(owner, {:history_gate, stage, self(), token, key})

      try do
        receive do
          {:release_history_gate, ^token} -> :ok
        after
          15_000 -> raise "history gate was not released: #{inspect({stage, key})}"
        end
      after
        Agent.update(gates, fn state -> Map.update!(normalize(state), :active, &Map.delete(&1, token)) end)
      end
    end
  end

  defp normalize(%{rule: _, active: _} = state), do: state
  defp normalize(rule), do: %{rule: rule, active: %{}}

  def log_event(_event, _measurements, %{transaction: transaction}, gates),
    do: pause(gates, :after_wal_sync, transaction)

  @impl true
  def put_if_not_exists(config, key, data, opts) do
    pause(config[:gates], :before_snapshot_publication, key)
    result = LocalFilesystem.put_if_not_exists(config, key, data, opts)
    pause(config[:gates], :after_snapshot_publication, key)
    result
  end

  @impl true
  def put(config, key, data, opts), do: LocalFilesystem.put(config, key, data, opts)
  @impl true
  def get(config, key), do: LocalFilesystem.get(config, key)
  @impl true
  def delete(config, key), do: LocalFilesystem.delete(config, key)
  @impl true
  def list(config, prefix, opts), do: LocalFilesystem.list(config, prefix, opts)
  @impl true
  def get_with_version(config, key), do: LocalFilesystem.get_with_version(config, key)
  @impl true
  def put_if_version_matches(config, key, data, version, opts),
    do: LocalFilesystem.put_if_version_matches(config, key, data, version, opts)
end
