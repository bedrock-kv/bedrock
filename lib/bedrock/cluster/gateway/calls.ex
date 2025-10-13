defmodule Bedrock.Cluster.Gateway.Calls do
  @moduledoc false

  alias Bedrock.Cluster.Gateway.State
  alias Bedrock.Cluster.Gateway.TransactionBuilder
  alias Bedrock.ControlPlane.Config.TransactionSystemLayout
  alias Bedrock.ControlPlane.Coordinator

  @spec begin_transaction(State.t(), opts :: []) ::
          {State.t(), {:ok, pid()} | {:error, :unavailable}}
  def begin_transaction(t, _opts \\ []) do
    t
    |> ensure_current_tsl()
    |> case do
      {:ok, tsl, updated_state} ->
        [transaction_system_layout: tsl]
        |> TransactionBuilder.start_link()
        |> case do
          {:ok, pid} -> {updated_state, {:ok, pid}}
          {:error, reason} -> {updated_state, {:error, reason}}
        end

      {:error, reason} ->
        {t, {:error, reason}}
    end
  end

  @spec ensure_current_tsl(State.t()) ::
          {:ok, TransactionSystemLayout.t(), State.t()}
          | {:error, :unavailable}
  defp ensure_current_tsl(%{known_coordinator: :unavailable}), do: {:error, :unavailable}

  defp ensure_current_tsl(%{known_coordinator: _coordinator, transaction_system_layout: nil}),
    do: {:error, :unavailable}

  defp ensure_current_tsl(%{known_coordinator: _coordinator, transaction_system_layout: tsl} = t)
       when not is_nil(tsl) do
    # Use cached TSL (updated via push notifications)
    {:ok, tsl, t}
  end

  defp ensure_current_tsl(%{known_coordinator: coordinator} = t) do
    # No cached TSL - fetch from coordinator
    case Coordinator.fetch_transaction_system_layout(coordinator) do
      {:ok, tsl} -> {:ok, tsl, %{t | transaction_system_layout: tsl}}
      {:error, reason} -> {:error, reason}
    end
  end
end
