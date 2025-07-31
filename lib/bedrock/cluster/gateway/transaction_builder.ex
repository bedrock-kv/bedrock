defmodule Bedrock.Cluster.Gateway.TransactionBuilder do
  @moduledoc false

  alias Bedrock.Cluster.Gateway
  alias Bedrock.Cluster.Gateway.TransactionBuilder.State

  import __MODULE__.Committing, only: [do_commit: 1]
  import __MODULE__.Fetching, only: [do_fetch: 2]
  import __MODULE__.Putting, only: [do_put: 3]
  import __MODULE__.ReadVersions, only: [renew_read_version_lease: 1]

  @doc false
  @spec start_link(
          opts :: [
            gateway: Gateway.ref(),
            transaction_system_layout: Bedrock.ControlPlane.Config.TransactionSystemLayout.t(),
            key_codec: module(),
            value_codec: module()
          ]
        ) ::
          {:ok, pid()} | {:error, {:already_started, pid()}}
  def start_link(opts) do
    gateway = Keyword.fetch!(opts, :gateway)
    transaction_system_layout = Keyword.fetch!(opts, :transaction_system_layout)
    key_codec = Keyword.fetch!(opts, :key_codec)
    value_codec = Keyword.fetch!(opts, :value_codec)
    GenServer.start_link(__MODULE__, {gateway, transaction_system_layout, key_codec, value_codec})
  end

  use GenServer
  import Bedrock.Internal.GenServer.Replies

  @impl true
  def init(arg),
    do: {:ok, arg, {:continue, :initialization}}

  @impl true
  def handle_continue(
        :initialization,
        {gateway, transaction_system_layout, key_codec, value_codec}
      ) do
    %State{
      state: :valid,
      gateway: gateway,
      transaction_system_layout: transaction_system_layout,
      key_codec: key_codec,
      value_codec: value_codec
    }
    |> noreply()
  end

  def handle_continue(:stop, t), do: t |> stop(:normal)

  def handle_continue(:update_version_lease_if_needed, t) when is_nil(t.read_version),
    do: t |> noreply()

  def handle_continue(:update_version_lease_if_needed, t) do
    now = :erlang.monotonic_time(:millisecond)
    ms_remaining = t.read_version_lease_expiration - now

    cond do
      ms_remaining <= 0 -> %{t | state: :expired} |> noreply()
      ms_remaining < t.lease_renewal_threshold -> t |> renew_read_version_lease() |> noreply()
      true -> t |> noreply()
    end
  end

  @impl true
  def handle_call(:nested_transaction, _from, t) do
    %{t | stack: [{t.reads, t.writes} | t.stack], reads: %{}, writes: %{}}
    |> reply(:ok)
  end

  def handle_call(:commit, _from, t) do
    case do_commit(t) do
      {:ok, t} -> t |> reply({:ok, t.commit_version}, continue: :stop)
      {:error, _reason} = error -> t |> reply(error)
    end
  end

  def handle_call({:fetch, key}, _from, t) do
    case do_fetch(t, key) do
      {t, result} -> t |> reply(result, continue: :update_version_lease_if_needed)
    end
  end

  @impl true
  def handle_cast({:put, key, value}, t) do
    case do_put(t, key, value) do
      {:ok, t} -> t |> noreply()
      :key_error -> raise KeyError, "key must be a binary"
    end
  end

  def handle_cast(:rollback, t) do
    case do_rollback(t) do
      :stop -> t |> noreply(continue: :stop)
      t -> t |> noreply()
    end
  end

  @impl true
  def handle_info(:timeout, t), do: {:stop, :normal, t}

  @spec do_rollback(State.t()) :: :stop | State.t()
  def do_rollback(%{stack: []}), do: :stop
  def do_rollback(%{stack: [_ | stack]} = t), do: %{t | stack: stack}
end
