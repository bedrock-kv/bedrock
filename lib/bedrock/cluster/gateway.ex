defmodule Bedrock.Cluster.Gateway do
  @moduledoc """
  The `Bedrock.Cluster.Gateway` is the interface to a GenServer that is
  responsible for finding and holding the current coordinator and director for
  a cluster.
  """

  use Bedrock.Internal.GenServerApi, for: __MODULE__.Server

  @type ref :: pid() | atom() | {atom(), node()}

  @spec begin_transaction(
          gateway_ref :: ref(),
          opts :: [
            retry_count: pos_integer(),
            timeout_in_ms: Bedrock.timeout_in_ms()
          ]
        ) :: {:ok, transaction_pid :: pid()} | {:error, :timeout}
  def begin_transaction(gateway, opts \\ []),
    do: gateway |> call({:begin_transaction, opts}, opts[:timeout_in_ms] || :infinity)

  @doc """
  Renew the lease for a transaction based on the read version.
  """
  @spec renew_read_version_lease(
          gateway_ref :: ref(),
          read_version :: Bedrock.version(),
          opts :: [
            timeout_in_ms: Bedrock.timeout_in_ms()
          ]
        ) ::
          {:ok, lease_deadline_ms :: Bedrock.interval_in_ms()} | {:error, :lease_expired}
  def renew_read_version_lease(t, read_version, opts \\ []),
    do: t |> call({:renew_read_version_lease, read_version}, opts[:timeout_in_ms] || :infinity)

  @doc """
  Report the addition of a new worker to the cluster director. It does so by
  sending an asynchronous message to the specified gateway process. The gateway
  process will then gather some information from the worker and pass it to the
  cluster director.

  ## Parameters
    - `gateway`: The GenServer name or PID of the gateway that will handle the
      new worker information.
    - `worker`: The PID of the new worker process that has been added. It will
      be interrogated for details before passing it to the cluster director.

  ## Returns
    - `:ok`: Always returns `:ok` as the message is sent asynchronously.
  """
  @spec advertise_worker(gateway :: ref(), worker :: pid()) :: :ok
  def advertise_worker(gateway, worker),
    do: gateway |> cast({:advertise_worker, worker})

  @doc """
  Get the cluster descriptor from the gateway.
  This includes the coordinator nodes and other cluster configuration.
  """
  @spec get_descriptor(
          gateway :: ref(),
          opts :: [timeout_in_ms: Bedrock.timeout_in_ms()]
        ) :: {:ok, Bedrock.Cluster.Descriptor.t()} | {:error, :unavailable | :timeout | :unknown}
  def get_descriptor(gateway, opts \\ []),
    do: gateway |> call(:get_descriptor, opts[:timeout_in_ms] || 1000)
end
