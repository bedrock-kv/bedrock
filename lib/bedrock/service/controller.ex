defmodule Bedrock.Service.Controller do
  alias Bedrock.Service.Worker

  use Bedrock.Internal.GenServerApi, for: __MODULE__.Server

  @type ref :: GenServer.server()

  @type health :: :ok | {:failed_to_start, :at_least_one_failed_to_start}

  @doc """
  Return a list of running workers.
  """
  @spec all(controller :: ref()) :: {:ok, [Worker.ref()]} | {:error, term()}
  def all(controller), do: call(controller, :workers, 5_000)

  @doc """
  Create a new worker.
  """
  @spec new_worker(controller :: ref(), id :: Worker.id(), kind :: :log | :storage) ::
          {:ok, Worker.ref()} | {:error, term()}
  def new_worker(controller, id, kind), do: call(controller, {:new_worker, id, kind}, 5_000)

  @doc """
  Wait until the controller signals that it (and all of it's workers) are
  reporting that they are healthy, or the timeout happens... whichever comes
  first.
  """
  @spec wait_for_healthy(controller :: ref(), timeout()) :: :ok | {:error, :unavailable}
  def wait_for_healthy(controller, timeout), do: call(controller, :wait_for_healthy, timeout)

  @doc """
  Called by a worker to report it's health to the controller.
  """
  @spec report_health(controller :: ref(), Worker.id(), any()) :: :ok
  def report_health(controller, worker_id, health),
    do: cast(controller, {:worker_health, worker_id, health})

  defmacro __using__(opts) do
    kind = opts[:kind] || raise "Missing :kind option."
    worker = opts[:worker] || raise "Missing :worker option."
    default_worker = opts[:default_worker] || raise "Missing :default_worker option."

    quote do
      alias Bedrock.Service.Controller

      @type ref :: Controller.ref()

      @spec all(controller :: ref()) :: {:ok, [unquote(worker).ref()]} | {:error, term()}
      defdelegate all(controller), to: Controller

      @spec new_worker(controller :: ref(), id :: Worker.id(), kind :: :log | :storage) ::
              {:ok, unquote(worker).ref()} | {:error, term()}
      defdelegate new_worker(controller, id, kind), to: Controller

      @spec wait_for_healthy(controller :: ref(), timeout()) :: :ok | {:error, :unavailable}
      defdelegate wait_for_healthy(controller, timeout), to: Controller

      @spec report_health(controller :: ref(), unquote(worker).id(), any()) :: :ok
      defdelegate report_health(controller, worker_id, health), to: Controller

      @doc false
      @spec child_spec(opts :: Keyword.t()) :: Supervisor.child_spec()
      def child_spec(opts) do
        cluster = Keyword.get(opts, :cluster) || raise "Missing :cluster option"

        path =
          Keyword.get(opts, :path) ||
            raise "Missing :path option; required when :log is specified in :services"

        default_worker =
          Keyword.get(opts, :default_worker) || unquote(default_worker)

        otp_name = cluster.otp_name(unquote(kind))

        worker_supervisor_otp_name = cluster.otp_name(:"#{otp_name}_worker_supervisor")

        children = [
          {DynamicSupervisor, name: worker_supervisor_otp_name},
          {Controller,
           [
             cluster: cluster,
             subsystem: :log,
             default_worker: default_worker,
             worker_supervisor_otp_name: worker_supervisor_otp_name,
             path: path,
             otp_name: otp_name
           ]}
        ]

        %{
          id: __MODULE__,
          start: {
            Supervisor,
            :start_link,
            [
              children,
              [strategy: :one_for_one]
            ]
          },
          restart: :permanent
        }
      end
    end
  end
end
