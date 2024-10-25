defmodule Bedrock.Service.Controller.State do
  @type t :: %__MODULE__{}
  defstruct [
    :cluster,
    :default_worker,
    :health,
    :otp_name,
    :path,
    :registry,
    :subsystem,
    :waiting_for_healthy,
    :worker_supervisor_otp_name,
    :workers
  ]

  def new_state(%{
        subsystem: subsystem,
        cluster: cluster,
        path: path,
        default_worker: default_worker,
        worker_supervisor_otp_name: worker_supervisor_otp_name,
        otp_name: otp_name
      }) do
    {:ok,
     %__MODULE__{
       subsystem: subsystem,
       cluster: cluster,
       path: path,
       default_worker: default_worker,
       worker_supervisor_otp_name: worker_supervisor_otp_name,
       otp_name: otp_name,
       #
       health: :starting,
       waiting_for_healthy: [],
       workers: %{}
     }}
  end

  def new_state(_), do: {:error, :missing_required_params}
end
