defmodule Bedrock.Service.Controller.Data do
  @type t :: %__MODULE__{}
  defstruct ~w[
    cluster
    subsystem
    default_worker
    worker_supervisor_otp_name
    workers
    health
    otp_name
    path
    registry
    waiting_for_healthy
  ]a
end
