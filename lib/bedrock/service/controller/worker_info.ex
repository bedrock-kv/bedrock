defmodule Bedrock.Service.Controller.WorkerInfo do
  @type t :: %__MODULE__{}
  defstruct [
    :id,
    :health,
    :otp_name,
    :path
  ]

  def update_health(worker_info, health), do: %{worker_info | health: health}
end
