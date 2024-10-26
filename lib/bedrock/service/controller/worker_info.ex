defmodule Bedrock.Service.Controller.WorkerInfo do
  @type t :: %__MODULE__{}
  defstruct [
    :id,
    :health,
    :otp_name,
    :path,
    :pid
  ]
end
