defmodule Bedrock.ControlPlane.ClusterController.ServiceInfo do
  @type id :: String.t()
  @type kind :: :log | :storage
  @type otp_name :: atom()
  @type status :: :unknown | :down | {:up, pid(), otp_name(), node()}

  @type t :: %__MODULE__{
          id: id(),
          kind: kind(),
          status: status()
        }
  defstruct [:id, :kind, :status]

  @spec new(id(), kind(), status()) :: t()
  def new(id, kind, status \\ :unknown), do: %__MODULE__{id: id, kind: kind, status: status}

  @spec up(t(), pid(), otp_name(), node()) :: t()
  def up(t, pid, otp_name, node),
    do: %{t | status: {:up, pid, otp_name, node}}
end
