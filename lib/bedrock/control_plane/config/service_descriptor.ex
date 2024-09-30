defmodule Bedrock.ControlPlane.Config.ServiceDescriptor do
  @type t :: %__MODULE__{
          type: atom(),
          otp_name: atom()
        }
  defstruct type: nil,
            otp_name: nil

  @spec new(atom(), atom()) :: t()
  def new(type, otp_name) do
    %__MODULE__{
      type: type,
      otp_name: otp_name
    }
  end
end
