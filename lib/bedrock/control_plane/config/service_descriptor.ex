defmodule Bedrock.ControlPlane.Config.ServiceDescriptor do
  defstruct type: nil,
            otp_name: nil

  @type t :: %__MODULE__{
          type: atom(),
          otp_name: atom()
        }

  @spec new(atom(), atom()) :: t()
  def new(type, otp_name) do
    %__MODULE__{
      type: type,
      otp_name: otp_name
    }
  end
end
