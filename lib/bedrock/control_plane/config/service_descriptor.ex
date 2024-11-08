defmodule Bedrock.ControlPlane.Config.ServiceDescriptor do
  @type kind :: :log | :storage
  @type otp_name :: atom()
  @type status :: {:up, pid()} | :unknown | :down

  @type t :: %{
          kind: kind(),
          last_seen: {otp_name(), node()} | nil,
          status: status()
        }
end
