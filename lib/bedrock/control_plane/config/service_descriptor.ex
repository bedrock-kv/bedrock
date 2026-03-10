defmodule Bedrock.ControlPlane.Config.ServiceDescriptor do
  @moduledoc """
  Describes a service's metadata including its kind, status, and location.
  """

  @type kind :: :log | :materializer
  @type otp_name :: atom()
  @type status :: {:up, pid()} | :unknown | :down

  @type t :: %{
          kind: kind(),
          last_seen: {otp_name(), node()} | nil,
          status: status()
        }
end
