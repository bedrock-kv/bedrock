defmodule Bedrock.ControlPlane.Config.ServiceDescriptor do
  @moduledoc """
  A module representing a service descriptor within the Bedrock control plane configuration.

  This module provides a structure for defining a service descriptor, which includes an `id`,
  `type`, and `pid`. It is used to describe and instantiate service components within the
  Bedrock system.

  ## Types

    * `id` - Represents the unique identifier of the service, corresponding to a `String.t()`.
    * `type` - Represents the type of the service, corresponding to a `Bedrock.service()`.
    * `t` - A struct of the service descriptor containing `id`, `type`, and `pid`.

  ## Functions

    * `new/3` - Creates a new service descriptor with given `id`, `type`, and `pid`.
  """

  @type id :: Bedrock.service_id()
  @type type :: Bedrock.service()

  @type t :: %__MODULE__{
          id: id(),
          type: type(),
          pid: pid()
        }
  defstruct id: nil,
            type: nil,
            pid: nil

  @doc """
  Creates a new service descriptor.
  """
  @spec new(id(), type(), pid()) :: t()
  def new(id, type, pid) do
    %__MODULE__{
      id: id,
      type: type,
      pid: pid
    }
  end
end
