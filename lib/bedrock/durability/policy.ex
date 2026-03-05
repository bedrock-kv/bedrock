defmodule Bedrock.Durability.Policy do
  @moduledoc """
  Policy enforcement for durability profile results.
  """

  alias Bedrock.Durability.Profile

  @type mode :: :strict | :relaxed

  @doc """
  Enforces durability checks according to mode.
  """
  @spec require(Profile.t(), mode()) :: :ok | {:error, {:durability_profile_failed, [atom()]}}
  def require(%Profile{status: :ok}, _mode), do: :ok

  def require(%Profile{status: :failed, reasons: reasons}, :strict) do
    {:error, {:durability_profile_failed, reasons}}
  end

  def require(%Profile{}, :relaxed), do: :ok
end
