defmodule Bedrock.Durability do
  @moduledoc """
  Durability profile contract for Bedrock clusters.

  This module exposes a small API that downstream integrations can call to:

  - inspect a cluster's durability profile (`profile/1`)
  - enforce profile requirements (`require/2`)
  """

  alias Bedrock.Durability.Policy
  alias Bedrock.Durability.Profile
  alias Bedrock.Durability.Telemetry

  @type target :: module() | keyword() | %{optional(:node_config) => keyword(), optional(:cluster_config) => map()}
  @type mode :: :strict | :relaxed

  @doc """
  Evaluates the durability profile for the given target.

  The target may be:

  - a cluster module (must export `node_config/0`, optionally `fetch_config/0`)
  - a node config keyword list
  - a map containing `:node_config` and optional `:cluster_config`
  """
  @spec profile(target()) :: Profile.t()
  def profile(target) do
    profile = Profile.evaluate(target)
    Telemetry.trace_profile(profile, telemetry_metadata(target))
    profile
  end

  @doc """
  Enforces durability profile requirements for the given mode.

  Returns `:ok` in `:relaxed` mode even when profile checks fail.
  Returns `{:error, {:durability_profile_failed, reasons}}` in `:strict` mode
  when requirements are not met.
  """
  @spec require(target(), mode()) :: :ok | {:error, {:durability_profile_failed, [atom()]}}
  def require(target, mode \\ :strict) do
    Policy.require(profile(target), mode)
  end

  defp telemetry_metadata(target) when is_atom(target) do
    %{
      target_type: :cluster_module,
      target_module: target
    }
  end

  defp telemetry_metadata(target) when is_list(target) do
    %{
      target_type: :node_config
    }
  end

  defp telemetry_metadata(_target) do
    %{
      target_type: :profile_input
    }
  end
end
