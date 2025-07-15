defmodule Bedrock.ControlPlane.Director.Recovery.Shared do
  @moduledoc """
  Shared utilities for recovery phase modules.
  """

  @type starter_fn :: (Supervisor.child_spec(), node() ->
                         {:ok, pid()} | {:error, term()})

  @doc """
  Creates a starter function for supervised processes.

  Returns a function that can start child processes on specific nodes
  using the given supervisor OTP name.
  """
  @spec starter_for(atom()) :: starter_fn()
  def starter_for(supervisor_otp_name) do
    fn child_spec, node ->
      try do
        {supervisor_otp_name, node}
        |> DynamicSupervisor.start_child(child_spec)
        |> case do
          {:ok, pid} -> {:ok, pid}
          {:ok, pid, _} -> {:ok, pid}
          {:error, {:already_started, pid}} -> {:ok, pid}
          {:error, reason} -> {:error, reason}
        end
      catch
        :exit, reason -> {:error, {:supervisor_exit, reason}}
        :error, reason -> {:error, {:supervisor_error, reason}}
        reason -> {:error, {:unexpected_failure, reason}}
      end
    end
  end
end
