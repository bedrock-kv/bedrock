defmodule Bedrock.ControlPlane.Director.Recovery.CommitProxySelection do
  @moduledoc """
  Utilities for selecting available commit proxies during recovery.

  This module contains logic extracted from the recovery process to enable
  better testing of commit proxy selection behavior.
  """

  @doc """
  Get the first available commit proxy from a list of proxies.

  This function filters out non-PID entries and returns the first valid PID.
  It does not check if the PID is alive, following the "fail fast" philosophy
  where actual communication attempts will handle any connection issues.

  ## Examples

      iex> get_available_commit_proxy([])
      {:error, :no_commit_proxies}

      iex> pid = spawn(fn -> :ok end)
      iex> get_available_commit_proxy([nil, "invalid", pid])
      {:ok, pid}
  """
  @spec get_available_commit_proxy([term()]) :: {:ok, pid()} | {:error, :no_commit_proxies}
  def get_available_commit_proxy([]), do: {:error, :no_commit_proxies}
  def get_available_commit_proxy([proxy | _rest]) when is_pid(proxy), do: {:ok, proxy}
  def get_available_commit_proxy([_invalid | rest]), do: get_available_commit_proxy(rest)
end
