defmodule Bedrock.DataPlane.Resolver.Telemetry do
  @moduledoc """
  Telemetry utilities for resolver operations.
  """

  alias Bedrock.DataPlane.Version

  @spec emit_received(list(), Version.t()) :: :ok
  def emit_received(transactions, next_version) do
    :telemetry.execute(
      [:bedrock, :resolver, :resolve_transactions, :received],
      %{transactions: transactions},
      %{next_version: next_version}
    )
  end

  @spec emit_completed(list(), list(), Version.t()) :: :ok
  def emit_completed(transactions, aborted, next_version) do
    :telemetry.execute(
      [:bedrock, :resolver, :resolve_transactions, :completed],
      %{transactions: transactions, aborted: aborted},
      %{next_version: next_version}
    )
  end

  @spec emit_waiting_list_inserted(list(), map(), Version.t()) :: :ok
  def emit_waiting_list_inserted(transactions, waiting_list, next_version) do
    :telemetry.execute(
      [:bedrock, :resolver, :resolve_transactions, :waiting_list_inserted],
      %{transactions: transactions, waiting_list: waiting_list},
      %{next_version: next_version}
    )
  end

  @spec emit_waiting_resolved(list(), list(), Version.t()) :: :ok
  def emit_waiting_resolved(transactions, aborted, next_version) do
    :telemetry.execute(
      [:bedrock, :resolver, :resolve_transactions, :waiting_resolved],
      %{transactions: transactions, aborted: aborted},
      %{next_version: next_version}
    )
  end
end
