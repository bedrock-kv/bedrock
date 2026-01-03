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

  @spec emit_processing(list(), Version.t()) :: :ok
  def emit_processing(transactions, next_version) do
    :telemetry.execute(
      [:bedrock, :resolver, :resolve_transactions, :processing],
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

  @spec emit_reply_sent(list(), list(), Version.t()) :: :ok
  def emit_reply_sent(transactions, aborted, next_version) do
    :telemetry.execute(
      [:bedrock, :resolver, :resolve_transactions, :reply_sent],
      %{transactions: transactions, aborted: aborted},
      %{next_version: next_version}
    )
  end

  @spec emit_waiting_list(list(), Version.t()) :: :ok
  def emit_waiting_list(transactions, next_version) do
    :telemetry.execute(
      [:bedrock, :resolver, :resolve_transactions, :waiting_list],
      %{transactions: transactions},
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

  @spec emit_validation_error(list(), term()) :: :ok
  def emit_validation_error(transactions, reason) do
    :telemetry.execute(
      [:bedrock, :resolver, :resolve_transactions, :validation_error],
      %{transactions: transactions},
      %{reason: reason}
    )
  end

  @spec emit_waiting_list_validation_error(list(), term()) :: :ok
  def emit_waiting_list_validation_error(transactions, reason) do
    :telemetry.execute(
      [:bedrock, :resolver, :resolve_transactions, :waiting_list_validation_error],
      %{transactions: transactions},
      %{reason: reason}
    )
  end
end
