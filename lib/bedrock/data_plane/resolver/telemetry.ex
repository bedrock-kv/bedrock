defmodule Bedrock.DataPlane.Resolver.Telemetry do
  @moduledoc """
  Telemetry utilities for resolver operations.
  """

  alias Bedrock.DataPlane.Version

  @spec emit_received(non_neg_integer(), Version.t(), Version.t(), Version.t()) :: :ok
  def emit_received(transaction_count, last_version, next_version, resolver_last_version) do
    :telemetry.execute(
      [:bedrock, :resolver, :resolve_transactions, :received],
      %{transaction_count: transaction_count},
      %{
        last_version: last_version,
        next_version: next_version,
        resolver_last_version: resolver_last_version
      }
    )
  end

  @spec emit_processing(non_neg_integer(), Version.t(), Version.t()) :: :ok
  def emit_processing(transaction_count, last_version, next_version) do
    :telemetry.execute(
      [:bedrock, :resolver, :resolve_transactions, :processing],
      %{transaction_count: transaction_count},
      %{last_version: last_version, next_version: next_version}
    )
  end

  @spec emit_completed(
          non_neg_integer(),
          non_neg_integer(),
          Version.t(),
          Version.t(),
          Version.t()
        ) :: :ok
  def emit_completed(transaction_count, aborted_count, last_version, next_version, resolver_last_version_after) do
    :telemetry.execute(
      [:bedrock, :resolver, :resolve_transactions, :completed],
      %{transaction_count: transaction_count, aborted_count: aborted_count},
      %{
        last_version: last_version,
        next_version: next_version,
        resolver_last_version_after: resolver_last_version_after
      }
    )
  end

  @spec emit_reply_sent(non_neg_integer(), non_neg_integer(), Version.t(), Version.t()) :: :ok
  def emit_reply_sent(transaction_count, aborted_count, last_version, next_version) do
    :telemetry.execute(
      [:bedrock, :resolver, :resolve_transactions, :reply_sent],
      %{transaction_count: transaction_count, aborted_count: aborted_count},
      %{last_version: last_version, next_version: next_version}
    )
  end

  @spec emit_waiting_list(non_neg_integer(), Version.t(), Version.t(), Version.t()) :: :ok
  def emit_waiting_list(transaction_count, last_version, next_version, resolver_last_version) do
    :telemetry.execute(
      [:bedrock, :resolver, :resolve_transactions, :waiting_list],
      %{transaction_count: transaction_count},
      %{
        last_version: last_version,
        next_version: next_version,
        resolver_last_version: resolver_last_version
      }
    )
  end

  @spec emit_waiting_list_inserted(
          non_neg_integer(),
          non_neg_integer(),
          Version.t(),
          Version.t(),
          Version.t()
        ) :: :ok
  def emit_waiting_list_inserted(
        transaction_count,
        waiting_list_size,
        last_version,
        next_version,
        resolver_last_version
      ) do
    :telemetry.execute(
      [:bedrock, :resolver, :resolve_transactions, :waiting_list_inserted],
      %{transaction_count: transaction_count, waiting_list_size: waiting_list_size},
      %{
        last_version: last_version,
        next_version: next_version,
        resolver_last_version: resolver_last_version
      }
    )
  end

  @spec emit_waiting_resolved(non_neg_integer(), non_neg_integer(), Version.t(), Version.t()) ::
          :ok
  def emit_waiting_resolved(transaction_count, aborted_count, next_version, resolver_last_version_after) do
    :telemetry.execute(
      [:bedrock, :resolver, :resolve_transactions, :waiting_resolved],
      %{transaction_count: transaction_count, aborted_count: aborted_count},
      %{next_version: next_version, resolver_last_version_after: resolver_last_version_after}
    )
  end

  @spec emit_validation_error(non_neg_integer(), term()) :: :ok
  def emit_validation_error(transaction_count, reason) do
    :telemetry.execute(
      [:bedrock, :resolver, :resolve_transactions, :validation_error],
      %{transaction_count: transaction_count},
      %{reason: reason}
    )
  end

  @spec emit_waiting_list_validation_error(non_neg_integer(), term()) :: :ok
  def emit_waiting_list_validation_error(transaction_count, reason) do
    :telemetry.execute(
      [:bedrock, :resolver, :resolve_transactions, :waiting_list_validation_error],
      %{transaction_count: transaction_count},
      %{reason: reason}
    )
  end
end
