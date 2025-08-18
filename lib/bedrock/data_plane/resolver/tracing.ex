defmodule Bedrock.DataPlane.Resolver.Tracing do
  @moduledoc false
  alias Bedrock.DataPlane.Version

  require Logger

  @spec handler_id() :: String.t()
  defp handler_id, do: "bedrock_trace_data_plane_resolver"

  @spec start() :: :ok | {:error, :already_exists}
  def start do
    :telemetry.attach_many(
      handler_id(),
      [
        [:bedrock, :resolver, :resolve_transactions, :received],
        [:bedrock, :resolver, :resolve_transactions, :processing],
        [:bedrock, :resolver, :resolve_transactions, :completed],
        [:bedrock, :resolver, :resolve_transactions, :reply_sent],
        [:bedrock, :resolver, :resolve_transactions, :validation_error],
        [:bedrock, :resolver, :resolve_transactions, :waiting_list],
        [:bedrock, :resolver, :resolve_transactions, :waiting_list_inserted],
        [:bedrock, :resolver, :resolve_transactions, :waiting_list_validation_error],
        [:bedrock, :resolver, :resolve_transactions, :waiting_resolved]
      ],
      &__MODULE__.handler/4,
      nil
    )
  end

  @spec stop() :: :ok | {:error, :not_found}
  def stop, do: :telemetry.detach(handler_id())

  @spec handler(list(atom()), map(), map(), term()) :: :ok
  def handler([:bedrock, :resolver, :resolve_transactions, event], measurements, metadata, _),
    do: log_event(event, measurements, metadata)

  @spec log_event(atom(), map(), map()) :: :ok
  def log_event(:received, measurements, metadata) do
    info(
      "Received #{measurements.transaction_count} transactions: last_version=#{Version.to_string(metadata.last_version)}, next_version=#{Version.to_string(metadata.next_version)}, resolver_last_version=#{Version.to_string(metadata.resolver_last_version)}"
    )
  end

  def log_event(:processing, measurements, metadata) do
    info(
      "Processing #{measurements.transaction_count} transactions: last_version=#{Version.to_string(metadata.last_version)}, next_version=#{Version.to_string(metadata.next_version)}"
    )
  end

  def log_event(:completed, measurements, metadata) do
    info(
      "Completed #{measurements.transaction_count} transactions (#{measurements.aborted_count} aborted): last_version=#{Version.to_string(metadata.last_version)}, next_version=#{Version.to_string(metadata.next_version)}, resolver_last_version_after=#{Version.to_string(metadata.resolver_last_version_after)}"
    )
  end

  def log_event(:reply_sent, measurements, metadata) do
    info(
      "Reply sent for #{measurements.transaction_count} transactions (#{measurements.aborted_count} aborted): last_version=#{Version.to_string(metadata.last_version)}, next_version=#{Version.to_string(metadata.next_version)}"
    )
  end

  def log_event(:validation_error, measurements, metadata) do
    info("Validation error for #{measurements.transaction_count} transactions: #{inspect(metadata.reason)}")
  end

  def log_event(:waiting_list, measurements, metadata) do
    info(
      "Adding #{measurements.transaction_count} transactions to waiting list: last_version=#{Version.to_string(metadata.last_version)} > resolver_last_version=#{Version.to_string(metadata.resolver_last_version)}"
    )
  end

  def log_event(:waiting_list_inserted, measurements, _metadata) do
    info(
      "Inserted #{measurements.transaction_count} transactions into waiting list (size: #{measurements.waiting_list_size})"
    )
  end

  def log_event(:waiting_list_validation_error, measurements, metadata) do
    info(
      "Waiting list validation error for #{measurements.transaction_count} transactions: #{inspect(metadata.reason)}"
    )
  end

  def log_event(:waiting_resolved, measurements, metadata) do
    info(
      "Resolved waiting transaction: #{measurements.transaction_count} transactions (#{measurements.aborted_count} aborted), next_version=#{Version.to_string(metadata.next_version)}, resolver_last_version_after=#{Version.to_string(metadata.resolver_last_version_after)}"
    )
  end

  defp info(message) do
    Logger.info("Bedrock Resolver: #{message}", ansi_color: :cyan)
  end
end
