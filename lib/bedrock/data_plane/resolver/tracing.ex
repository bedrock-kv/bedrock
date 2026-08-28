defmodule Bedrock.DataPlane.Resolver.Tracing do
  @moduledoc false
  alias Bedrock.DataPlane.Version

  require Logger

  @spec handler_id() :: String.t()
  defp handler_id, do: "bedrock_trace_data_plane_resolver"

  # Exactly what `Resolver.Telemetry` emits. A subscription without an
  # emitter is dead weight; a handler clause that reads a key the emitter
  # never sends raises, and :telemetry detaches the whole handler on the
  # first event — so the two lists have to be kept in step.
  @spec start() :: :ok | {:error, :already_exists}
  def start do
    :telemetry.attach_many(
      handler_id(),
      [
        [:bedrock, :resolver, :resolve_transactions, :received],
        [:bedrock, :resolver, :resolve_transactions, :completed],
        [:bedrock, :resolver, :resolve_transactions, :waiting_list_inserted],
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
  def log_event(:received, %{transactions: transactions}, %{next_version: next_version}) do
    info("Received #{length(transactions)} transactions: next_version=#{Version.to_string(next_version)}")
  end

  def log_event(:completed, %{transactions: transactions, aborted: aborted}, %{next_version: next_version}) do
    info(
      "Completed #{length(transactions)} transactions (#{length(aborted)} aborted): next_version=#{Version.to_string(next_version)}"
    )
  end

  def log_event(:waiting_list_inserted, %{transactions: transactions, waiting_list: waiting_list}, %{
        next_version: next_version
      }) do
    info(
      "Inserted #{length(transactions)} transactions into waiting list (size: #{map_size(waiting_list)}): next_version=#{Version.to_string(next_version)}"
    )
  end

  # The emitter always passes an empty aborted list here — the aborts are
  # computed later, and reported by the :completed event this transaction
  # goes on to raise — so there is nothing to count.
  def log_event(:waiting_resolved, %{transactions: transactions}, %{next_version: next_version}) do
    info(
      "Resolved waiting transaction: #{length(transactions)} transactions, next_version=#{Version.to_string(next_version)}"
    )
  end

  defp info(message) do
    Logger.info("Bedrock Resolver: #{message}", ansi_color: :cyan)
  end
end
