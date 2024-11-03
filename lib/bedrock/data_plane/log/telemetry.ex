defmodule Bedrock.DataPlane.Log.Telemetry do
  alias Bedrock.Telemetry
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.DataPlane.Log

  def trace_log_started(cluster, id, otp_name) do
    Telemetry.execute([:bedrock, :data_plane, :log, :started], %{}, %{
      cluster: cluster,
      id: id,
      otp_name: otp_name
    })
  end

  def trace_log_lock_for_recovery(cluster, id, epoch) do
    Telemetry.execute([:bedrock, :data_plane, :log, :lock_for_recovery], %{}, %{
      cluster: cluster,
      id: id,
      epoch: epoch
    })
  end

  def trace_log_recover_from(cluster, id, source_log, first_version, last_version) do
    Telemetry.execute([:bedrock, :data_plane, :log, :recover_from], %{}, %{
      cluster: cluster,
      id: id,
      source_log: source_log,
      first_version: first_version,
      last_version: last_version
    })
  end

  @spec trace_log_push_transaction(
          cluster :: module(),
          Log.id(),
          Transaction.t(),
          expected_version :: Bedrock.version()
        ) :: :ok
  def trace_log_push_transaction(cluster, id, transaction, expected_version) do
    Telemetry.execute(
      [:bedrock, :data_plane, :log, :push],
      %{
        n_keys: map_size(transaction |> Transaction.key_values())
      },
      %{
        cluster: cluster,
        id: id,
        expected_version: expected_version,
        transaction: transaction
      }
    )
  end

  def trace_log_pull_transactions(cluster, id, from_version, opts) do
    Telemetry.execute([:bedrock, :data_plane, :log, :pull], %{}, %{
      cluster: cluster,
      id: id,
      from_version: from_version,
      opts: opts
    })
  end
end
