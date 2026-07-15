defmodule Bedrock.ControlPlane.Distributor.State do
  @moduledoc """
  Internal state structure for the Distributor process.
  """

  alias Bedrock.ControlPlane.Config.TransactionSystemLayout

  @type t :: %__MODULE__{
          cluster: module(),
          epoch: Bedrock.epoch(),
          director: pid(),
          shard_layout: TransactionSystemLayout.shard_layout(),
          transaction_system_layout: TransactionSystemLayout.t() | %{},
          durable_version: Bedrock.version(),
          node_capabilities: %{Bedrock.Cluster.capability() => [node()]},
          materializer_monitors: %{reference() => {Bedrock.range_tag(), pid()}},
          placeholder: pid() | nil,
          placeholder_tags: MapSet.t(Bedrock.range_tag()),
          pending_demands: MapSet.t(Bedrock.range_tag()),
          healing: MapSet.t(Bedrock.range_tag()),
          backoff: %{Bedrock.range_tag() => expires_at_ms :: integer()},
          backoff_ms: pos_integer(),
          recruitment_overrides: map()
        }
  defstruct cluster: nil,
            epoch: nil,
            director: nil,
            shard_layout: %{},
            transaction_system_layout: %{},
            durable_version: nil,
            node_capabilities: %{},
            materializer_monitors: %{},
            placeholder: nil,
            placeholder_tags: MapSet.new(),
            pending_demands: MapSet.new(),
            healing: MapSet.new(),
            backoff: %{},
            backoff_ms: 5_000,
            recruitment_overrides: %{}

  @doc """
  Validates a caller-supplied epoch against the distributor's own epoch.

  Returns `:ok` when they match, `{:error, :epoch_superseded}` when the
  caller carries a newer epoch (this distributor is stale and must stop),
  and `{:error, :newer_epoch_exists}` when the caller carries an older
  epoch (the caller is stale).
  """
  @spec check_epoch(t(), Bedrock.epoch()) ::
          :ok | {:error, :epoch_superseded | :newer_epoch_exists}
  def check_epoch(%__MODULE__{epoch: epoch}, epoch), do: :ok

  def check_epoch(%__MODULE__{epoch: own_epoch}, caller_epoch) when caller_epoch > own_epoch,
    do: {:error, :epoch_superseded}

  def check_epoch(%__MODULE__{}, _caller_epoch), do: {:error, :newer_epoch_exists}
end
