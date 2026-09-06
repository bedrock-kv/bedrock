defmodule Bedrock.DataPlane.Log.Shale.State do
  @moduledoc """
  Internal state struct for Shale log servers.
  """

  alias Bedrock.ControlPlane.Director
  alias Bedrock.DataPlane.Demux
  alias Bedrock.DataPlane.Log.Shale.Segment
  alias Bedrock.DataPlane.Log.Shale.SegmentRecycler
  alias Bedrock.DataPlane.Log.Shale.Writer
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.Service.Foreman
  alias Bedrock.Service.Worker

  @type mode :: :locked | :running | :recovering

  @type init_state :: :initialized | {:retrying, attempt :: pos_integer()}

  @type t :: %__MODULE__{
          init_state: init_state(),
          cluster: module(),
          director: Director.ref() | nil,
          epoch: Bedrock.epoch() | nil,
          recovery_authority: Bedrock.Service.RecoveryAuthority.input() | nil,
          recovery_control: Bedrock.Service.RecoveryControl.t(),
          id: Worker.id(),
          foreman: Foreman.ref(),
          path: String.t(),
          segment_loader: (String.t() -> {:ok, [Segment.t()]} | {:error, term()}) | nil,
          segment_recycler: SegmentRecycler.server() | nil,
          object_storage: module() | nil,
          demux: pid() | nil,
          demux_supervisor: pid() | nil,
          cut_interval_us: pos_integer() | nil,
          min_durable_version: Bedrock.version() | nil,
          #
          last_version: Bedrock.version(),
          writer: Writer.t() | nil,
          writer_opts: keyword(),
          active_segment: Segment.t() | nil,
          segments: [Segment.t()],
          pending_pushes: %{
            Bedrock.version() => %{
              authority: map(),
              transaction: Transaction.encoded(),
              waiters: [term()]
            }
          },
          replay_operation: nil | map(),
          #
          reject_pushes_above_lag_us: non_neg_integer() | nil,
          #
          mode: mode(),
          available_after: Bedrock.version(),
          oldest_version: Bedrock.version(),
          otp_name: Worker.otp_name(),
          params: %{
            default_pull_limit: pos_integer(),
            max_pull_limit: pos_integer()
          },
          waiting_pullers: %{
            Bedrock.version() => [
              {Bedrock.timestamp_in_ms(), reply_to_fn :: (any() -> :ok), opts :: [limit: integer(), timeout: timeout()]}
            ]
          }
        }
  defstruct init_state: :initialized,
            cluster: nil,
            director: nil,
            epoch: nil,
            recovery_authority: nil,
            recovery_control: nil,
            foreman: nil,
            id: nil,
            path: nil,
            segment_loader: nil,
            segment_recycler: nil,
            object_storage: nil,
            demux: nil,
            demux_supervisor: nil,
            cut_interval_us: nil,
            min_durable_version: nil,
            #
            last_version: nil,
            writer: nil,
            writer_opts: [],
            segments: [],
            active_segment: nil,
            pending_pushes: %{},
            replay_operation: nil,
            #
            reject_pushes_above_lag_us: nil,
            #
            mode: :locked,
            available_after: <<0::unsigned-big-64>>,
            oldest_version: nil,
            otp_name: nil,
            pending_transactions: %{},
            waiting_pullers: %{},
            params: %{
              default_pull_limit: 100,
              max_pull_limit: 500
            }

  @doc """
  The cut-interval width this log operates on, in microseconds of
  version-time.

  The single resolution point for the default. The WAL's segment-roll
  boundary and the Demux's cut boundary are the SAME boundary — a
  segment holds exactly one cut bucket, which is what lets trimming drop
  history at the cut cadence even though the active segment is
  trim-immune. Two independent reads of "the default" would hold only by
  coincidence, and would diverge the moment a log's Demux were
  configured with a different width.
  """
  @spec cut_interval_us(t()) :: pos_integer()
  def cut_interval_us(%__MODULE__{cut_interval_us: nil}), do: Demux.Server.default_cut_interval_us()
  def cut_interval_us(%__MODULE__{cut_interval_us: cut_interval_us}), do: cut_interval_us
end
