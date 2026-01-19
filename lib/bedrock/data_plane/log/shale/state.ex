defmodule Bedrock.DataPlane.Log.Shale.State do
  @moduledoc false

  alias Bedrock.ControlPlane.Director
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
          id: Worker.id(),
          foreman: Foreman.ref(),
          path: String.t(),
          segment_recycler: SegmentRecycler.server() | nil,
          object_storage: module() | nil,
          demux: pid() | nil,
          demux_supervisor: pid() | nil,
          min_durable_version: Bedrock.version() | nil,
          #
          last_version: Bedrock.version(),
          writer: Writer.t() | nil,
          active_segment: Segment.t() | nil,
          segments: [Segment.t()],
          pending_pushes: %{
            Bedrock.version() =>
              {encoded_transaction :: Transaction.encoded(), ack_fn :: (:ok | {:error, term()} -> :ok)}
          },
          #
          mode: mode(),
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
            foreman: nil,
            id: nil,
            path: nil,
            segment_recycler: nil,
            object_storage: nil,
            demux: nil,
            demux_supervisor: nil,
            min_durable_version: nil,
            #
            last_version: nil,
            writer: nil,
            segments: [],
            active_segment: nil,
            pending_pushes: %{},
            #
            mode: :locked,
            oldest_version: nil,
            otp_name: nil,
            pending_transactions: %{},
            waiting_pullers: %{},
            params: %{
              default_pull_limit: 100,
              max_pull_limit: 500
            }
end
