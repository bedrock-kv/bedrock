defmodule Bedrock.DataPlane.Log.Shale.State do
  alias Bedrock.Service.Worker
  alias Bedrock.Service.Foreman
  alias Bedrock.ControlPlane.Director
  alias Bedrock.DataPlane.Log.Shale.Segment
  alias Bedrock.DataPlane.Log.Shale.Writer
  alias Bedrock.DataPlane.Log.Shale.SegmentRecycler

  @type mode :: :locked | :running

  @type t :: %__MODULE__{
          cluster: module(),
          director: Director.ref(),
          epoch: Bedrock.epoch(),
          id: Worker.id(),
          foreman: Foreman.ref(),
          path: String.t(),
          segment_recycler: SegmentRecycler.server(),
          #
          last_version: Bedrock.version(),
          writer: Writer.t() | nil,
          active_segment: Segment.t() | nil,
          segments: [Segment.t()],
          pending_pushes: %{
            Bedrock.version() =>
              {encoded_transaction :: binary(), ack_fn :: (:ok | {:error, term()} -> :ok)}
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
              {Bedrock.timestamp_in_ms(), reply_to_fn :: (any() -> :ok), opts :: keyword()}
            ]
          }
        }
  defstruct cluster: nil,
            director: nil,
            epoch: nil,
            foreman: nil,
            id: nil,
            path: nil,
            segment_recycler: nil,
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
