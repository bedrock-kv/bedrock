defmodule Bedrock.DataPlane.Demux.ShardServer.State do
  @moduledoc false

  alias Bedrock.Internal.WaitingList
  alias Bedrock.ObjectStorage
  alias Bedrock.ObjectStorage.ChunkReader

  @type t :: %__MODULE__{
          shard_id: non_neg_integer(),
          demux: pid(),
          cluster: String.t(),
          object_storage: ObjectStorage.backend(),
          persistence_worker: pid(),
          chunk_reader: ChunkReader.t(),
          buffer: [{Bedrock.version(), binary()}],
          waiting_list: WaitingList.t(),
          flush_in_progress: boolean(),
          pending_cuts: [Bedrock.version()],
          pending_flush_cut: Bedrock.version() | nil,
          durable_version: Bedrock.version() | nil,
          latest_version: Bedrock.version() | nil,
          high_water: Bedrock.version() | nil,
          kcv: Bedrock.version() | nil
        }

  defstruct [
    :shard_id,
    :demux,
    :cluster,
    :object_storage,
    :persistence_worker,
    :chunk_reader,
    buffer: [],
    waiting_list: %{},
    flush_in_progress: false,
    pending_cuts: [],
    pending_flush_cut: nil,
    durable_version: nil,
    latest_version: nil,
    high_water: nil,
    kcv: nil
  ]
end
