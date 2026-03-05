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
          version_gap: pos_integer(),
          buffer: [{Bedrock.version(), binary()}],
          waiting_list: WaitingList.t(),
          flush_in_progress: boolean(),
          pending_flush_max_version: Bedrock.version() | nil,
          durable_version: Bedrock.version() | nil,
          latest_version: Bedrock.version() | nil
        }

  defstruct [
    :shard_id,
    :demux,
    :cluster,
    :object_storage,
    :persistence_worker,
    :chunk_reader,
    :version_gap,
    buffer: [],
    waiting_list: %{},
    flush_in_progress: false,
    pending_flush_max_version: nil,
    durable_version: nil,
    latest_version: nil
  ]
end
