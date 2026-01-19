defmodule Bedrock.DataPlane.Demux.ShardServer.State do
  @moduledoc false

  alias Bedrock.Internal.WaitingList
  alias Bedrock.ObjectStorage
  alias Bedrock.ObjectStorage.ChunkReader
  alias Bedrock.ObjectStorage.ChunkWriter

  @type t :: %__MODULE__{
          shard_id: non_neg_integer(),
          demux: pid(),
          cluster: String.t(),
          object_storage: ObjectStorage.backend(),
          chunk_writer: ChunkWriter.t(),
          chunk_reader: ChunkReader.t(),
          version_gap: pos_integer(),
          buffer: [{Bedrock.version(), binary()}],
          waiting_list: WaitingList.t(),
          durable_version: Bedrock.version() | nil,
          latest_version: Bedrock.version() | nil
        }

  defstruct [
    :shard_id,
    :demux,
    :cluster,
    :object_storage,
    :chunk_writer,
    :chunk_reader,
    :version_gap,
    buffer: [],
    waiting_list: %{},
    durable_version: nil,
    latest_version: nil
  ]
end
