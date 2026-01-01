defmodule Bedrock.DataPlane.Log.Shale do
  @moduledoc """
  Shale is a write-ahead log (WAL) implementation for Bedrock.

  Shale provides durable, ordered storage of committed transactions that enables:
  - **Durability**: Transactions are persisted before acknowledgment
  - **Recovery**: Replaying the log restores storage state after crashes
  - **Replication**: Log entries can be streamed to storage replicas

  ## Architecture

  Shale writes transactions sequentially to segment files on disk. Each segment
  contains a contiguous range of transaction versions, enabling efficient
  sequential reads during recovery and replication.

  ## Usage

  Shale is typically started as part of a Bedrock cluster and accessed through
  the `Bedrock.DataPlane.Log` behaviour. Direct usage:

      {:ok, pid} = Bedrock.DataPlane.Log.Shale.child_spec(
        otp_name: :my_log,
        foreman: foreman_pid,
        id: "log_1",
        path: "/var/lib/bedrock/log"
      ) |> start_child()

  """

  use Bedrock.Service.WorkerBehaviour, kind: :log

  alias Bedrock.DataPlane.Log.Shale.Server

  @doc false
  defdelegate child_spec(opts), to: Server
end
