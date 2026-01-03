defmodule Bedrock.DataPlane.Storage.Olivine do
  @moduledoc """
  Olivine is a persistent key-value storage engine for Bedrock.

  Olivine provides versioned, ordered key-value storage with:
  - **MVCC**: Multiple versions per key for snapshot isolation
  - **Range queries**: Efficient iteration over key ranges
  - **Persistence**: Durable storage with crash recovery
  - **Compaction**: Background merging of historical versions

  ## Architecture

  Olivine uses a B+tree-like index structure stored in memory with data pages
  persisted to disk. The index maps keys to version histories, enabling
  efficient point lookups and range scans at any committed version.

  ## Usage

  Olivine is typically started as part of a Bedrock cluster and accessed through
  the `Bedrock.DataPlane.Storage` behaviour. Direct usage:

      {:ok, pid} = Bedrock.DataPlane.Storage.Olivine.child_spec(
        otp_name: :my_storage,
        foreman: foreman_pid,
        id: "storage_1",
        path: "/var/lib/bedrock/storage"
      ) |> start_child()

  """

  use Bedrock.Service.WorkerBehaviour, kind: :storage

  @doc false
  @spec child_spec(
          opts :: [
            otp_name: atom(),
            foreman: Bedrock.Service.Foreman.ref(),
            id: Bedrock.service_id(),
            path: Path.t()
          ]
        ) :: Supervisor.child_spec()
  defdelegate child_spec(opts), to: __MODULE__.Server
end
