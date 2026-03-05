defmodule Bedrock.Service.Foreman.StartingWorkersTest do
  use ExUnit.Case, async: true

  alias Bedrock.Service.Foreman.StartingWorkers
  alias Bedrock.Service.Foreman.StartingWorkers.StartWorkerOp

  # Define mock modules at compile time
  defmodule MockWorker do
    @moduledoc false
    def child_spec(opts) do
      # Return the opts as the start tuple so we can inspect what was passed
      %{id: __MODULE__, start: {__MODULE__, :start_link, [opts]}}
    end
  end

  defmodule MockCluster do
    @moduledoc false
    def otp_name(:foreman), do: :test_foreman
    def otp_name(:worker_supervisor), do: :test_worker_supervisor
  end

  describe "build_child_spec/1" do
    test "includes object_storage in worker options" do
      mock_manifest = %{
        worker: MockWorker,
        params: %{}
      }

      object_storage = {Bedrock.ObjectStorage.LocalFilesystem, root: "/tmp/test"}

      op = %StartWorkerOp{
        id: "test-worker",
        path: "/tmp/workers/test-worker",
        otp_name: :test_worker,
        cluster: MockCluster,
        manifest: mock_manifest,
        object_storage: object_storage,
        error: nil
      }

      result = StartingWorkers.build_child_spec(op)

      # Extract the opts that were passed to the worker's child_spec
      %{start: {_mod, :start_link, [opts]}} = result.child_spec

      assert Keyword.get(opts, :object_storage) == object_storage
    end
  end
end
