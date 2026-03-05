# scripts/cluster_flow.exs
# Based on livebooks/class_scheduling.livemd setup block

# Create a temporary folder to persist data to
# working_dir = Path.join(System.tmp_dir!(), "bedrock_#{:rand.uniform(99999)}")
working_dir = Path.join(System.tmp_dir!(), "bedrock_99999")
File.mkdir_p!(working_dir)

defmodule Tutorial.Cluster do
  use Bedrock.Cluster,
    otp_app: :tutorial_app,
    name: "tutorial",
    config: [
      # What is this node allowed to be? In Bedrock, nodes can have different capabilities. Some
      # might only store data, others might coordinate cluster operations, and some might handle
      # both. For production deployments, you'd typically split these roles across different
      # machines for better performance and reliability.
      capabilities: [:coordination, :log, :materializer],

      # Optional tracing of internal operations
      trace: [],

      # Where does coordinator, storage, log data go?
      coordinator: [path: working_dir],
      materializer: [path: working_dir],
      log: [path: working_dir]
    ]
end

IO.puts("[cluster_flow] working_dir: #{working_dir}")
IO.puts("[cluster_flow] node: #{node()}")

IO.puts("[cluster_flow] Starting cluster...")

{:ok, cluster_pid} =
  Supervisor.start_link(
    [{Tutorial.Cluster, []}],
    strategy: :one_for_one,
    name: :tutorial_cluster_sup
  )

IO.puts("[cluster_flow] Cluster started: #{inspect(cluster_pid)}")

defmodule Tutorial.Repo do
  use Bedrock.Repo, cluster: Tutorial.Cluster
end

IO.puts("[cluster_flow] Running test transaction...")

result =
  Tutorial.Repo.transact(fn ->
    Tutorial.Repo.put("test_key", "test_value_#{System.system_time(:millisecond)}")
    Tutorial.Repo.get("test_key")
  end)

IO.puts("[cluster_flow] Transaction result: #{inspect(result)}")
IO.puts("[cluster_flow] SUCCESS")
