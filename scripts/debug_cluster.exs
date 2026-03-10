# scripts/debug_cluster.exs
# Orchestrates cluster debugging with distributed Erlang for code injection
#
# Usage:
#   mix run scripts/debug_cluster.exs --scenario=fresh
#   mix run scripts/debug_cluster.exs --scenario=reconnect --timeout=60000
#
# After timeout, the target node stays running for inspection:
#   mix run scripts/inject.exs "Process.list() |> length()"
#   mix run scripts/inject.exs ":sys.get_state(Process.whereis(SomeServer))"
#
# Scenarios:
#   fresh     - Clean working directory, single run
#   reconnect - Two runs with same working directory (simulates restart)

{opts, _, _} =
  OptionParser.parse(System.argv(),
    switches: [scenario: :string, timeout: :integer, working_dir: :string, keep_alive: :boolean],
    aliases: [s: :scenario, t: :timeout, w: :working_dir, k: :keep_alive]
  )

scenario = Keyword.get(opts, :scenario, "fresh")
timeout = Keyword.get(opts, :timeout, 30_000)
keep_alive = Keyword.get(opts, :keep_alive, true)

working_dir =
  Keyword.get(
    opts,
    :working_dir,
    Path.join(System.tmp_dir!(), "bedrock_debug_cluster")
  )

defmodule Orchestrator do
  @moduledoc false
  @cookie "bedrock_debug"

  def run_distributed(script_path, working_dir, timeout, opts \\ []) do
    node_name = "debug_#{:rand.uniform(99999)}"
    keep_alive = Keyword.get(opts, :keep_alive, true)

    # Get short hostname for node addressing
    {:ok, hostname} = :inet.gethostname()
    full_node = "#{node_name}@#{hostname}"

    IO.puts("[orchestrator] Starting distributed node: #{full_node}")
    IO.puts("[orchestrator] Cookie: #{@cookie}")
    IO.puts("[orchestrator] Timeout: #{timeout}ms")
    IO.puts("[orchestrator] Keep alive on timeout: #{keep_alive}")

    # Start with --sname and --cookie for distributed Erlang
    port =
      Port.open(
        {:spawn_executable, System.find_executable("elixir")},
        [
          :binary,
          :exit_status,
          :stderr_to_stdout,
          args: [
            "--sname", node_name,
            "--cookie", @cookie,
            "-S", "mix", "run", script_path,
            "--working-dir", working_dir
          ],
          cd: Path.dirname(Path.dirname(script_path))
        ]
      )

    # Write node info to file for inject.exs to find
    node_file = Path.join(working_dir, ".debug_node")
    File.write!(node_file, full_node)

    result = collect_output(port, timeout, System.monotonic_time(:millisecond), [], full_node)

    case result do
      {:error, :timeout} when keep_alive ->
        IO.puts("")
        IO.puts("[orchestrator] Target node still running: #{full_node}")
        IO.puts("[orchestrator] Inject code with:")
        IO.puts("  mix run scripts/inject.exs \"Process.list() |> length()\"")
        IO.puts("")
        IO.puts("[orchestrator] Kill with: pkill -f \"#{node_name}\"")
        {:error, :timeout, full_node}

      {:error, :timeout} ->
        kill_node(node_name)
        {:error, :timeout}

      other ->
        other
    end
  end

  defp collect_output(port, timeout, start_time, acc, node_name) do
    elapsed = System.monotonic_time(:millisecond) - start_time
    remaining = max(0, timeout - elapsed)

    receive do
      {^port, {:data, data}} ->
        IO.write(data)
        collect_output(port, timeout, start_time, [data | acc], node_name)

      {^port, {:exit_status, 0}} ->
        {:ok, IO.iodata_to_binary(Enum.reverse(acc))}

      {^port, {:exit_status, status}} ->
        {:error, {:exit, status, IO.iodata_to_binary(Enum.reverse(acc))}}
    after
      remaining ->
        {:error, :timeout}
    end
  end

  defp kill_node(node_name) do
    System.cmd("pkill", ["-f", node_name], stderr_to_stdout: true)
  end

  def cookie, do: @cookie
end

IO.puts("=== Cluster Debug Harness (Distributed) ===")
IO.puts("Scenario: #{scenario}")
IO.puts("Timeout: #{timeout}ms")
IO.puts("Working dir: #{working_dir}")
IO.puts("Keep alive: #{keep_alive}")
IO.puts("")

script_path = Path.join(__DIR__, "cluster_flow.exs")

result =
  case scenario do
    "fresh" ->
      IO.puts("--- Fresh Start (clean working directory) ---")
      File.rm_rf!(working_dir)
      File.mkdir_p!(working_dir)
      Orchestrator.run_distributed(script_path, working_dir, timeout, keep_alive: keep_alive)

    "reconnect" ->
      IO.puts("--- First Start (creating initial state) ---")
      File.rm_rf!(working_dir)
      File.mkdir_p!(working_dir)

      # First run with shorter timeout and no keep-alive
      case Orchestrator.run_distributed(script_path, working_dir, timeout, keep_alive: false) do
        {:ok, _} ->
          IO.puts("")
          IO.puts("--- Reconnect (reusing working directory) ---")
          Orchestrator.run_distributed(script_path, working_dir, timeout, keep_alive: keep_alive)

        {:error, :timeout} ->
          IO.puts("")
          IO.puts("First start timed out - cannot test reconnect")
          {:error, :timeout}

        error ->
          error
      end

    other ->
      {:error, "Unknown scenario: #{other}"}
  end

IO.puts("")

case result do
  {:ok, _} ->
    IO.puts("SCENARIO PASSED: #{scenario}")

  {:error, :timeout, node_name} ->
    IO.puts("TIMEOUT - cluster hung after #{timeout}ms")
    IO.puts("Target node running: #{node_name}")
    IO.puts("Working dir: #{working_dir}")
    System.halt(1)

  {:error, :timeout} ->
    IO.puts("TIMEOUT - cluster hung after #{timeout}ms")
    IO.puts("Working dir: #{working_dir}")
    System.halt(1)

  {:error, {:exit, status, _output}} ->
    IO.puts("PROCESS EXITED with status #{status}")
    System.halt(1)

  {:error, reason} ->
    IO.puts("ERROR: #{inspect(reason)}")
    System.halt(1)
end
