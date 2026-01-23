# scripts/inject.exs
# Inject and execute code on a running debug node
#
# Usage:
#   mix run scripts/inject.exs "Process.list() |> length()"
#   mix run scripts/inject.exs ":sys.get_state(Process.whereis(SomeServer))"
#   mix run scripts/inject.exs "Debug.Cluster |> :sys.get_state()"
#
# Options:
#   --node NODE    Target node name (default: auto-detect from running debug_* nodes)
#   --timeout MS   RPC timeout in milliseconds (default: 30000)
#
# The script starts its own distributed node to connect to the target.

{opts, args, _} =
  OptionParser.parse(System.argv(),
    switches: [node: :string, timeout: :integer],
    aliases: [n: :node, t: :timeout]
  )

timeout = Keyword.get(opts, :timeout, 30_000)
code = Enum.join(args, " ")

if code == "" do
  IO.puts("Usage: mix run scripts/inject.exs \"<elixir code>\"")
  IO.puts("")
  IO.puts("Examples:")
  IO.puts("  mix run scripts/inject.exs \"Process.list() |> length()\"")
  IO.puts("  mix run scripts/inject.exs \":sys.get_state(Process.whereis(SomeServer))\"")
  System.halt(1)
end

defmodule Injector do
  @cookie :bedrock_debug
  @default_working_dir Path.join(System.tmp_dir!(), "bedrock_debug_cluster")

  def find_target_node(explicit_node) do
    case explicit_node do
      nil ->
        # Auto-detect: read from .debug_node file
        node_file = Path.join(@default_working_dir, ".debug_node")

        case File.read(node_file) do
          {:ok, node_str} ->
            {:ok, String.to_atom(String.trim(node_str))}

          {:error, :enoent} ->
            {:error, "No debug node file found at #{node_file}. Start one with: mix run scripts/debug_cluster.exs"}

          {:error, reason} ->
            {:error, "Failed to read node file: #{inspect(reason)}"}
        end

      node_str ->
        {:ok, String.to_atom(node_str)}
    end
  end

  def start_distribution do
    # Start this node with a unique name
    node_name = :"injector_#{:rand.uniform(99999)}@#{elem(:inet.gethostname(), 1)}"

    case Node.start(node_name, :shortnames) do
      {:ok, _} ->
        Node.set_cookie(@cookie)
        :ok

      {:error, reason} ->
        {:error, "Failed to start distribution: #{inspect(reason)}"}
    end
  end

  def inject(target_node, code, timeout) do
    IO.puts("[inject] Connecting to #{target_node}...")

    case Node.connect(target_node) do
      true ->
        IO.puts("[inject] Connected. Executing code...")
        IO.puts("[inject] Code: #{code}")
        IO.puts("")

        try do
          # Use :erpc.call with MFA to avoid anonymous function serialization issues
          # Code.eval_string is called on the remote node with the code string
          result = :erpc.call(target_node, Code, :eval_string, [code], timeout)
          {value, _binding} = result

          IO.puts("Result:")
          IO.puts(inspect(value, pretty: true, limit: :infinity, width: 120))
          :ok
        rescue
          e ->
            IO.puts("Error: #{inspect(e)}")
            {:error, e}
        end

      false ->
        IO.puts("[inject] Failed to connect to #{target_node}")
        IO.puts("[inject] Make sure the target node is running and using cookie: #{@cookie}")
        {:error, :connection_failed}

      :ignored ->
        IO.puts("[inject] Connection ignored (local node not alive?)")
        {:error, :not_alive}
    end
  end
end

# Find target node
case Injector.find_target_node(opts[:node]) do
  {:ok, target_node} ->
    # Start distribution on this node
    case Injector.start_distribution() do
      :ok ->
        case Injector.inject(target_node, code, timeout) do
          :ok -> :ok
          {:error, _} -> System.halt(1)
        end

      {:error, msg} ->
        IO.puts("Error: #{msg}")
        System.halt(1)
    end

  {:error, msg} ->
    IO.puts("Error: #{msg}")
    System.halt(1)
end
