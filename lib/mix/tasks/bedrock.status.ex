defmodule Mix.Tasks.Bedrock.Status do
  use Mix.Task
  import Mix.Bedrock

  @shortdoc "Prints status information"

  @moduledoc """
  Print status information about the given cluster.

  The clusters to interrogate are the ones specified under the
  `:bedrock_clusters` option in the current app configuration. However,
  if the `-c` option is given, it replaces the `:bedrock_clusters` config.

  Since Bedrock tasks can only be executed once, if you need to get status
  for multiple clusters, set `:bedrock_clusters` accordingly or pass the `-c`
  flag multiple times.

  ## Examples

      $ mix bedrock.status
      $ mix bedrock.status -r Custom.Cluster

  ## Command line options

    * `-c`, `--cluster` - the cluster to examine

  """

  defp switches,
    do: [
      cluster: [:string, :keep]
    ]

  defp aliases,
    do: [
      c: :cluster
    ]

  @spec run([String.t()]) :: [term()]
  def run(argv) do
    {opts, _args} = OptionParser.parse!(argv, switches: switches(), aliases: aliases())

    Application.ensure_all_started(:bedrock)

    clusters =
      opts
      |> Keyword.get_values(:cluster)
      |> parse_clusters()

    {:ok, _pid} =
      Supervisor.start_link(clusters, strategy: :one_for_one)
      |> case do
        {:ok, pid} -> {:ok, pid}
        {:error, {:already_started, pid}} -> {:ok, pid}
      end

    for cluster <- clusters do
      cluster.config()
      |> case do
        {:ok, config} -> config |> IO.inspect()
        {:error, reason} -> IO.puts("Error: #{reason}")
      end
    end
  end
end
