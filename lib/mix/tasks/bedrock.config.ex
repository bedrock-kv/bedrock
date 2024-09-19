defmodule Mix.Tasks.Bedrock.Config do
  use Mix.Task
  import Mix.Bedrock

  @shortdoc "Prints configuration"

  @moduledoc """
  Print configuration information about the given cluster.

  The clusters to interrogate are the ones specified under the
  `:bedrock_clusters` option in the current app configuration. However,
  if the `-c` option is given, it replaces the `:bedrock_clusters` config.

  Since Bedrock tasks can only be executed once, if you need to get status
  for multiple clusters, set `:bedrock_clusters` accordingly or pass the `-c`
  flag multiple times.

  ## Examples

      $ mix bedrock.config
      $ mix bedrock.config -r Custom.Cluster

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

  def run(argv) do
    {opts, _args} = OptionParser.parse!(argv, switches: switches(), aliases: aliases())

    opts
    |> Keyword.get_values(:cluster)
    |> parse_clusters()
    |> Enum.each(fn cluster ->
      IO.puts(
        "\n#{pretty_name(cluster)} (#{cluster.name()}): #{inspect(cluster.config(), limit: :infinity, pretty: true)}"
      )
    end)
  end

  def pretty_name(cluster) do
    cluster
    |> Module.split()
    |> Enum.join(".")
  end
end
