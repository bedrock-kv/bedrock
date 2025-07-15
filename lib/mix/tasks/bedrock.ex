defmodule Mix.Tasks.Bedrock do
  use Mix.Task

  @shortdoc "Prints Bedrock help information"

  @moduledoc """
  Prints Bedrock tasks and their information.

      $ mix bedrock

  """

  @spec run([String.t()]) :: :ok
  def run(argv) do
    {_opts, args} = OptionParser.parse!(argv, strict: [])

    case args do
      [] -> general()
      _ -> Mix.raise("Invalid arguments, expected: mix ecto")
    end
  end

  defp general do
    Application.ensure_all_started(:bedrock)
    Mix.shell().info("Bedrock v#{Application.spec(:bedrock, :vsn)}")
    Mix.shell().info("A distributed ACID compliant key-value storage system for Elixir.")
    Mix.shell().info("\nAvailable tasks:\n")
    Mix.Tasks.Help.run(["--search", "bedrock."])
  end
end
