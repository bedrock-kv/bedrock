Code.compile_file("mix_helpers.exs", Path.join(__DIR__, "../.."))

defmodule Bedrock.MixProject do
  use Mix.Project
  use Bedrock.MixHelpers, app: :bedrock

  @source_url "https://github.com/bedrock-kv/bedrock"

  def project do
    [
      app: :bedrock,
      version: @version,
      build_path: "../../_build",
      config_path: "../../config/config.exs",
      deps_path: "../../deps",
      lockfile: "../../mix.lock",
      elixir: "~> 1.17",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description: description(),
      package: package(),
      docs: docs(),
      source_url: @source_url
    ]
  end

  def application do
    [extra_applications: [:logger]]
  end

  defp deps do
    [
      umbrella_dep(:bedrock_core),
      umbrella_dep(:bedrock_log_shale),
      umbrella_dep(:bedrock_storage_olivine)
    ]
  end

  defp description do
    "Bedrock - an embedded, distributed key-value store with ACID++ guarantees. This convenience package bundles the core library with default storage and log drivers."
  end

  defp package do
    [
      name: "bedrock",
      licenses: ["MIT"],
      links: %{
        "GitHub" => @source_url,
        "Livebook Example" =>
          "https://livebook.dev/run?url=https%3A%2F%2Fraw.githubusercontent.com%2Fbedrock-kv%2Fbedrock%2Frefs%2Fheads%2Fdevelop%2Flivebooks%2Fclass_scheduling.livemd"
      },
      maintainers: ["Jason Allum"]
    ]
  end

  defp docs do
    [main: "Bedrock", source_ref: "v#{@version}"]
  end
end
