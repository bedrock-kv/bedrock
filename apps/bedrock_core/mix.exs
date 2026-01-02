Code.compile_file("mix_helpers.exs", Path.join(__DIR__, "../.."))

defmodule BedrockCore.MixProject do
  use Mix.Project
  use Bedrock.MixHelpers, app: :bedrock_core

  @source_url "https://github.com/bedrock-kv/bedrock"

  def project do
    [
      app: :bedrock_core,
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
      elixirc_paths: elixirc_paths(Mix.env()),
      source_url: @source_url,
      test_coverage: [tool: ExCoveralls]
    ]
  end

  def application do
    [extra_applications: [:logger, :crypto]]
  end

  defp deps do
    [
      {:bedrock_raft, "~> 0.9"},
      {:jason, "~> 1.4"},
      {:telemetry, "~> 1.2"},
      {:stream_data, "~> 1.1", only: :test}
    ]
  end

  defp description do
    "An embedded, distributed key-value store with guarantees beyond ACID, featuring consistent reads, strict serialization, and transactions across the key-space."
  end

  defp package do
    [
      name: "bedrock_core",
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
    [main: "Bedrock", extras: ["README.md"], source_ref: "v#{@version}"]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]
end
