Code.compile_file("mix_helpers.exs", Path.join(__DIR__, "../.."))

defmodule BedrockLogShale.MixProject do
  use Mix.Project
  use Bedrock.MixHelpers, app: :bedrock_log_shale

  @source_url "https://github.com/bedrock-kv/bedrock"

  def project do
    [
      app: :bedrock_log_shale,
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
    [extra_applications: [:logger]]
  end

  defp deps do
    [
      umbrella_dep(:bedrock_core),
      {:mox, "~> 1.1", only: :test}
    ]
  end

  defp description do
    "Shale log driver for Bedrock - write-ahead log implementation for distributed transactions."
  end

  defp package do
    [
      name: "bedrock_log_shale",
      licenses: ["MIT"],
      links: %{"GitHub" => @source_url},
      maintainers: ["Jason Allum"]
    ]
  end

  defp docs do
    [main: "Bedrock.DataPlane.Log.Shale", source_ref: "v#{@version}"]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]
end
