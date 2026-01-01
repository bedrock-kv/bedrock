Code.compile_file("mix_helpers.exs", Path.join(__DIR__, "../.."))

defmodule BedrockDirectory.MixProject do
  use Mix.Project
  use Bedrock.MixHelpers, app: :bedrock_directory

  @source_url "https://github.com/bedrock-kv/bedrock"

  def project do
    [
      app: :bedrock_directory,
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
      source_url: @source_url
    ]
  end

  def application do
    [extra_applications: [:logger]]
  end

  defp deps do
    [
      umbrella_dep(:bedrock_core),
      umbrella_dep(:bedrock_hca),
      {:mox, "~> 1.1", only: :test}
    ]
  end

  defp description do
    "Directory layer for Bedrock - hierarchical namespace management based on FoundationDB directory layer."
  end

  defp package do
    [
      name: "bedrock_directory",
      licenses: ["MIT"],
      links: %{"GitHub" => @source_url},
      maintainers: ["Jason Allum"]
    ]
  end

  defp docs do
    [main: "Bedrock.Directory", source_ref: "v#{@version}"]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]
end
