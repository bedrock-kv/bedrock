Code.compile_file("mix_helpers.exs", Path.join(__DIR__, "../.."))

defmodule BedrockJobQueue.MixProject do
  use Mix.Project
  use Bedrock.MixHelpers, app: :bedrock_job_queue

  @source_url "https://github.com/bedrock-kv/bedrock"

  def project do
    [
      app: :bedrock_job_queue,
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
    [
      extra_applications: [:logger, :crypto],
      mod: {Bedrock.JobQueue.Application, []}
    ]
  end

  defp deps do
    [
      umbrella_dep(:bedrock_core),
      umbrella_dep(:bedrock_directory),
      {:jason, "~> 1.4"},
      {:telemetry, "~> 1.2"},
      {:mox, "~> 1.1", only: :test},
      {:stream_data, "~> 1.1", only: :test}
    ]
  end

  defp description do
    "Durable job queue for Bedrock - transactional, priority-ordered job processing modeled after Apple's QuiCK."
  end

  defp package do
    [
      name: "bedrock_job_queue",
      licenses: ["MIT"],
      links: %{"GitHub" => @source_url},
      maintainers: ["Jason Allum"]
    ]
  end

  defp docs do
    [main: "Bedrock.JobQueue", source_ref: "v#{@version}"]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]
end
