defmodule Bedrock.MixProject do
  use Mix.Project

  def project do
    [
      app: :bedrock,
      version: "0.1.0",
      elixir: "~> 1.15",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      test_coverage: [tool: ExCoveralls],
      preferred_cli_env: [
        coveralls: :test
      ],
      elixirc_paths: elixirc_paths(Mix.env())
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger, :crypto]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:gearbox, "~> 0.3"},
      {:jason, "~> 1.4"},
      {:telemetry, "~> 1.2"}
    ]
    |> add_deps_for_dev_and_test()
  end

  def add_deps_for_dev_and_test(deps) do
    deps ++
      [
        {:credo, "~> 1.6", only: [:dev, :test], runtime: false},
        {:faker, "~> 0.17", only: :test},
        {:mix_test_watch, "~> 1.0", only: [:dev, :test], runtime: false},
        {:mox, "~> 1.1", only: :test},
        {:excoveralls, "~> 0.18", only: :test}
      ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]
end
