defmodule Bedrock.MixProject do
  use Mix.Project

  def project do
    [
      app: :bedrock,
      version: "0.3.1",
      elixir: "~> 1.15",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      docs: &docs/0,
      test_coverage: [tool: ExCoveralls],
      preferred_cli_env: [
        coveralls: :test,
        "coveralls.json": :test,
        dialyzer: :dev
      ],
      elixirc_paths: elixirc_paths(Mix.env()),
      dialyzer: dialyzer(),
      aliases: aliases()
    ]
  end

  defp aliases, do: [quality: ["format --check-formatted", "credo --strict", "dialyzer"]]

  defp dialyzer do
    [
      plt_core_path: "priv/plts",
      plt_file: {:no_warn, "priv/plts/dialyzer.plt"},
      plt_add_apps: [:ex_unit, :mix]
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
    add_deps_for_dev_and_test([
      {:bedrock_raft, git: "https://github.com/jallum/bedrock_raft.git", tag: "0.9.6"},
      {:jason, "~> 1.4"},
      {:telemetry, "~> 1.2"}
    ])
  end

  def add_deps_for_dev_and_test(deps) do
    deps ++
      [
        {:stream_data, "~> 1.1", only: :test},
        {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
        {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
        {:faker, "~> 0.17", only: :test},
        {:mix_test_watch, "~> 1.0", only: [:dev, :test], runtime: false},
        {:mox, "~> 1.1", only: :test},
        {:excoveralls, "~> 0.18", only: :test},
        {:benchee, "~> 1.3", only: :dev},
        {:ex_doc, "~> 0.34", only: :dev, runtime: false, warn_if_outdated: true},
        {:styler, "~> 1.0", only: [:dev, :test], runtime: false}
      ]
  end

  defp docs do
    [
      main: "Bedrock",
      extras: ["README.md"]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]
end
