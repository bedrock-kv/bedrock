defmodule Bedrock.Umbrella.MixProject do
  use Mix.Project

  def project do
    [
      apps_path: "apps",
      version: "0.4.0",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      dialyzer: dialyzer(),
      aliases: aliases(),
      test_coverage: [tool: ExCoveralls]
    ]
  end

  def cli do
    [
      preferred_envs: [
        coveralls: :test,
        "coveralls.json": :test,
        "coveralls.github": :test,
        dialyzer: :dev
      ]
    ]
  end

  defp aliases, do: [quality: ["format --check-formatted", "credo --strict", "dialyzer"]]

  defp dialyzer do
    [
      plt_core_path: "priv/plts",
      plt_file: {:no_warn, "priv/plts/dialyzer.plt"},
      plt_add_apps: [:ex_unit, :mix],
      # Disable opaque type checks due to OTP 28 issues with structs containing
      # MapSet/queue. See: https://github.com/elixir-lang/elixir/issues/14576
      flags: [:no_opaque]
    ]
  end

  # Shared dev/test dependencies for all apps
  defp deps do
    [
      {:stream_data, "~> 1.1", only: :test},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4.7", only: [:dev, :test], runtime: false},
      {:faker, "~> 0.17", only: :test},
      {:mix_test_watch, "~> 1.0", only: [:dev, :test], runtime: false},
      {:mox, "~> 1.1", only: :test},
      {:excoveralls, "~> 0.18", only: :test},
      {:benchee, "~> 1.3", only: :dev},
      {:ex_doc, "~> 0.39", only: :dev, runtime: false, warn_if_outdated: true},
      {:styler, "~> 1.0", only: [:dev, :test], runtime: false}
    ]
  end
end
