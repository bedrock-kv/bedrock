defmodule Bedrock.MixProject do
  use Mix.Project

  def project do
    [
      app: :bedrock,
      version: "0.7.0",
      elixir: "~> 1.17",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      compilers: [:elixir_make] ++ Mix.compilers(),
      make_clean: ["clean"],
      description: description(),
      package: package(),
      docs: &docs/0,
      test_coverage: [tool: ExCoveralls],
      elixirc_paths: elixirc_paths(Mix.env()),
      dialyzer: dialyzer(),
      aliases: aliases(),
      source_url: "https://github.com/bedrock-kv/bedrock"
    ]
  end

  def cli do
    [
      preferred_envs: [
        coveralls: :test,
        "coveralls.json": :test,
        dialyzer: :dev
      ]
    ]
  end

  defp description do
    "An embedded, distributed key-value store with guarantees beyond ACID, featuring consistent reads, strict serialization, and transactions across the key-space."
  end

  defp package do
    [
      name: "bedrock",
      files:
        ~w(lib priv/schemas c_src Makefile guides/local-filesystem.md scripts/local_filesystem_smoke.exs mix.exs README.md CHANGELOG.md LICENSE .formatter.exs),
      licenses: ["MIT"],
      links: %{
        "GitHub" => "https://github.com/bedrock-kv/bedrock",
        "Livebook Example" =>
          "https://livebook.dev/run?url=https%3A%2F%2Fraw.githubusercontent.com%2Fbedrock-kv%2Fbedrock%2Frefs%2Fheads%2Fdevelop%2Flivebooks%2Fclass_scheduling.livemd"
      },
      maintainers: ["Jason Allum"]
    ]
  end

  defp aliases, do: [quality: ["format --check-formatted", "credo --strict", "dialyzer"]]

  defp dialyzer do
    [
      plt_core_path: "plts",
      plt_file: {:no_warn, "plts/dialyzer.plt"},
      plt_add_apps: [:ex_unit, :mix],
      # Disable opaque type checks due to OTP 28 issues with structs containing
      # MapSet/queue. See: https://github.com/elixir-lang/elixir/issues/14576
      flags: [:no_opaque]
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
      {:elixir_make, "~> 0.10", runtime: false},
      {:bedrock_raft, "~> 0.10"},
      {:flatbuffer, "~> 0.6"},
      {:jason, "~> 1.4"},
      {:telemetry, "~> 1.2"},
      {:ex_aws, "~> 2.7"},
      {:ex_aws_s3, "~> 2.5"},
      {:req, "~> 0.7"},
      {:sweet_xml, "~> 0.7"}
    ])
  end

  def add_deps_for_dev_and_test(deps) do
    deps ++
      [
        {:stream_data, "~> 1.1", only: :test},
        {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
        {:mix_audit, "~> 2.1", only: [:dev, :test], runtime: false},
        {:dialyxir, "~> 1.4.7", only: [:dev, :test], runtime: false},
        {:faker, "~> 0.17", only: :test},
        {:mix_test_watch, "~> 1.0", only: [:dev, :test], runtime: false},
        {:mox, "~> 1.1", only: :test},
        {:minio_server, "~> 0.4.0", only: [:dev, :test]},
        {:excoveralls, "~> 0.18", only: :test},
        {:benchee, "~> 1.3", only: :dev},
        {:ex_doc, "~> 0.39", only: :dev, runtime: false, warn_if_outdated: true},
        {:styler, "~> 1.0", only: [:dev, :test], runtime: false}
      ]
  end

  defp docs do
    [
      main: "Bedrock",
      # Named in prose by the architecture guides, but carrying
      # `@moduledoc false` on purpose. Listing them here documents that the
      # silence is deliberate, rather than leaving ex_doc to warn on each.
      skip_code_autolink_to: [
        "Bedrock.Cluster.Link.Server",
        "Bedrock.Cluster.Link.State",
        "Bedrock.Internal.GenServerApi"
      ],
      # Every guide the README links to has to be an extra, or ex_doc
      # resolves the link to nothing and it 404s on hexdocs.
      extras: [
        "README.md",
        "guides/quick-reads/users-perspective.md",
        "guides/quick-reads/transactions.md",
        "guides/quick-reads/transaction-format.md",
        "guides/quick-reads/data-plane.md",
        "guides/quick-reads/control-plane.md",
        "guides/quick-reads/transaction-system-layout.md",
        "guides/quick-reads/system-keyspace.md",
        "guides/quick-reads/recovery.md",
        "guides/deep-dives/architecture.md",
        "guides/deep-dives/transactions.md",
        "guides/deep-dives/cluster-startup.md",
        "guides/deep-dives/recovery.md",
        "guides/durability-foundation.md",
        "guides/durability-profile.md",
        "guides/object-storage-s3.md",
        "guides/local-filesystem.md",
        "guides/async-persistence-queue.md",
        "guides/distributed-durability-tests.md",
        "guides/glossary.md",
        "guides/ai-start-here.md",
        "guides/deep-dives/architecture/control-plane/coordinator.md",
        "guides/deep-dives/architecture/control-plane/director.md",
        "guides/deep-dives/architecture/data-plane/commit-proxy.md",
        "guides/deep-dives/architecture/data-plane/log.md",
        "guides/deep-dives/architecture/data-plane/resolver.md",
        "guides/deep-dives/architecture/data-plane/sequencer.md",
        "guides/deep-dives/architecture/data-plane/materializer.md",
        "guides/deep-dives/architecture/implementations/README.md",
        "guides/deep-dives/architecture/implementations/olivine.md",
        "guides/deep-dives/architecture/implementations/shale.md",
        "guides/deep-dives/architecture/infrastructure/cluster.md",
        "guides/deep-dives/architecture/infrastructure/foreman.md",
        "guides/deep-dives/architecture/infrastructure/link.md",
        "guides/deep-dives/architecture/infrastructure/transaction-builder.md",
        "guides/quick-reads/recovery/log-recovery-planning.md",
        "guides/quick-reads/recovery/log-recruitment.md",
        "guides/quick-reads/recovery/log-replay.md",
        "guides/quick-reads/recovery/materializer-bootstrap.md",
        "guides/quick-reads/recovery/monitoring.md",
        "guides/quick-reads/recovery/persistence.md",
        "guides/quick-reads/recovery/proxy-startup.md",
        "guides/quick-reads/recovery/resolver-startup.md",
        "guides/quick-reads/recovery/sequencer-startup.md",
        "guides/quick-reads/recovery/service-locking.md",
        "guides/quick-reads/recovery/transaction-system-layout.md",
        "guides/quick-reads/recovery/tsl-validation.md",
        "LICENSE"
      ],
      groups_for_extras: [
        "Recovery Phases": ~r"guides/quick-reads/recovery/",
        "Component Deep Dives": ~r"guides/deep-dives/architecture/",
        "Quick Reads": ~r"guides/quick-reads/",
        "Deep Dives": ~r"guides/deep-dives/",
        Durability: [
          "guides/durability-foundation.md",
          "guides/durability-profile.md",
          "guides/object-storage-s3.md",
          "guides/local-filesystem.md",
          "guides/async-persistence-queue.md",
          "guides/distributed-durability-tests.md"
        ],
        Reference: ["guides/glossary.md", "guides/ai-start-here.md", "LICENSE"]
      ]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]
end
