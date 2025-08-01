%{
  configs: [
    %{
      name: "default",
      files: %{
        included: [
          "lib/",
          "src/",
          "test/",
          "apps/*/lib/",
          "apps/*/src/",
          "apps/*/test/",
        ],
        excluded: [~r"/_build/", ~r"/deps/"]
      },
      plugins: [],
      requires: [],
      strict: false,
      parse_timeout: 5000,
      color: true,
      checks: %{

        disabled: [
          # Disable the Logger metadata warning globally
          {Credo.Check.Warning.MissedMetadataKeyInLoggerConfig, []},

          # Disable checks incompatible with Elixir 1.18.3
          {Credo.Check.Refactor.MapInto, []},
          {Credo.Check.Warning.LazyLogging, []},

          # Disable some checks that may be too strict for this project
          {Credo.Check.Readability.MultiAlias, []},
          {Credo.Check.Readability.Specs, []}
        ]
      }
    }
  ]
}
