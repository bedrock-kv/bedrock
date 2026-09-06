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
      # List form: merges with credo's default check set.
      # `{check, false}` disables a check; `{check, opts}` enables/configures one.
      checks: [
        # Disable the Logger metadata warning globally
        {Credo.Check.Warning.MissedMetadataKeyInLoggerConfig, false},

        # Disable checks incompatible with Elixir 1.18.3
        {Credo.Check.Refactor.MapInto, false},
        {Credo.Check.Warning.LazyLogging, false},

        # Disable some checks that may be too strict for this project
        {Credo.Check.Readability.MultiAlias, false},

        # Specs required for lib code only; test helpers are exempt (bedrock-qjp)
        {Credo.Check.Readability.Specs, files: %{excluded: [~r"(^|/)test/"]}},

      ]
    }
  ]
}
