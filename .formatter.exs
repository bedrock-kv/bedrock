# Used by "mix format"
[
  line_length: 120,
  inputs: [
    "{mix,.formatter}.exs",
    "{config,benchmarks}/**/*.{ex,exs}",
    "apps/*/{mix,.formatter}.exs",
    "apps/*/{config,lib,test}/**/*.{ex,exs}"
  ],
  plugins: [Styler]
]
