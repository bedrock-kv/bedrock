# Bedrock

[![Elixir CI](https://github.com/jallum/bedrock/actions/workflows/elixir_ci.yaml/badge.svg)](https://github.com/jallum/bedrock/actions/workflows/elixir_ci.yaml)
[![Coverage Status](https://coveralls.io/repos/github/jallum/bedrock/badge.svg?branch=develop)](https://coveralls.io/github/jallum/bedrock?branch=develop)

Bedrock is an embedded, distributed key-value store with guarantees beyond ACID.
It features consistent reads, strict serialization, transactions across the
key-space and a simple API.

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed
by adding `bedrock` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:bedrock, "~> 0.1"}
  ]
end
```

## Example

[![Run in Livebook](https://livebook.dev/badge/v1/blue.svg)](https://livebook.dev/run?url=https%3A%2F%2Fraw.githubusercontent.com%2Fjallum%2Fbedrock%2Frefs%2Fheads%2Fdevelop%2Flivebooks%2Fexample_bank.livemd&rev=0.1.1)
