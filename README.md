# Bedrock

Bedrock is an embedded, distributed key-value store with guarantees beyond ACID.
It features consistent reads, strict serialization, transactions across the
key-space and a simple API. It is designed to store your data without cluttering
up _your_ code with storage concerns.

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
