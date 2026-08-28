defmodule Bedrock.Encoding do
  @moduledoc """
  How a `Bedrock.Keyspace` turns your keys and values into bytes, and back.

  Bedrock itself stores binaries and nothing else — the repo API is binary in,
  binary out. An encoding is what lets you work in terms of tuples, numbers, or
  arbitrary Elixir terms above that: a keyspace packs through its
  `:key_encoding` before writing a key and through its `:value_encoding` before
  writing a value, and unpacks on the way back.

      alias Bedrock.{Encoding, Keyspace}

      balances =
        "app"
        |> Keyspace.new(key_encoding: Encoding.Tuple)
        |> Keyspace.partition("balances", value_encoding: Encoding.BERT)

  ## Choosing one

  | Encoding | Accepts | Order-preserving | Use for |
  |---|---|---|---|
  | `Bedrock.Encoding.Tuple` | tuples, lists, binaries, integers, floats, `nil` | **yes** | keys |
  | `Bedrock.Encoding.None` | binaries only | yes (it is the identity) | keys or values |
  | `Bedrock.Encoding.BERT` | any Elixir term | **no** | values |

  Order preservation is the property that matters for keys. A key encoding is
  order-preserving when the byte ordering of packed keys matches the logical
  ordering of the values they came from — which is exactly what makes a range
  read over a keyspace return what you expect. `BERT` does not have this
  property, so it belongs on `:value_encoding`.

  A keyspace with no encoding set passes binaries through unchanged, the same
  as `Bedrock.Encoding.None`.

  ## Implementing your own

  Implement both callbacks such that `unpack(pack(value)) == value`. If the
  encoding is to be used for keys, it must also be order-preserving.
  """

  @doc "Encodes a value as a binary."
  @callback pack(value :: any()) :: binary()

  @doc "Decodes a binary produced by `c:pack/1`."
  @callback unpack(packed :: binary()) :: any()
end
