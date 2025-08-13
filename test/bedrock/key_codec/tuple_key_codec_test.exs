defmodule Bedrock.KeyCodec.TupleKeyCodecTest do
  use ExUnit.Case
  use ExUnitProperties

  alias Bedrock.KeyCodec.TupleKeyCodec

  property "encode and decode integers" do
    check all(int <- integer()) do
      {:ok, encoded} = TupleKeyCodec.encode_key(int)
      {:ok, decoded} = TupleKeyCodec.decode_key(encoded)
      assert decoded == int
    end
  end

  property "encode and decode floats" do
    check all(float <- float()) do
      {:ok, encoded} = TupleKeyCodec.encode_key(float)
      {:ok, decoded} = TupleKeyCodec.decode_key(encoded)
      assert decoded == float
    end
  end

  property "encode and decode binaries" do
    check all(binary <- binary()) do
      {:ok, encoded} = TupleKeyCodec.encode_key(binary)
      {:ok, decoded} = TupleKeyCodec.decode_key(encoded)
      assert decoded == binary
    end
  end

  property "encode and decode tuples" do
    check all(tuple <- tuple({integer(), float(), binary()})) do
      {:ok, encoded} = TupleKeyCodec.encode_key(tuple)
      {:ok, decoded} = TupleKeyCodec.decode_key(encoded)
      assert decoded == tuple
    end
  end

  property "encode and decode nested tuples" do
    check all(
            nested_tuple <-
              tuple({integer(), tuple({float(min: -1.0e10, max: 1.0e10), binary()}), integer()})
          ) do
      {:ok, encoded} = TupleKeyCodec.encode_key(nested_tuple)
      {:ok, decoded} = TupleKeyCodec.decode_key(encoded)
      assert decoded == nested_tuple
    end
  end
end
