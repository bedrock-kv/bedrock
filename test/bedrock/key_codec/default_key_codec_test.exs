defmodule Bedrock.KeyCodec.BinaryKeyCodecTest do
  use ExUnit.Case
  use ExUnitProperties

  alias Bedrock.KeyCodec.BinaryKeyCodec

  property "encode_key/1 returns {:ok, key} for binary keys" do
    check all(key <- binary()) do
      assert BinaryKeyCodec.encode_key(key) == {:ok, key}
    end
  end

  property "encode_key/1 returns {:error, :invalid_key} for non-binary keys" do
    check all(key <- term(), not is_binary(key)) do
      assert BinaryKeyCodec.encode_key(key) == {:error, :invalid_key}
    end
  end

  property "decode_key/1 returns {:ok, encoded_key} for binary encoded_keys" do
    check all(encoded_key <- binary()) do
      assert BinaryKeyCodec.decode_key(encoded_key) == {:ok, encoded_key}
    end
  end

  property "decode_key/1 returns {:error, :invalid_key} for non-binary encoded_keys" do
    check all(encoded_key <- term(), not is_binary(encoded_key)) do
      assert BinaryKeyCodec.decode_key(encoded_key) == {:error, :invalid_key}
    end
  end
end
