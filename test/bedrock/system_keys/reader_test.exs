defmodule Bedrock.SystemKeys.ReaderTest do
  use ExUnit.Case, async: true

  alias Bedrock.SystemKeys
  alias Bedrock.SystemKeys.Reader
  alias Bedrock.SystemKeys.Values

  describe "decode_config_parameters/1" do
    test "decodes entries into a map keyed by the parameter name in the key" do
      # Names stay binaries: decoding durable bytes never creates atoms,
      # so consumers look up the names SystemKeys publishes.
      entries = [
        {SystemKeys.config_key(SystemKeys.desired_commit_proxies()), Values.encode_config_integer(3)},
        {SystemKeys.config_key("some_future_parameter"), Values.encode_config_integer(9)}
      ]

      assert {:ok, %{"desired_commit_proxies" => 3, "some_future_parameter" => 9}} =
               Reader.decode_config_parameters(entries)
    end

    test "an empty family decodes as no parameters, not as an error" do
      # This is the fresh-cluster read, and it is what tells the
      # persistence phase to seed.
      assert {:ok, %{}} = Reader.decode_config_parameters([])
    end

    test "an undecodable value fails the WHOLE read" do
      # Skipping it would report the parameter as absent, and absent means
      # "seed from the coordinator's anchor" — a corrupt value would
      # silently revert a configured cluster and re-commit the default.
      key = SystemKeys.config_key(SystemKeys.desired_commit_proxies())

      entries = [
        {key, Values.encode_materializer_node("not_a_count")},
        {SystemKeys.config_key("other"), Values.encode_config_integer(2)}
      ]

      assert {:error, {:invalid_config_entry, ^key}} = Reader.decode_config_parameters(entries)
    end

    test "a foreign key fails the read rather than being ignored" do
      key = SystemKeys.shard_key("m")
      entries = [{key, Values.encode_config_integer(3)}]

      assert {:error, {:invalid_config_entry, ^key}} = Reader.decode_config_parameters(entries)
    end
  end
end
