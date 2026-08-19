defmodule Bedrock.SystemKeys.ValuesTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Bedrock.SystemKeys
  alias Bedrock.SystemKeys.Values

  describe "integer round-trip" do
    property "encodes and decodes any integer" do
      check all(i <- integer()) do
        assert {:ok, ^i} = i |> Values.encode_integer() |> Values.decode_integer()
      end
    end

    test "large timestamps round-trip" do
      ts = System.system_time(:millisecond)
      assert {:ok, ^ts} = ts |> Values.encode_integer() |> Values.decode_integer()
    end

    test "decoding a non-integer value returns an error" do
      assert {:error, _} = Values.decode_integer(Values.encode_id("not-an-int"))
    end
  end

  describe "boolean round-trip" do
    test "true and false round-trip" do
      assert {:ok, true} = true |> Values.encode_boolean() |> Values.decode_boolean()
      assert {:ok, false} = false |> Values.encode_boolean() |> Values.decode_boolean()
    end

    test "decoding a non-boolean value returns an error" do
      assert {:error, _} = Values.decode_boolean(Values.encode_integer(7))
    end
  end

  describe "atom round-trip" do
    property "atoms round-trip as strings" do
      check all(a <- member_of([:completed, :running, :stalled, :a@host])) do
        assert {:ok, ^a} = a |> Values.encode_atom() |> Values.decode_atom()
      end
    end

    test "decoding a string that names no existing atom errors and creates no atom" do
      string = "bedrock_values_test_never_an_atom_#{System.unique_integer([:positive])}"

      assert {:error, :unknown_atom} = Values.decode_atom(Values.encode_id(string))
      assert_raise ArgumentError, fn -> String.to_existing_atom(string) end
    end
  end

  describe "id round-trip" do
    property "binary ids round-trip" do
      check all(id <- binary()) do
        assert {:ok, ^id} = id |> Values.encode_id() |> Values.decode_id()
      end
    end

    property "integer ids round-trip" do
      check all(id <- non_negative_integer()) do
        assert {:ok, ^id} = id |> Values.encode_id() |> Values.decode_id()
      end
    end
  end

  describe "node list round-trip" do
    property "lists of node atoms round-trip" do
      check all(nodes <- list_of(member_of([:a@host, :b@host, :c@other]))) do
        assert {:ok, ^nodes} = nodes |> Values.encode_node_list() |> Values.decode_node_list()
      end
    end

    test "decoding a non-list returns an error" do
      assert {:error, _} = Values.decode_node_list(Values.encode_integer(1))
    end

    test "a node name that is not an existing atom errors and creates no atom" do
      string = "unknown_node_#{System.unique_integer([:positive])}@nowhere"
      encoded = Bedrock.Encoding.Tuple.pack([string])

      assert {:error, :unknown_atom} = Values.decode_node_list(encoded)
      assert_raise ArgumentError, fn -> String.to_existing_atom(string) end
    end
  end

  describe "tag list round-trip (log descriptors)" do
    property "lists of integer tags round-trip" do
      check all(tags <- list_of(non_negative_integer())) do
        assert {:ok, ^tags} = tags |> Values.encode_tag_list() |> Values.decode_tag_list()
      end
    end

    test "decoding a list containing non-integers returns an error" do
      assert {:error, _} = Values.decode_tag_list(Values.encode_node_list([:a@host]))
    end
  end

  describe "shard key entry round-trip" do
    property "{tag, start_key} round-trips" do
      check all(
              tag <- non_negative_integer(),
              start_key <- binary()
            ) do
        assert {:ok, {^tag, ^start_key}} =
                 tag
                 |> Values.encode_shard_key_entry(start_key)
                 |> Values.decode_shard_key_entry()
      end
    end

    test "decoding a bare integer returns an error" do
      assert {:error, _} = Values.decode_shard_key_entry(Values.encode_integer(7))
    end
  end

  describe "structured round-trip (versioned)" do
    property "nested config-shaped terms round-trip" do
      check all(term <- structured_term()) do
        assert {:ok, ^term} = term |> Values.encode_structured() |> Values.decode_structured()
      end
    end

    test "config_monolithic shape round-trips" do
      config = {4, %{coordinators: [:a@host], parameters: %{desired_logs: 2}, policies: %{}}}
      assert {:ok, ^config} = config |> Values.encode_structured() |> Values.decode_structured()
    end

    test "service map shape round-trips" do
      services = %{
        "log-1" => %{kind: :log, status: :unknown, last_seen: {:worker_1, :a@host}},
        "mat-1" => %{kind: :materializer, status: :down, last_seen: nil}
      }

      assert {:ok, ^services} = services |> Values.encode_structured() |> Values.decode_structured()
    end

    test "encoding a pid raises (writers must sanitize)" do
      assert_raise ArgumentError, fn -> Values.encode_structured(%{status: {:up, self()}}) end
    end

    test "unsupported version byte returns an error" do
      <<_version, rest::binary>> = Values.encode_structured(%{})
      assert {:error, _} = Values.decode_structured(<<255, rest::binary>>)
    end

    test "truncated payload returns an error" do
      encoded = Values.encode_structured({1, %{a: [1, 2, 3]}})
      truncated = binary_part(encoded, 0, byte_size(encoded) - 2)
      assert {:error, _} = Values.decode_structured(truncated)
    end

    test "an atom payload naming no existing atom errors and creates no atom" do
      string = "bedrock_structured_never_an_atom_#{System.unique_integer([:positive])}"
      # v1 wire format: version byte 0x01, atom tag 0x03, 16-bit length, bytes
      payload = <<0x01, 0x03, byte_size(string)::16, string::binary>>

      assert {:error, :invalid_encoding} = Values.decode_structured(payload)
      assert_raise ArgumentError, fn -> String.to_existing_atom(string) end
    end
  end

  describe "decode_for/2 family dispatch" do
    test "dispatches every family PersistencePhase writes" do
      assert {:ok, {7, "a"}} = Values.decode_for({:shard_key, "m"}, Values.encode_shard_key_entry(7, "a"))
      assert {:ok, "opaque"} = Values.decode_for({:shard, "3"}, "opaque")
      assert {:ok, "opaque"} = Values.decode_for({:materializer_key, "m"}, "opaque")
      assert {:ok, [0, 1]} = Values.decode_for({:layout_log, "log-1"}, Values.encode_tag_list([0, 1]))
      assert {:ok, %{}} = Values.decode_for(:layout_services, Values.encode_structured(%{}))
      assert {:ok, "layout-abc"} = Values.decode_for(:layout_id, Values.encode_id("layout-abc"))
      assert {:ok, [:a@host]} = Values.decode_for({:cluster, :coordinators}, Values.encode_node_list([:a@host]))
      assert {:ok, 4} = Values.decode_for({:cluster, :epoch}, Values.encode_integer(4))
      assert {:ok, true} = Values.decode_for({:cluster_policy, :volunteer_nodes}, Values.encode_boolean(true))
      assert {:ok, 2} = Values.decode_for({:cluster_parameter, :desired_logs}, Values.encode_integer(2))
      assert {:ok, 1} = Values.decode_for({:recovery, :attempt}, Values.encode_integer(1))
      assert {:ok, :completed} = Values.decode_for({:recovery, :state}, Values.encode_atom(:completed))
      assert {:ok, 123} = Values.decode_for({:recovery, :last_completed}, Values.encode_integer(123))
      assert {:ok, {4, %{}}} = Values.decode_for(:config_monolithic, Values.encode_structured({4, %{}}))
      assert {:ok, 4} = Values.decode_for(:epoch_legacy, Values.encode_integer(4))
      assert {:ok, 123} = Values.decode_for(:last_recovery_legacy, Values.encode_integer(123))
    end

    test "a parsed key family without a decoder returns an error instead of raising" do
      assert {:error, :unknown_family} = Values.decode_for({:future_family, "x"}, "anything")
    end

    property "garbage bytes never raise, for any parsed key family" do
      families = [
        {:shard_key, "m"},
        {:layout_log, "log-1"},
        :layout_services,
        :layout_id,
        {:cluster, :coordinators},
        {:cluster, :epoch},
        {:cluster_policy, :volunteer_nodes},
        {:cluster_parameter, :desired_logs},
        {:recovery, :attempt},
        {:recovery, :state},
        {:recovery, :last_completed},
        :config_monolithic,
        :epoch_legacy,
        :last_recovery_legacy
      ]

      check all(
              garbage <- binary(min_length: 1),
              parsed <- member_of(families)
            ) do
        # Must return a tagged result -- never raise or exit
        result = Values.decode_for(parsed, garbage)
        assert match?({:ok, _}, result) or match?({:error, _}, result)
      end
    end
  end

  describe "no term_to_binary in encoded output" do
    test "encoded values are not decodable as external term format" do
      # BERT payloads start with 131; none of our encodings should
      refute match?(<<131, _::binary>>, Values.encode_integer(7))
      refute match?(<<131, _::binary>>, Values.encode_structured(%{a: 1}))
      refute match?(<<131, _::binary>>, Values.encode_shard_key_entry(1, ""))
    end
  end

  # A generator approximating the shapes stored in config_monolithic /
  # layout_services: maps and tuples of atoms, binaries, integers, lists.
  defp structured_term do
    leaf =
      one_of([
        constant(nil),
        boolean(),
        integer(),
        binary(),
        member_of([:log, :materializer, :down, :unknown, :a@host])
      ])

    tree(leaf, fn child ->
      one_of([
        list_of(child, max_length: 3),
        map_of(one_of([binary(), member_of([:kind, :status, :last_seen])]), child, max_length: 3),
        bind(list_of(child, max_length: 3), fn elems -> constant(List.to_tuple(elems)) end)
      ])
    end)
  end

  # Sanity: SystemKeys.parse_key output feeds decode_for
  test "parse_key output is accepted by decode_for" do
    key = SystemKeys.shard_key("m")
    parsed = SystemKeys.parse_key(key)
    assert {:ok, {7, ""}} = Values.decode_for(parsed, Values.encode_shard_key_entry(7, ""))
  end
end
