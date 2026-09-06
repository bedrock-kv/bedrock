defmodule Bedrock.ControlPlane.ExclusionTest do
  use ExUnit.Case, async: true

  alias Bedrock.ControlPlane.Exclusion
  alias Bedrock.KeyRange
  alias Bedrock.SystemKeys
  alias Bedrock.SystemKeys.Values

  describe "check_logs/2" do
    test "refuses a node holding a log of the CURRENT generation" do
      keyspace = keyspace(current: %{"log_a" => "node_a@host"})

      assert {:unsafe, [{:current, "log_a", "node_a@host"}]} = Exclusion.check_logs(read_fn(keyspace), ["node_a@host"])
    end

    test "refuses a node holding only a log of an OLD generation" do
      # The whole reason the record is keyed by generation. This log serves
      # no shard in the current epoch — anything derived from a tag list
      # would read the machine as idle — but recovery has not finished
      # copying from it, so taking the machine away loses the window.
      keyspace = keyspace(current: %{"log_b" => "node_b@host"}, old: %{"log_a" => "node_a@host"})

      assert {:unsafe, [{:old, "log_a", "node_a@host"}]} = Exclusion.check_logs(read_fn(keyspace), ["node_a@host"])
    end

    test "names every log standing in the way, across both generations" do
      keyspace =
        keyspace(
          current: %{"log_b" => "node_a@host", "log_c" => "node_c@host"},
          old: %{"log_a" => "node_a@host"}
        )

      assert {:unsafe, [{:current, "log_b", "node_a@host"}, {:old, "log_a", "node_a@host"}]} =
               Exclusion.check_logs(read_fn(keyspace), ["node_a@host"])
    end

    test "a node the record never names has no log blockers" do
      keyspace = keyspace(current: %{"log_a" => "node_a@host"}, old: %{"log_b" => "node_b@host"})

      assert :no_log_blockers = Exclusion.check_logs(read_fn(keyspace), ["node_c@host"])
    end

    test "a record naming no logs at all has no log blockers" do
      assert :no_log_blockers = Exclusion.check_logs(read_fn(%{}), ["node_a@host"])
    end

    test "a failed read is an error, never a verdict" do
      # "The record could not be read" and "the record names nobody here"
      # must not look alike to an operator about to power a machine down.
      assert {:error, {:log_locations_query_failed, :timeout}} =
               Exclusion.check_logs(fn _start -> {:error, :timeout} end, ["node_a@host"])
    end

    test "an entry that will not decode fails the check rather than reading as empty" do
      key = SystemKeys.log_key(:current, "log_a")
      keyspace = %{key => "not tuple-encoded at all"}

      assert {:error, {:invalid_log_entry, ^key}} = Exclusion.check_logs(read_fn(keyspace), ["node_a@host"])
    end

    test "an atom node raises rather than matching nothing and reading as clear" do
      # `node()` is the natural Elixir shape and the one the recovery
      # phases carry until they stringify. Matched against a family of
      # strings it would match nothing, and the miss would surface as the
      # DANGEROUS answer.
      keyspace = keyspace(current: %{"log_a" => "node_a@host"})

      assert_raise ArgumentError, fn -> Exclusion.check_logs(read_fn(keyspace), [:node_a@host]) end
    end

    test "a foreign key inside the family's range fails the check" do
      key = SystemKeys.logs_prefix() <> "not-a-generation"
      keyspace = %{key => Values.encode_log_node("node_a@host")}

      assert {:error, {:invalid_log_entry, ^key}} = Exclusion.check_logs(read_fn(keyspace), ["node_a@host"])
    end
  end

  defp keyspace(generations) do
    for {generation, locations} <- generations,
        {log_id, node} <- locations,
        into: %{},
        do: {SystemKeys.log_key(generation, log_id), Values.encode_log_node(node)}
  end

  # Serves the family as one page, the way a materializer range read does.
  defp read_fn(keyspace) do
    {_range_start, range_end} = KeyRange.from_prefix(SystemKeys.logs_prefix())

    fn start_key ->
      entries =
        keyspace
        |> Enum.filter(fn {key, _value} -> key >= start_key and key < range_end end)
        |> Enum.sort()

      {:ok, {entries, false}}
    end
  end
end
