defmodule Bedrock.DataPlane.CommitProxy.FinalizationPrivateMutationTest do
  @moduledoc """
  Retirement travels in-band, on the stream the victim already follows.

  FDB's proxy privatizes a `serverKeys` mutation by prefixing it with
  `systemKeys.begin` — moving the key outside `allKeys` so no shard can
  store it — addresses it to the affected server's tag, and writes it
  into the SAME commit's mutation stream
  (`ApplyMetadataMutation.cpp:291-317`). The storage server recognizes
  the prefix and diverts it to `applyPrivateData` before any normal
  application (`storageserver.actor.cpp:11444-11450`), and asks FDB's
  own "is this about me?" question — `startsWith(data->sk)` (`:11523`).

  Ours is the same shape, with the collapse that set-valued membership
  bought: the worker id is IN the key, so the victim is NAMED rather
  than inferred from a value naming someone else.
  """
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.CommitProxy.Batch
  alias Bedrock.DataPlane.CommitProxy.Finalization
  alias Bedrock.DataPlane.CommitProxy.ResolverLayout
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.DataPlane.Version
  alias Bedrock.SystemKeys
  alias Bedrock.SystemKeys.Values
  alias Bedrock.Test.DataPlane.FinalizationTestSupport, as: Support

  describe "privatized_mutations/1" do
    test "a membership CLEAR is addressed to the shard the key names" do
      key = SystemKeys.materializer_key(7, "wkr_victim")

      assert [{{:clear, private}, 7}] = Finalization.privatized_mutations({:clear, key})

      # Prefixed past end_of_keyspace, exactly as FDB moves the key
      # outside allKeys: no shard can store it, and the routing tag is
      # explicit rather than derived from a boundary walk that would
      # raise for a key past every shard.
      assert private == Bedrock.end_of_keyspace() <> key
    end

    test "a membership SET produces nothing — only removal retires" do
      # Gaining a member is not news the victim needs, and adoption is
      # already director-driven. Emitting it would be a mutation with no
      # reader (guard-only-real-hazards).
      key = SystemKeys.materializer_key(7, "wkr_new")

      assert Finalization.privatized_mutations({:set, key, Values.encode_materializer_node("node@host")}) == []
    end

    test "ordinary mutations produce nothing" do
      assert Finalization.privatized_mutations({:clear, "user_key"}) == []
      assert Finalization.privatized_mutations({:clear, SystemKeys.shard_key("m")}) == []
      assert Finalization.privatized_mutations({:set, "user_key", "v"}) == []
      assert Finalization.privatized_mutations({:clear_range, "a", "z"}) == []
      assert Finalization.privatized_mutations({:atomic, :add, "k", <<1>>}) == []
    end
  end

  describe "the notice reaches the log stream" do
    test "a committed membership clear pushes BOTH the mutation and its privatized copy, tagged from the key" do
      # The layout here is one shard, tag 0, covering everything — so the
      # ordinary mutation routes to tag 0 by boundary walk. The notice is
      # addressed to tag 5, which appears in no boundary at all: proof the
      # tag came from the KEY rather than from a shard lookup, and proof
      # that a key past every boundary does not fail the batch.
      key = SystemKeys.materializer_key(5, "wkr_victim")
      test_pid = self()

      layout = %{logs: %{"log_1" => [0]}, services: %{"log_1" => %{kind: :log, status: {:up, self()}}}}
      routing_data = Support.build_routing_data(layout)

      batch = %Batch{
        started_at: 0,
        finalized_at: 0,
        last_commit_version: Version.from_integer(99),
        commit_version: Version.from_integer(100),
        n_transactions: 1,
        buffer: [
          {0, fn _result -> :ok end,
           Transaction.encode(%{
             mutations: [{:clear, key}],
             read_conflicts: nil,
             write_conflicts: [{key, key <> <<0>>}]
           }), :system}
        ]
      }

      assert {:ok, 0, 1} =
               Finalization.finalize_batch(batch,
                 epoch: 1,
                 sequencer: :test_sequencer,
                 resolver_layout: %ResolverLayout.Single{resolver_ref: :test_resolver},
                 metadata_apply_fn: Support.metadata_apply_fn(routing_data),
                 resolver_fn: fn _r, _e, last, commit, _txns, _md, _o ->
                   {:ok, [], Support.tiling_window(last, commit)}
                 end,
                 batch_log_push_fn: fn _last, by_log, _commit, _opts ->
                   send(test_pid, {:pushed, by_log})
                   :ok
                 end,
                 sequencer_notify_fn: fn _s, _e, _c, _o -> :ok end
               )

      assert_receive {:pushed, by_log}
      pushed = Map.fetch!(by_log, "log_1")
      private = Bedrock.end_of_keyspace() <> key

      assert {:ok, mutations} = Transaction.mutations(pushed)
      mutations = Enum.to_list(mutations)

      assert {:clear, key} in mutations
      assert {:clear, private} in mutations

      # The SHARD_INDEX is what the demux slices on, so the tag must be
      # recorded there — tag 5 for the notice, alongside tag 0 for the
      # ordinary mutation the boundary walk placed.
      tags = pushed |> Transaction.shard_index!() |> Enum.map(&elem(&1, 0)) |> Enum.sort()
      assert tags == [0, 5]
    end
  end
end
