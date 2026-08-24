defmodule Bedrock.DataPlane.CommitProxy.Finalization.IngressRejectionTest do
  @moduledoc """
  Pins per-transaction keyspace validation inside the finalization pipeline
  (bedrock-q67.28).

  Validation moved off the commit proxy's serialized accept loop into the
  batch pipeline: invalid transactions are rejected individually - each gets
  its specific error through its own reply - while the rest of the batch
  proceeds. This is FDB's shape (validateAndProcessTenantAccess replies
  per-transaction inside postResolution and the batch continues), improved in
  one respect: a rejected transaction is replaced with an empty transaction
  before resolution, so its conflict ranges never pollute resolver history.

  The legal write range depends on the commit's mode, which travels with each
  transaction in the batch: user commits end at the system boundary, system
  commits at the end of the keyspace; corrupt mutation payloads fail closed
  as :invalid_transaction. Atomics are bounded like any other mutation - FDB
  parity; they never enter the metadata stream (metadata_mutation?/1 admits
  only sets and clears, exactly FDB's isMetadataMutation), so no view can
  diverge from one.
  """
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.CommitProxy.Batch
  alias Bedrock.DataPlane.CommitProxy.Finalization
  alias Bedrock.DataPlane.CommitProxy.ResolverLayout
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.DataPlane.Version
  alias Bedrock.Test.DataPlane.FinalizationTestSupport, as: Support

  defp encode(mutations, conflict_key) do
    Transaction.encode(%{
      mutations: mutations,
      read_conflicts: nil,
      write_conflicts: [{conflict_key, conflict_key <> <<0>>}]
    })
  end

  # Rewrites the MUTATIONS section with a garbage-opcode payload under a
  # valid CRC: the section opens cleanly and the lazy decode raises
  # mid-stream - the failure mode validation must contain.
  defp corrupt_mutation_payload(<<header::binary-size(8), sections::binary>>),
    do: header <> corrupt_mutations_section(sections)

  defp corrupt_mutations_section(
         <<0x01, size::unsigned-big-24, _crc::unsigned-big-32, _payload::binary-size(size), rest::binary>>
       ) do
    payload = :binary.copy(<<0xEE>>, max(size, 1))
    crc = :erlang.crc32(<<0x01, byte_size(payload)::unsigned-big-24, payload::binary>>)
    <<0x01, byte_size(payload)::unsigned-big-24, crc::unsigned-big-32, payload::binary>> <> rest
  end

  defp corrupt_mutations_section(
         <<tag, size::unsigned-big-24, crc::unsigned-big-32, payload::binary-size(size), rest::binary>>
       ) do
    <<tag, size::unsigned-big-24, crc::unsigned-big-32, payload::binary>> <> corrupt_mutations_section(rest)
  end

  defp reply_to_self(name) do
    test_pid = self()
    fn result -> send(test_pid, {name, result}) end
  end

  defp batch_of(entries) do
    buffer =
      entries
      |> Enum.with_index()
      |> Enum.map(fn {{reply_fn, tx, mode}, idx} -> {idx, reply_fn, tx, mode} end)
      |> Enum.reverse()

    %Batch{
      started_at: 0,
      finalized_at: 0,
      last_commit_version: Version.from_integer(99),
      commit_version: Version.from_integer(100),
      n_transactions: length(entries),
      buffer: buffer
    }
  end

  defp finalize(batch, opts_overrides \\ []) do
    test_pid = self()

    layout = %{logs: %{"log_1" => [0]}, services: %{"log_1" => %{kind: :log, status: {:up, self()}}}}
    routing_data = Support.build_routing_data(layout)

    resolver_fn = fn _ref, _epoch, last, commit, transactions, _metadata, _opts ->
      send(test_pid, {:resolved, transactions})
      {:ok, [], Support.tiling_window(last, commit)}
    end

    opts =
      Keyword.merge(
        [
          epoch: 1,
          sequencer: :test_sequencer,
          resolver_layout: %ResolverLayout.Single{resolver_ref: :test_resolver},
          metadata_apply_fn: Support.metadata_apply_fn(routing_data),
          resolver_fn: resolver_fn,
          batch_log_push_fn: fn _last, by_log, _commit, _opts ->
            send(test_pid, {:pushed, by_log})
            :ok
          end,
          sequencer_notify_fn: fn _sequencer, _epoch, _commit, _opts -> :ok end
        ],
        opts_overrides
      )

    Finalization.finalize_batch(batch, opts)
  end

  test "rejects an out-of-range user mutation per-transaction while the batch commits the rest" do
    bad_key = <<0xFF, "/system/forbidden">>

    batch =
      batch_of([
        {reply_to_self(:ok_tx), encode([{:set, "a", "1"}], "a"), :user},
        {reply_to_self(:bad_tx), encode([{:set, bad_key, "x"}], "b"), :user}
      ])

    assert {:ok, 1, 1} = finalize(batch)

    assert_receive {:bad_tx, {:error, {:key_out_of_range, ^bad_key}}}
    assert_receive {:ok_tx, {:ok, _version, _index}}
  end

  test "the mode travels per transaction: system and user commits mix in one batch" do
    system_key = <<0xFF, "/system/ok">>

    batch =
      batch_of([
        {reply_to_self(:system_tx), encode([{:set, system_key, "v"}], "s"), :system},
        {reply_to_self(:user_tx), encode([{:set, system_key, "v"}], "u"), :user}
      ])

    assert {:ok, 1, 1} = finalize(batch)

    assert_receive {:system_tx, {:ok, _version, _index}}
    assert_receive {:user_tx, {:error, {:key_out_of_range, ^system_key}}}
  end

  test "a corrupt mutation payload fails closed without failing the batch" do
    corrupt = corrupt_mutation_payload(encode([{:set, "k", "v"}], "k"))

    batch =
      batch_of([
        {reply_to_self(:good_tx), encode([{:set, "a", "1"}], "a"), :user},
        {reply_to_self(:corrupt_tx), corrupt, :user}
      ])

    assert {:ok, 1, 1} = finalize(batch)

    assert_receive {:corrupt_tx, {:error, :invalid_transaction}}
    assert_receive {:good_tx, {:ok, _version, _index}}
  end

  test "rejected transactions contribute no conflicts to resolution" do
    bad_key = <<0xFF, "/system/forbidden">>

    batch =
      batch_of([
        {reply_to_self(:ok_tx), encode([{:set, "a", "1"}], "a"), :user},
        {reply_to_self(:bad_tx), encode([{:set, bad_key, "x"}], "poison_range"), :user}
      ])

    assert {:ok, 1, 1} = finalize(batch)

    empty_sections = Transaction.extract_sections!(Transaction.empty_transaction(), [:read_conflicts, :write_conflicts])

    assert_receive {:resolved, [_ok_sections, rejected_sections]}
    assert rejected_sections == empty_sections
  end

  test "rejected transactions are excluded from log pushes" do
    bad_key = <<0xFF, "/system/forbidden">>

    batch =
      batch_of([
        {reply_to_self(:ok_tx), encode([{:set, "a", "1"}], "a"), :user},
        {reply_to_self(:bad_tx), encode([{:set, bad_key, "x"}], "b"), :user}
      ])

    assert {:ok, 1, 1} = finalize(batch)

    assert_receive {:pushed, by_log}

    for {_log_id, transaction} <- by_log do
      mutations = transaction |> Transaction.mutations!() |> Enum.to_list()
      refute Enum.any?(mutations, fn mutation -> match?({:set, ^bad_key, _}, mutation) end)
    end
  end

  test "boundary matrix: the legal write range depends on the commit mode" do
    eok = Bedrock.end_of_keyspace()
    boundary = Bedrock.end_of_user_keyspace()

    cases = [
      # {mode, mutations, expected_error_or_ok}
      {:user, [{:set, "user/ok", "v"}], :ok},
      {:user, [{:atomic, :add, "user/counter", <<1::64-little>>}], :ok},
      {:user, [{:clear_range, "a", boundary}], :ok},
      {:user, [{:set, <<0xFE, 0xFF, 0xFF, 0xFF>>, "v"}], :ok},
      {:user, [{:set, boundary, "v"}], {:key_out_of_range, boundary}},
      {:user, [{:clear, <<0xFF, 0x00>>}], {:key_out_of_range, <<0xFF, 0x00>>}},
      {:user, [{:clear_range, "a", <<0xFF, 0x00>>}], {:key_out_of_range, <<0xFF, 0x00>>}},
      {:user, [{:clear_range, boundary, boundary <> <<1>>}], {:key_out_of_range, boundary}},
      {:user, [{:set, "fine", "v"}, {:set, eok, "v"}], {:key_out_of_range, eok}},
      {:system, [{:set, <<0xFF, "/system/ok">>, "v"}], :ok},
      {:system, [{:clear_range, "a", eok}], :ok},
      {:system, [{:set, <<0xFF, 0xFE, 0xFF, 0xFF>>, "v"}], :ok},
      {:system, [{:set, eok, "v"}], {:key_out_of_range, eok}},
      {:system, [{:clear, eok <> <<1>>}], {:key_out_of_range, eok <> <<1>>}},
      {:system, [{:clear_range, eok, eok <> <<1>>}], {:key_out_of_range, eok}},
      {:system, [{:clear_range, "a", eok <> <<1>>}], {:key_out_of_range, eok <> <<1>>}},
      {:system, [{:atomic, :add, eok <> <<1>>, <<1::64-little>>}], {:key_out_of_range, eok <> <<1>>}},
      {:system, [{:atomic, :add, <<0xFF, "/system/n">>, <<1::64-little>>}, {:set, "a", "v"}], :ok}
    ]

    for {{mode, mutations, expected}, n} <- Enum.with_index(cases) do
      name = :"case_#{n}"
      batch = batch_of([{reply_to_self(name), encode(mutations, "ck#{n}"), mode}])

      case expected do
        :ok ->
          assert {:ok, 0, 1} = finalize(batch), "case #{n} should commit"
          assert_receive {^name, {:ok, _version, _index}}

        error ->
          assert {:ok, 1, 0} = finalize(batch), "case #{n} should reject"
          assert_receive {^name, {:error, ^error}}
      end
    end
  end

  test "a batch of only rejected transactions still completes" do
    bad_key = Bedrock.end_of_keyspace()

    batch = batch_of([{reply_to_self(:bad_tx), encode([{:set, bad_key, "v"}], "a"), :system}])

    assert {:ok, 1, 0} = finalize(batch)
    assert_receive {:bad_tx, {:error, {:key_out_of_range, ^bad_key}}}
  end
end
