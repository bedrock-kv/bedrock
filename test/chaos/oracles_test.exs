defmodule Bedrock.Chaos.OraclesTest do
  @moduledoc """
  Proves the oracles can fail.

  A green oracle is only evidence if a red one is reachable, and the baseline
  run (`workload_baseline_test.exs`) can only ever show them green. These check
  the other direction against hand-built violations, so a refactor that quietly
  turns an oracle into `:ok`-always gets caught here.

  No cluster involved: the conservation and reconciliation checks are pure
  functions of a snapshot, which is exactly why they can be called mid-run. So
  these are not tagged `:chaos` and run in the ordinary suite.
  """
  use ExUnit.Case, async: true

  alias Bedrock.Test.Chaos.Journal
  alias Bedrock.Test.Chaos.Oracles

  @config %{accounts: 3, starting_balance: 100}

  describe "conservation" do
    test "passes when the balances still total what the run started with" do
      assert :ok = Oracles.check_conservation(%{balances: %{0 => 90, 1 => 100, 2 => 110}}, 300)
    end

    test "fails when money went missing" do
      assert {:error, %{expected_total: 300, actual_total: 290}} =
               Oracles.check_conservation(%{balances: %{0 => 90, 1 => 100, 2 => 100}}, 300)
    end
  end

  describe "reconciliation" do
    test "passes when the balances are the replay of the visible receipts" do
      snapshot = %{balances: %{0 => 90, 1 => 110, 2 => 100}, receipts: %{{0, 1} => {0, 1, 10}}}

      assert :ok = Oracles.check_reconciliation(snapshot, @config)
    end

    test "fails on a torn transaction: the receipt is visible, its balances are not" do
      snapshot = %{balances: %{0 => 100, 1 => 100, 2 => 100}, receipts: %{{0, 1} => {0, 1, 10}}}

      assert {:error, %{differences: differences}} = Oracles.check_reconciliation(snapshot, @config)
      assert differences[0] == %{expected: 90, actual: 100}
      assert differences[1] == %{expected: 110, actual: 100}
    end

    test "fails when the total is conserved but the wrong accounts moved" do
      snapshot = %{balances: %{0 => 100, 1 => 90, 2 => 110}, receipts: %{{0, 1} => {0, 1, 10}}}

      assert :ok = Oracles.check_conservation(snapshot, 300)
      assert {:error, _} = Oracles.check_reconciliation(snapshot, @config)
    end
  end

  describe "journal" do
    setup do
      dir = Path.join(System.tmp_dir!(), "bedrock-journal-test-#{:erlang.unique_integer([:positive])}")
      on_exit(fn -> File.rm_rf!(dir) end)
      {:ok, dir: dir}
    end

    test "entries survive the writer going away without being closed", %{dir: dir} do
      journal = Journal.open!(dir, 0)
      Journal.append!(journal, %{transfer: {0, 1}})
      Journal.append!(journal, %{transfer: {0, 2}})

      assert %{entries: entries, torn: 0} = Journal.read_all(dir)
      assert entries == [%{transfer: {0, 1}}, %{transfer: {0, 2}}]
    end

    test "a torn trailing entry is dropped and counted, not raised on", %{dir: dir} do
      journal = Journal.open!(dir, 0)
      Journal.append!(journal, %{transfer: {0, 1}})
      Journal.close(journal)

      File.write!(Path.join(dir, "0.journal"), "!!not base64", [:append])

      assert %{entries: [%{transfer: {0, 1}}], torn: 1} = Journal.read_all(dir)
    end

    test "collects every writer's file", %{dir: dir} do
      for writer <- 0..2 do
        journal = Journal.open!(dir, writer)
        Journal.append!(journal, %{transfer: {writer, 1}})
        Journal.close(journal)
      end

      assert %{entries: entries, torn: 0} = Journal.read_all(dir)
      assert length(entries) == 3
    end
  end
end
