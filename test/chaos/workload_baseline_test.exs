defmodule Bedrock.Chaos.WorkloadBaselineTest do
  @moduledoc """
  The zero-fault baseline for the chaos harness (bedrock-hve.5).

  Nothing is killed here, and that is the point. Every later ticket injects
  faults and then asks "is this failure survivable or a defect?", and that
  question is unanswerable unless the harness itself is known to be green when
  nothing is going wrong. So this test runs the workload hard, checks the
  oracles continuously while it runs, and checks them again once it has drained.

  The oracles are run against every node, not just the one that committed: a
  value visible only to its writer would mean the commit never reached the
  shared transaction system.
  """
  use ExUnit.Case, async: false

  alias Bedrock.Test.Chaos.Journal
  alias Bedrock.Test.Chaos.Oracles
  alias Bedrock.Test.Chaos.PeerCluster
  alias Bedrock.Test.Chaos.Workload

  @moduletag :chaos
  @moduletag timeout: 300_000

  setup do
    cluster = PeerCluster.start!(count: 3)
    journal_dir = Path.join(System.tmp_dir!(), "bedrock-chaos-journal-#{:erlang.unique_integer([:positive])}")

    on_exit(fn ->
      PeerCluster.stop(cluster)
      File.rm_rf!(journal_dir)
    end)

    {:ok, cluster: cluster, journal_dir: journal_dir}
  end

  test "a sustained workload keeps every oracle green, during and after", ctx do
    [first, second, third] = PeerCluster.node_names(ctx.cluster)

    run =
      Workload.start(ctx.cluster,
        journal_dir: ctx.journal_dir,
        accounts: 24,
        workers: 6,
        ops_per_worker: 300,
        distribution: :hotspot,
        seed: 20_260_914
      )

    mid_run_checks = check_while_running(run, second, 0)

    assert mid_run_checks > 0,
           "the workload finished before a single mid-run check ran; the oracles were never exercised under load"

    stats = Workload.await!(run)

    assert stats.committed > 0
    assert stats.failed == 0, "workers hit failures with no faults injected: #{inspect(stats.failures)}"

    for node <- [first, second, third] do
      assert :ok = Oracles.check_all(node, run), "oracles failed when checked through #{node}"
    end

    %{entries: entries, torn: torn} = Journal.read_all(ctx.journal_dir)

    assert torn == 0
    assert length(entries) == stats.committed
  end

  test "commits are journalled as they are acked, not buffered to the end", ctx do
    run =
      Workload.start(ctx.cluster,
        journal_dir: ctx.journal_dir,
        workers: 4,
        ops_per_worker: 150,
        seed: 7
      )

    # If the journal only landed at the end of the run, this would time out: the
    # whole value of the journal is that a cluster which wedges mid-run still
    # leaves behind the record of what it promised.
    await_journal_entries!(run, ctx.journal_dir, 5_000)

    stats = Workload.await!(run)
    assert stats.failed == 0, inspect(stats.failures)
  end

  defp check_while_running(run, node, count) do
    if Enum.any?(run.tasks, &Process.alive?(&1.pid)) do
      assert :ok = Oracles.check_all(node, run), "oracles failed mid-run, after #{count} clean checks"
      check_while_running(run, node, count + 1)
    else
      count
    end
  end

  defp await_journal_entries!(run, journal_dir, timeout_ms) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    await_journal_entries!(run, journal_dir, deadline, 0)
  end

  defp await_journal_entries!(run, journal_dir, deadline, polls) do
    case Journal.read_all(journal_dir) do
      %{entries: [_ | _]} ->
        :ok

      %{entries: []} ->
        workers_alive? = Enum.any?(run.tasks, &Process.alive?(&1.pid))

        cond do
          not workers_alive? ->
            flunk("the run finished without journalling a single acked commit")

          System.monotonic_time(:millisecond) >= deadline ->
            flunk("no acked commit was journalled within the deadline (#{polls} polls)")

          true ->
            await_journal_entries!(run, journal_dir, deadline, polls + 1)
        end
    end
  end
end
