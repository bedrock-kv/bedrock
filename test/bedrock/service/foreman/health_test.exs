defmodule Bedrock.Service.Foreman.HealthTest do
  use ExUnit.Case, async: true

  import Bedrock.Service.Foreman.Health, only: [compute_health_from_worker_info: 1]

  # The verdict summarises a SET of workers, so it must not depend on the
  # order they happen to arrive in. `recompute_health/1` folds
  # `Map.values(t.workers)`, and for small maps Erlang returns keys in
  # term order — so an order-sensitive fold makes the foreman's health a
  # function of how worker ids happen to sort.

  defp ok, do: %{health: {:ok, self()}}
  defp failed, do: %{health: {:failed_to_start, :manifest_does_not_exist}}
  defp stopped, do: %{health: :stopped}
  defp unrecognised, do: %{health: {:error, :timeout}}

  describe "compute_health_from_worker_info/1" do
    test "no workers is healthy" do
      assert compute_health_from_worker_info([]) == :ok
    end

    test "all running is healthy" do
      assert compute_health_from_worker_info([ok(), ok(), ok()]) == :ok
    end

    test "a stopped worker is still starting" do
      assert compute_health_from_worker_info([ok(), stopped()]) == :starting
    end

    test "a single failure is reported however many healthy workers surround it" do
      assert compute_health_from_worker_info([failed()]) ==
               {:failed_to_start, :at_least_one_failed_to_start}

      assert compute_health_from_worker_info([ok(), failed()]) ==
               {:failed_to_start, :at_least_one_failed_to_start}

      assert compute_health_from_worker_info([failed(), ok()]) ==
               {:failed_to_start, :at_least_one_failed_to_start}
    end

    # The specific regression: `{:ok, _}, _ -> :starting` overwrote an
    # already-recorded failure, so [bad, ok] reported :starting — a
    # wedged worker indistinguishable from a slow boot.
    test "a healthy worker never clears a recorded failure" do
      assert compute_health_from_worker_info([failed(), failed(), ok()]) ==
               {:failed_to_start, :at_least_one_failed_to_start}

      assert compute_health_from_worker_info([failed(), ok(), ok(), ok()]) ==
               {:failed_to_start, :at_least_one_failed_to_start}
    end

    test "failure outranks merely starting" do
      assert compute_health_from_worker_info([stopped(), failed()]) ==
               {:failed_to_start, :at_least_one_failed_to_start}

      assert compute_health_from_worker_info([failed(), stopped()]) ==
               {:failed_to_start, :at_least_one_failed_to_start}
    end

    # Reachable in fact, not just in theory: Foreman.report_health/3
    # accepts Worker.health(), which includes {:error, :timeout}, and
    # that value lands straight in this field.
    test "an unrecognized health is not silently treated as progress" do
      assert compute_health_from_worker_info([ok(), unrecognised()]) == :unknown

      # The discriminating order: a healthy worker AFTER the unknown one
      # used to overwrite the verdict back to :starting.
      assert compute_health_from_worker_info([unrecognised(), ok()]) == :unknown
    end

    # The property the fold has to satisfy, stated directly.
    test "the verdict is invariant under permutation" do
      for workers <- [
            [ok(), failed(), stopped()],
            [ok(), ok(), failed()],
            [stopped(), ok()],
            [failed(), failed()],
            [ok(), ok(), ok()],
            [stopped(), stopped(), ok(), failed()],
            [ok(), unrecognised()],
            [unrecognised(), stopped(), ok()],
            [failed(), unrecognised(), ok()]
          ] do
        expected = compute_health_from_worker_info(workers)

        for permutation <- permutations(workers) do
          assert compute_health_from_worker_info(permutation) == expected,
                 "order changed the verdict for #{inspect(Enum.map(workers, & &1.health))}"
        end
      end
    end
  end

  defp permutations([]), do: [[]]

  defp permutations(list) do
    for element <- list, rest <- permutations(list -- [element]), do: [element | rest]
  end
end
