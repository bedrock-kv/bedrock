defmodule Bedrock.ControlPlane.Distributor.RecruitmentTest do
  @moduledoc """
  The recruitment pipeline: node pick → Foreman worker creation (params
  carry the shard assignment) → epoch lock → unlock at the worker's own
  reported durable version with typed pull sources from the single
  ShardRouter placement site. A worker that never reached service is
  removed before the error returns — failed recruitment leaks nothing.
  """
  use ExUnit.Case, async: true

  alias Bedrock.ControlPlane.Distributor.Recruitment
  alias Bedrock.DataPlane.ShardRouter
  alias Bedrock.DataPlane.Version

  defmodule TestCluster do
    @moduledoc false
    def otp_name(:foreman), do: :recruitment_test_foreman
    def otp_name_for_worker(id), do: :"recruitment_test_worker_#{id}"
  end

  defp ctx(overrides) do
    Map.merge(
      %{
        cluster: TestCluster,
        epoch: 4,
        node_capabilities: %{materializer: [:node_a@host]},
        logs: %{"log_1" => [], "log_2" => []},
        log_refs: %{"log_1" => :ref_1, "log_2" => :ref_2},
        create_worker_fn: fn _foreman, _id, :materializer, _opts -> {:ok, :worker_ref} end,
        lock_materializer_fn: fn _worker, _epoch -> {:ok, self(), %{durable_version: Version.zero()}} end,
        unlock_materializer_fn: fn _pid, _version, _sources -> :ok end,
        remove_worker_fn: fn _foreman, _id, _opts -> :ok end
      },
      overrides
    )
  end

  test "the pipeline: create with shard params, lock at epoch, unlock at the worker's own version with its replica set" do
    test_pid = self()

    ctx =
      ctx(%{
        create_worker_fn: fn foreman, id, :materializer, opts ->
          send(test_pid, {:created, foreman, id, opts[:params]})
          {:ok, :worker_ref}
        end,
        lock_materializer_fn: fn worker, epoch ->
          send(test_pid, {:locked, worker, epoch})
          {:ok, test_pid, %{durable_version: Version.from_integer(0)}}
        end,
        unlock_materializer_fn: fn pid, version, sources ->
          send(test_pid, {:unlocked, pid, version, sources})
          :ok
        end
      })

    assert {:ok, _pid, :node_a@host, worker_id} = Recruitment.recruit(7, :node_a@host, ctx)

    assert_received {:created, {:recruitment_test_foreman, :node_a@host}, ^worker_id, params}
    assert params["shard_id"] == 7

    assert_received {:locked, {:worker_ref, :node_a@host}, 4}

    # The seed is the shard's replica set from the single placement site.
    expected =
      7
      |> ShardRouter.log_ids_for_tag(ShardRouter.log_map(["log_1", "log_2"]), 2)
      |> Enum.map(fn
        "log_1" -> {"log_1", :ref_1}
        "log_2" -> {"log_2", :ref_2}
      end)

    assert_received {:unlocked, _pid, version, ^expected}
    assert version == Version.from_integer(0)
  end

  test "the epoch's policy params ride the creation, alongside the shard assignment" do
    test_pid = self()

    ctx =
      ctx(%{
        worker_params: %{"idle_timeout" => 900_000},
        create_worker_fn: fn _foreman, _id, :materializer, opts ->
          send(test_pid, {:created, opts[:params]})
          {:ok, :worker_ref}
        end
      })

    assert {:ok, _pid, :node_a@host, _worker_id} = Recruitment.recruit(7, :node_a@host, ctx)
    assert_received {:created, params}
    assert params == %{"idle_timeout" => 900_000, "shard_id" => 7}
  end

  # bedrock-q67.21.8: tag 0 reaches this site for real — it is in the
  # shard layout and in the committed family, so the distributor
  # monitors, verifies and heals it like any other tag. A system
  # materializer carrying the cluster's idle timeout would eventually
  # delete its own foreman entry and working directory, and the next
  # recovery stalls on a named system member it cannot reach.
  test "the system shard is created with the shard assignment alone, whatever policy the epoch carries" do
    test_pid = self()

    ctx =
      ctx(%{
        worker_params: %{"idle_timeout" => 900_000},
        create_worker_fn: fn _foreman, _id, :materializer, opts ->
          send(test_pid, {:created, opts[:params]})
          {:ok, :worker_ref}
        end
      })

    assert {:ok, _pid, :node_a@host, _worker_id} = Recruitment.recruit(0, :node_a@host, ctx)
    assert_received {:created, params}
    assert params == %{"shard_id" => 0}
  end

  test "a lock failure removes the orphan before the error returns" do
    test_pid = self()

    ctx =
      ctx(%{
        lock_materializer_fn: fn _worker, _epoch -> {:error, :nope} end,
        remove_worker_fn: fn foreman, id, _opts ->
          send(test_pid, {:removed, foreman, id})
          :ok
        end
      })

    assert {:error, {:materializer_lock_failed, :nope, :node_a@host}} = Recruitment.recruit(7, :node_a@host, ctx)
    assert_received {:removed, {:recruitment_test_foreman, :node_a@host}, _worker_id}
  end

  test "an unlock failure removes the orphan too" do
    test_pid = self()

    ctx =
      ctx(%{
        unlock_materializer_fn: fn _pid, _v, _sources -> {:failure, :timeout, :ref} end,
        remove_worker_fn: fn _foreman, id, _opts ->
          send(test_pid, {:removed, id})
          :ok
        end
      })

    assert {:error, {:unlock_failed, :timeout, :node_a@host}} = Recruitment.recruit(7, :node_a@host, ctx)
    assert_received {:removed, _worker_id}
  end

  test "orphan removal never masks the original error, even when the foreman is unreachable" do
    ctx =
      ctx(%{
        lock_materializer_fn: fn _worker, _epoch -> {:error, :original} end,
        remove_worker_fn: fn _f, _i, _o -> exit(:foreman_gone) end
      })

    log =
      ExUnit.CaptureLog.capture_log(fn ->
        assert {:error, {:materializer_lock_failed, :original, _node}} = Recruitment.recruit(7, :node_a@host, ctx)
      end)

    assert log =~ "Failed to remove orphaned materializer worker"
  end

  test "an empty replica set fails loudly before any worker exists — never a silent black hole" do
    ctx =
      ctx(%{
        log_refs: %{},
        create_worker_fn: fn _f, _i, _k, _o -> flunk("must not create a worker with no pull sources") end
      })

    assert {:error, {:no_pull_sources, 7}} = Recruitment.recruit(7, :node_a@host, ctx)
  end

  # bedrock-22g: the minimal spread — least loaded wins, ties by the
  # directory's order. Placement by observed load and locality is
  # bedrock-q67.46's; this only stops every shard piling onto the first
  # capable node.
  describe "placement" do
    @capable %{materializer: [:node_a@host, :node_b@host, :node_c@host]}

    test "no capable node has nowhere to place" do
      assert {:error, :no_materializer_capable_nodes} = Recruitment.place(%{}, [])
      assert {:error, :no_materializer_capable_nodes} = Recruitment.place(%{materializer: []}, ["node_a@host"])
    end

    test "successive placements against an accumulating view go round the capable nodes" do
      assert {:ok, :node_a@host} = Recruitment.place(@capable, [])
      assert {:ok, :node_b@host} = Recruitment.place(@capable, ["node_a@host"])
      assert {:ok, :node_c@host} = Recruitment.place(@capable, ["node_a@host", "node_b@host"])

      # A full turn wraps: every node carries one, so the directory's
      # order decides again.
      assert {:ok, :node_a@host} =
               Recruitment.place(@capable, ["node_a@host", "node_b@host", "node_c@host"])
    end

    test "the least loaded wins outright, whatever the directory's order" do
      assert {:ok, :node_c@host} =
               Recruitment.place(@capable, ["node_a@host", "node_a@host", "node_b@host", "node_b@host"])
    end

    test "a node the directory does not name carries no weight" do
      assert {:ok, :node_a@host} = Recruitment.place(@capable, ["node_z@host", "node_z@host"])
    end

    # The heal case: the dead member has already left the view, so its
    # node is placed against as the empty node it now is — no counter
    # survived the death to send the replacement elsewhere, and none
    # sent a second shard to the survivor.
    test "a replacement is placed against what is left after the retirement" do
      assert {:ok, :node_b@host} = Recruitment.place(@capable, ["node_a@host", "node_c@host"])
    end
  end

  describe "adoption: recruitment minus creation, minus destruction" do
    test "an adopt locks at the epoch and unlocks at the worker's OWN durable version with its replica set" do
      test_pid = self()
      own_durable = Version.from_integer(9)

      ctx =
        ctx(%{
          lock_materializer_fn: fn worker, epoch ->
            send(test_pid, {:locked, worker, epoch})
            {:ok, test_pid, %{durable_version: own_durable}}
          end,
          unlock_materializer_fn: fn pid, version, sources ->
            send(test_pid, {:unlocked, pid, version, sources})
            :ok
          end,
          create_worker_fn: fn _f, _i, _k, _o -> flunk("adoption must never create a worker") end
        })

      assert {:ok, _pid, :node_b@host, "wkr_named"} = Recruitment.adopt(7, "wkr_named", :node_b@host, ctx)

      # Locked by its callable name on ITS node — no node selection.
      assert_received {:locked, {:recruitment_test_worker_wkr_named, :node_b@host}, 4}

      # Unlocked at the version the worker itself reports: it resumes
      # pulling from exactly where its own store left off.
      assert_received {:unlocked, _pid, ^own_durable, [{"log_1", :ref_1} | _]}
    end

    test "a failed adoption never removes the worker — it pre-exists this attempt and holds real state" do
      ctx =
        ctx(%{
          lock_materializer_fn: fn _worker, _epoch -> {:error, :wedged} end,
          remove_worker_fn: fn _f, _i, _o -> flunk("a pre-existing worker must never be removed") end,
          create_worker_fn: fn _f, _i, _k, _o -> flunk("adoption must never create a worker") end
        })

      assert {:error, {:materializer_lock_failed, :wedged, :node_b@host}} =
               Recruitment.adopt(7, "wkr_named", :node_b@host, ctx)
    end

    test "an adopt with no pull sources fails loudly before touching the worker" do
      ctx =
        ctx(%{
          log_refs: %{},
          lock_materializer_fn: fn _w, _e -> flunk("must not lock a worker it cannot feed") end
        })

      assert {:error, {:no_pull_sources, 7}} = Recruitment.adopt(7, "wkr_named", :node_b@host, ctx)
    end
  end
end
