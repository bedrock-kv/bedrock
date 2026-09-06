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
        recovery_authority: %{generation: 4, recovery_id: "recruitment-test"},
        node_capabilities: %{materializer: [:node_a@host]},
        logs: %{"log_1" => [], "log_2" => []},
        log_refs: %{"log_1" => :ref_1, "log_2" => :ref_2},
        create_worker_fn: fn _foreman, _id, :materializer, _opts -> {:ok, :worker_ref} end,
        lock_materializer_fn: fn _worker, _epoch -> {:ok, self(), %{durable_version: Version.zero()}} end,
        unlock_materializer_fn: fn _pid, _authority, _version, _sources -> :ok end,
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
        unlock_materializer_fn: fn pid, authority, version, sources ->
          send(test_pid, {:unlocked, pid, authority, version, sources})
          :ok
        end
      })

    assert {:ok, _pid, :node_a@host, worker_id} = Recruitment.recruit(7, ctx)

    assert_received {:created, {:recruitment_test_foreman, :node_a@host}, ^worker_id, params}
    assert params["shard_id"] == 7

    assert_received {:locked, {:worker_ref, :node_a@host}, %{generation: 4, recovery_id: "recruitment-test"}}

    # The seed is the shard's replica set from the single placement site.
    expected =
      7
      |> ShardRouter.log_ids_for_tag(ShardRouter.log_map(["log_1", "log_2"]), 2)
      |> Enum.map(fn
        "log_1" -> {"log_1", :ref_1}
        "log_2" -> {"log_2", :ref_2}
      end)

    assert_received {:unlocked, _pid, %{generation: 4, recovery_id: "recruitment-test"}, version, ^expected}
    assert version == Version.from_integer(0)
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

    assert {:error, {:materializer_lock_failed, :nope, :node_a@host}} = Recruitment.recruit(7, ctx)
    assert_received {:removed, {:recruitment_test_foreman, :node_a@host}, _worker_id}
  end

  test "an unlock failure removes the orphan too" do
    test_pid = self()

    ctx =
      ctx(%{
        unlock_materializer_fn: fn _pid, _authority, _v, _sources -> {:failure, :timeout, :ref} end,
        remove_worker_fn: fn _foreman, id, _opts ->
          send(test_pid, {:removed, id})
          :ok
        end
      })

    assert {:error, {:unlock_failed, :timeout, :node_a@host}} = Recruitment.recruit(7, ctx)
    assert_received {:removed, _worker_id}
  end

  test "no capable node fails before any worker is created" do
    ctx =
      ctx(%{
        node_capabilities: %{},
        create_worker_fn: fn _f, _i, _k, _o -> flunk("must not create without a node") end
      })

    assert {:error, :no_materializer_capable_nodes} = Recruitment.recruit(7, ctx)
  end

  test "orphan removal never masks the original error, even when the foreman is unreachable" do
    ctx =
      ctx(%{
        lock_materializer_fn: fn _worker, _epoch -> {:error, :original} end,
        remove_worker_fn: fn _f, _i, _o -> exit(:foreman_gone) end
      })

    log =
      ExUnit.CaptureLog.capture_log(fn ->
        assert {:error, {:materializer_lock_failed, :original, _node}} = Recruitment.recruit(7, ctx)
      end)

    assert log =~ "Failed to remove orphaned materializer worker"
  end

  test "an empty replica set fails loudly before any worker exists — never a silent black hole" do
    ctx =
      ctx(%{
        log_refs: %{},
        create_worker_fn: fn _f, _i, _k, _o -> flunk("must not create a worker with no pull sources") end
      })

    assert {:error, {:no_pull_sources, 7}} = Recruitment.recruit(7, ctx)
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
          unlock_materializer_fn: fn pid, authority, version, sources ->
            send(test_pid, {:unlocked, pid, authority, version, sources})
            :ok
          end,
          create_worker_fn: fn _f, _i, _k, _o -> flunk("adoption must never create a worker") end
        })

      assert {:ok, _pid, :node_b@host, "wkr_named"} = Recruitment.adopt(7, "wkr_named", :node_b@host, ctx)

      # Locked by its callable name on ITS node — no node selection.
      assert_received {:locked, {:recruitment_test_worker_wkr_named, :node_b@host},
                       %{generation: 4, recovery_id: "recruitment-test"}}

      # Unlocked at the version the worker itself reports: it resumes
      # pulling from exactly where its own store left off.
      assert_received {:unlocked, _pid, %{generation: 4, recovery_id: "recruitment-test"}, ^own_durable,
                       [{"log_1", :ref_1} | _]}
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
