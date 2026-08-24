defmodule Bedrock.ControlPlane.Distributor.ServerTest do
  @moduledoc """
  The per-epoch distributor singleton: lock first, everything else
  second; a superseded lock cedes (:normal, no re-recruit); transient
  failures stop :shutdown so the director's retry recruits afresh; the
  poll-to-die loop evaluates the fence read-only; the singleton dies
  with its director.
  """
  use ExUnit.Case, async: true

  alias Bedrock.ControlPlane.Distributor.Lock
  alias Bedrock.ControlPlane.Distributor.Placeholder
  alias Bedrock.ControlPlane.Distributor.Server
  alias Bedrock.ControlPlane.Distributor.State
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.DataPlane.Version

  # The test module doubles as the cluster: the placeholder registers
  # under otp_name_for_worker(placeholder_worker_id).
  def otp_name_for_worker(id), do: :"distributor_server_test_worker_#{id}"
  def otp_name(:foreman), do: :distributor_server_test_foreman

  defp scripted_deps(overrides) do
    Map.merge(
      %{
        epoch: 3,
        proxies: [:proxy],
        next_read_version_fn: fn -> {:ok, Version.from_integer(1)} end,
        get_fn: fn _key, _version -> {:error, :not_found} end,
        commit_fn: fn _proxy, _epoch, _encoded, _opts -> {:ok, Version.from_integer(2), 0} end,
        get_range_fn: fn _start, _end, _version -> {:ok, {[], false}} end
      },
      overrides
    )
  end

  defp state(deps_overrides, state_overrides \\ []) do
    director = Keyword.get(state_overrides, :director, self())

    struct!(
      %State{
        cluster: __MODULE__,
        epoch: 3,
        director: director,
        director_monitor: Keyword.get(state_overrides, :director_monitor, make_ref()),
        deps: scripted_deps(deps_overrides),
        poll_interval_ms: 5
      },
      state_overrides
    )
  end

  describe "taking the lock at startup" do
    test "success installs the lock, arms the poll, and continues into the startup sweep" do
      assert {:noreply, %State{lock: %Lock{}} = t, {:continue, :startup_sweep}} =
               Server.handle_continue(:take_lock, state(%{}))

      assert_receive :poll_lock, 100
      assert t.lock.prev_owner == nil
    end

    test "exhausted take aborts stop :shutdown — the director recruits again; supersession is the poll's to deliver" do
      deps = %{commit_fn: fn _p, _e, _t, _o -> {:error, :aborted} end}

      assert {:stop, {:shutdown, {:lock_take_failed, {:lock_commit_failed, :aborted}}}, _t} =
               Server.handle_continue(:take_lock, state(deps))
    end

    test "a transient failure stops :shutdown so the director retries" do
      deps = %{get_fn: fn _k, _v -> {:failure, :unavailable, :ref} end}

      assert {:stop, {:shutdown, {:lock_take_failed, {:lock_read_failed, :unavailable}}}, _t} =
               Server.handle_continue(:take_lock, state(deps))
    end
  end

  describe "the poll-to-die loop" do
    test "a superseding owner cedes" do
      {lock, _} = Lock.take(nil, nil)
      usurper = Lock.new_uid()

      t = state(%{get_fn: fn _k, _v -> {:ok, usurper} end}, lock: lock)

      assert {:stop, :normal, _t} = Server.handle_info(:poll_lock, t)
    end

    test "a healthy fence re-arms" do
      {lock, _} = Lock.take(nil, nil)

      t =
        state(
          %{
            get_fn: fn key, _v ->
              if String.ends_with?(key, "owner"), do: {:ok, lock.my_owner}, else: {:error, :not_found}
            end
          },
          lock: lock
        )

      assert {:noreply, _t} = Server.handle_info(:poll_lock, t)
      assert_receive :poll_lock, 100
    end

    test "an unavailable read is not a verdict — re-arm and retry" do
      {lock, _} = Lock.take(nil, nil)
      t = state(%{get_fn: fn _k, _v -> {:failure, :timeout, :ref} end}, lock: lock)

      assert {:noreply, _t} = Server.handle_info(:poll_lock, t)
      assert_receive :poll_lock, 100
    end
  end

  describe "the singleton dies with its epoch" do
    test "director DOWN cedes" do
      ref = make_ref()
      t = state(%{}, director_monitor: ref)

      assert {:stop, :normal, _t} = Server.handle_info({:DOWN, ref, :process, self(), :shutdown}, t)
    end
  end

  describe "the startup sweep" do
    alias Bedrock.SystemKeys
    alias Bedrock.SystemKeys.Values

    defp two_shard_snapshot_deps(refs_entries, test_pid) do
      shard_entries = [
        {SystemKeys.shard_key("m"), Values.encode_shard_key_entry(1, <<>>)},
        {SystemKeys.shard_key(<<0xFF, 0xFF>>), Values.encode_shard_key_entry(0, "m")}
      ]

      %{
        get_range_fn: fn start_key, _end, _version ->
          cond do
            String.starts_with?(start_key, SystemKeys.shard_keys_prefix()) -> {:ok, {shard_entries, false}}
            String.starts_with?(start_key, SystemKeys.materializers_prefix()) -> {:ok, {refs_entries, false}}
          end
        end,
        commit_fn: fn _p, _e, encoded, _o ->
          send(test_pid, {:committed, encoded})
          {:ok, Version.from_integer(9), 0}
        end,
        get_fn: fn key, _v ->
          # commit_checked's fence read: we are the steady-state owner.
          if String.ends_with?(key, "owner"), do: {:ok, Process.get(:my_owner)}, else: {:error, :not_found}
        end
      }
    end

    defp swept_state(refs_entries, test_pid) do
      {lock, _} = Lock.take(nil, nil)
      Process.put(:my_owner, lock.my_owner)

      state(two_shard_snapshot_deps(refs_entries, test_pid),
        lock: lock,
        placeholder_start_fn: fn opts ->
          send(test_pid, {:placeholder_started, opts})
          {:ok, spawn(fn -> Process.sleep(:infinity) end)}
        end
      )
    end

    test "publishes placeholder refs for every uncovered tag in one fenced commit" do
      test_pid = self()

      # Tag 0 is covered; tag 1 has no entry — the gap.
      refs_entries = [
        {SystemKeys.materializer_key(0), Values.encode_materializer_ref("wkr_sys", Atom.to_string(node()))}
      ]

      assert {:noreply, t} = Server.handle_continue(:startup_sweep, swept_state(refs_entries, test_pid))
      assert t.snapshot.shard_layout == %{"m" => {1, <<>>}, <<0xFF, 0xFF>> => {0, "m"}}

      assert_received {:placeholder_started, opts}
      assert opts[:shard_layout] == t.snapshot.shard_layout

      assert_received {:committed, encoded}
      assert {:ok, mutations} = Transaction.mutations(encoded)

      placeholder_sets =
        for {:set, key, value} <- Enum.to_list(mutations),
            String.starts_with?(key, SystemKeys.materializers_prefix()),
            do: {key, Values.decode_materializer_ref(value)}

      node_string = Atom.to_string(node())

      assert placeholder_sets == [
               {SystemKeys.materializer_key(1), {:ok, {Placeholder.worker_id(), node_string}}}
             ]
    end

    test "pre-existing placeholder refs are not republished — same name, same node, still valid" do
      test_pid = self()
      node_string = Atom.to_string(node())

      refs_entries = [
        {SystemKeys.materializer_key(0), Values.encode_materializer_ref("wkr_sys", node_string)},
        {SystemKeys.materializer_key(1), Values.encode_materializer_ref(Placeholder.worker_id(), node_string)}
      ]

      assert {:noreply, _t} = Server.handle_continue(:startup_sweep, swept_state(refs_entries, test_pid))
      refute_received {:committed, _}
    end

    test "full coverage publishes nothing" do
      test_pid = self()

      refs_entries = [
        {SystemKeys.materializer_key(0), Values.encode_materializer_ref("wkr_sys", Atom.to_string(node()))},
        {SystemKeys.materializer_key(1), Values.encode_materializer_ref("wkr_a", Atom.to_string(node()))}
      ]

      assert {:noreply, _t} = Server.handle_continue(:startup_sweep, swept_state(refs_entries, test_pid))
      refute_received {:committed, _}
    end

    test "the sweep recruits its uncovered tags eagerly — no first-touch wait for demand" do
      test_pid = self()
      {lock, _} = Lock.take(nil, nil)
      Process.put(:my_owner, lock.my_owner)

      refs_entries = [
        {SystemKeys.materializer_key(0), Values.encode_materializer_ref("wkr_sys", Atom.to_string(node()))}
      ]

      t =
        state(two_shard_snapshot_deps(refs_entries, test_pid),
          lock: lock,
          placeholder_start_fn: fn _opts -> {:ok, spawn(fn -> Process.sleep(:infinity) end)} end,
          recruitment_ctx: %{
            cluster: __MODULE__,
            epoch: 3,
            node_capabilities: %{materializer: [node()]},
            logs: %{"log_1" => []},
            log_refs: %{"log_1" => :ref},
            create_worker_fn: fn _f, _i, _k, _o -> {:error, :scripted} end
          }
        )

      assert {:noreply, t2} = Server.handle_continue(:startup_sweep, t)
      assert MapSet.member?(t2.recruiting, 1)
      refute MapSet.member?(t2.recruiting, 0)
    end

    test "supersession at the publish cedes" do
      test_pid = self()
      {lock, _} = Lock.take(nil, nil)

      deps =
        Map.merge(two_shard_snapshot_deps([], test_pid), %{
          # A usurper owns the lock: the fence read refuses.
          get_fn: fn _k, _v -> {:ok, Lock.new_uid()} end,
          commit_fn: fn _p, _e, _t, _o -> flunk("must not commit past a refused fence") end
        })

      t =
        state(deps,
          lock: lock,
          placeholder_start_fn: fn _opts -> {:ok, spawn(fn -> Process.sleep(:infinity) end)} end
        )

      assert {:stop, :normal, _t} = Server.handle_continue(:startup_sweep, t)
    end
  end

  describe "coverage demand" do
    test "an already-covered tag hands the placeholder the callable ref" do
      test_pid = self()

      placeholder =
        spawn(fn ->
          receive do
            {:"$gen_cast", msg} -> send(test_pid, {:placeholder_got, msg})
          end
        end)

      t =
        state(%{},
          placeholder: placeholder,
          snapshot: %{shard_layout: %{}, materializer_refs: %{7 => {"wkr_a", Atom.to_string(node())}}}
        )

      assert {:noreply, _t} = Server.handle_cast({:coverage_demand, 7}, t)
      assert_receive {:placeholder_got, {:covered, 7, {_otp_name, _node}}}
    end

    test "an uncovered tag is recorded as pending demand for recruitment" do
      t = state(%{}, snapshot: %{shard_layout: %{}, materializer_refs: %{}})

      assert {:noreply, t} = Server.handle_cast({:coverage_demand, 9}, t)
      assert MapSet.member?(t.pending_demands, 9)
    end

    test "a placeholder-covered tag is still pending — the placeholder is not coverage" do
      t =
        state(%{},
          snapshot: %{
            shard_layout: %{},
            materializer_refs: %{9 => {Placeholder.worker_id(), Atom.to_string(node())}}
          }
        )

      assert {:noreply, t} = Server.handle_cast({:coverage_demand, 9}, t)
      assert MapSet.member?(t.pending_demands, 9)
    end
  end

  describe "placeholder lifecycle" do
    test "a failed placeholder restart stops :shutdown for the director's retry" do
      dead = spawn(fn -> :ok end)

      t =
        state(%{},
          placeholder: dead,
          snapshot: %{shard_layout: %{}, materializer_refs: %{}},
          placeholder_start_fn: fn _opts -> {:error, :nope} end
        )

      ExUnit.CaptureLog.capture_log(fn ->
        assert {:stop, {:shutdown, {:placeholder_restart_failed, :nope}}, _t} =
                 Server.handle_info({:EXIT, dead, :boom}, t)
      end)
    end

    test "a crashed placeholder restarts under the same name — no republication needed" do
      test_pid = self()
      dead = spawn(fn -> :ok end)
      fresh = spawn(fn -> Process.sleep(:infinity) end)

      t =
        state(%{},
          placeholder: dead,
          snapshot: %{shard_layout: %{"m" => {1, <<>>}}, materializer_refs: %{}},
          placeholder_start_fn: fn opts ->
            send(test_pid, {:restarted_with, opts[:shard_layout]})
            {:ok, fresh}
          end
        )

      log =
        ExUnit.CaptureLog.capture_log(fn ->
          assert {:noreply, t2} = Server.handle_info({:EXIT, dead, :boom}, t)
          assert t2.placeholder == fresh
        end)

      assert log =~ "placeholder exited"
      assert_received {:restarted_with, %{"m" => {1, <<>>}}}
    end
  end

  describe "demand-driven recruitment" do
    defp recruiting_state(overrides) do
      {lock, _} = Lock.take(nil, nil)
      Process.put(:my_owner, lock.my_owner)

      deps = %{
        get_fn: fn key, _v ->
          if String.ends_with?(key, "owner"), do: {:ok, Process.get(:my_owner)}, else: {:error, :not_found}
        end
      }

      state(
        deps,
        Keyword.merge(
          [
            lock: lock,
            placeholder: Keyword.get(overrides, :placeholder, self()),
            snapshot: %{shard_layout: %{}, materializer_refs: %{}},
            recruitment_ctx: %{
              cluster: __MODULE__,
              epoch: 3,
              node_capabilities: %{materializer: [node()]},
              logs: %{},
              log_refs: %{},
              create_worker_fn: fn _f, _i, _k, _o -> {:error, :scripted_creation_failure} end,
              remove_worker_fn: fn _f, id, _o ->
                send(self(), :never_used)
                {:removed_sync, id}
              end
            }
          ],
          overrides
        )
      )
    end

    test "an uncovered demand starts recruitment once; a second demand is deduped by the in-flight set" do
      t = recruiting_state([])

      assert {:noreply, t} = Server.handle_cast({:coverage_demand, 9}, t)
      assert MapSet.member?(t.recruiting, 9)
      assert_receive {:recruitment_complete, 9, {:error, _}}, 500

      # Second demand while in flight: no second task.
      assert {:noreply, t2} = Server.handle_cast({:coverage_demand, 9}, t)
      assert t2.recruiting == t.recruiting
      refute_receive {:recruitment_complete, 9, _}, 100
    end

    test "a successful recruit publishes under the fence, updates the view, and drains the placeholder" do
      test_pid = self()

      placeholder =
        spawn(fn ->
          receive do
            {:"$gen_cast", msg} -> send(test_pid, {:placeholder_got, msg})
          end
        end)

      commit_recorder = fn _p, _e, encoded, _o ->
        send(test_pid, {:committed, encoded})
        {:ok, Version.from_integer(9), 0}
      end

      t = recruiting_state(placeholder: placeholder)
      t = %{t | deps: Map.put(t.deps, :commit_fn, commit_recorder), recruiting: MapSet.new([9])}

      recruited = spawn(fn -> Process.sleep(:infinity) end)

      assert {:noreply, t2} =
               Server.handle_info({:recruitment_complete, 9, {:ok, recruited, node(), "wkr_new"}}, t)

      assert_received {:committed, encoded}
      assert {:ok, mutations} = Transaction.mutations(encoded)

      assert Enum.any?(Enum.to_list(mutations), fn
               {:set, key, _} -> key == Bedrock.SystemKeys.materializer_key(9)
               _ -> false
             end)

      assert_receive {:placeholder_got, {:covered, 9, {_otp, _node}}}
      assert t2.snapshot.materializer_refs[9] == {"wkr_new", Atom.to_string(node())}
      refute MapSet.member?(t2.recruiting, 9)

      # The published assignment is monitored from this moment — healing
      # coverage starts at publication, not at the next sweep.
      assert Map.values(t2.assignment_monitors) == [9]
    end

    test "a superseded publish removes the orphan and cedes" do
      test_pid = self()

      t = recruiting_state([])

      superseding_deps =
        Map.merge(t.deps, %{
          get_fn: fn _k, _v -> {:ok, Lock.new_uid()} end,
          commit_fn: fn _p, _e, _t, _o -> flunk("must not commit past a refused fence") end
        })

      ctx =
        Map.put(t.recruitment_ctx, :remove_worker_fn, fn _f, id, _o ->
          send(test_pid, {:orphan_removed, id})
          :ok
        end)

      t = %{t | deps: superseding_deps, recruitment_ctx: ctx}

      log =
        ExUnit.CaptureLog.capture_log(fn ->
          assert {:stop, :normal, _t} =
                   Server.handle_info({:recruitment_complete, 9, {:ok, self(), node(), "wkr_new"}}, t)

          assert_received {:orphan_removed, "wkr_new"}
        end)

      assert log =~ "superseded publishing"
    end

    test "a failed recruit sheds the tag at the placeholder and backs off" do
      test_pid = self()

      placeholder =
        spawn(fn ->
          receive do
            {:"$gen_cast", msg} -> send(test_pid, {:placeholder_got, msg})
          end
        end)

      t = recruiting_state(placeholder: placeholder)

      log =
        ExUnit.CaptureLog.capture_log(fn ->
          assert {:noreply, t2} = Server.handle_info({:recruitment_complete, 9, {:error, :no_nodes}}, t)

          assert_receive {:placeholder_got, {:coverage_failed, 9, :no_nodes}}
          assert Map.has_key?(t2.backoff, 9)

          # Backoff suppresses an immediate re-recruit.
          assert {:noreply, t3} = Server.handle_cast({:coverage_demand, 9}, t2)
          refute MapSet.member?(t3.recruiting, 9)
        end)

      assert log =~ "recruitment for tag 9 failed"
    end
  end

  describe "failure containment" do
    test "a crashed recruit task synthesizes a failed completion — the tag never wedges" do
      test_pid = self()

      placeholder =
        spawn(fn ->
          receive do
            {:"$gen_cast", msg} -> send(test_pid, {:placeholder_got, msg})
          end
        end)

      {lock, _} = Lock.take(nil, nil)

      t =
        state(%{},
          lock: lock,
          placeholder: placeholder,
          snapshot: %{shard_layout: %{}, materializer_refs: %{}},
          recruitment_ctx: %{
            cluster: __MODULE__,
            epoch: 3,
            node_capabilities: %{materializer: [node()]},
            logs: %{"log_1" => []},
            log_refs: %{"log_1" => :ref},
            create_worker_fn: fn _f, _i, _k, _o -> raise "task crash" end
          }
        )

      ExUnit.CaptureLog.capture_log(fn ->
        assert {:noreply, t} = Server.handle_cast({:coverage_demand, 9}, t)
        assert MapSet.member?(t.recruiting, 9)
        assert map_size(t.recruit_task_refs) == 1

        # The crash arrives as a DOWN; the server must synthesize the
        # failed completion itself (handler-level: drive the DOWN).
        assert_receive {:DOWN, ref, :process, _pid, {%RuntimeError{}, _}} = down
        assert Map.fetch!(t.recruit_task_refs, ref) == 9

        assert {:noreply, t2} = Server.handle_info(down, t)
        refute MapSet.member?(t2.recruiting, 9)
        assert t2.recruit_task_refs == %{}
        assert_receive {:placeholder_got, {:coverage_failed, 9, {:recruit_task_crashed, _}}}
      end)
    end

    test "an ambiguous commit failure never removes the worker; a definitive abort does" do
      test_pid = self()
      {lock, _} = Lock.take(nil, nil)
      Process.put(:my_owner, lock.my_owner)

      base_deps = %{
        get_fn: fn key, _v ->
          if String.ends_with?(key, "owner"), do: {:ok, Process.get(:my_owner)}, else: {:error, :not_found}
        end
      }

      ctx = %{
        cluster: __MODULE__,
        epoch: 3,
        node_capabilities: %{materializer: [node()]},
        logs: %{},
        log_refs: %{},
        remove_worker_fn: fn _f, id, _o ->
          send(test_pid, {:orphan_removed, id})
          :ok
        end
      }

      base =
        state(base_deps,
          lock: lock,
          placeholder: spawn(fn -> Process.sleep(:infinity) end),
          snapshot: %{shard_layout: %{}, materializer_refs: %{}},
          recruitment_ctx: ctx
        )

      # Ambiguous: the commit timed out — it MAY have landed. Removing
      # the worker would durably name a deleted worker.
      ambiguous = %{base | deps: Map.put(base.deps, :commit_fn, fn _p, _e, _t, _o -> {:error, :timeout} end)}

      ExUnit.CaptureLog.capture_log(fn ->
        assert {:noreply, _} =
                 Server.handle_info({:recruitment_complete, 9, {:ok, self(), node(), "wkr_ambig"}}, ambiguous)
      end)

      refute_received {:orphan_removed, "wkr_ambig"}

      # Definitive: exhausted aborts mean the commit never landed.
      definitive = %{base | deps: Map.put(base.deps, :commit_fn, fn _p, _e, _t, _o -> {:error, :aborted} end)}

      ExUnit.CaptureLog.capture_log(fn ->
        assert {:noreply, _} =
                 Server.handle_info({:recruitment_complete, 9, {:ok, self(), node(), "wkr_orphan"}}, definitive)
      end)

      assert_received {:orphan_removed, "wkr_orphan"}
    end
  end

  describe "death healing" do
    alias Bedrock.SystemKeys, as: HealKeys
    alias Bedrock.SystemKeys.Values, as: HealValues

    defp healing_state(test_pid, overrides) do
      {lock, _} = Lock.take(nil, nil)
      Process.put(:my_owner, lock.my_owner)

      deps = %{
        get_fn: fn key, _v ->
          if String.ends_with?(key, "owner"), do: {:ok, Process.get(:my_owner)}, else: {:error, :not_found}
        end,
        commit_fn: fn _p, _e, encoded, _o ->
          send(test_pid, {:committed, encoded})
          {:ok, Version.from_integer(9), 0}
        end
      }

      placeholder =
        spawn(fn ->
          receive do
            {:"$gen_cast", msg} -> send(test_pid, {:placeholder_got, msg})
          end
        end)

      state(
        deps,
        Keyword.merge(
          [
            lock: lock,
            placeholder: placeholder,
            snapshot: %{
              shard_layout: %{},
              materializer_refs: %{7 => {"wkr_dead", Atom.to_string(node())}}
            },
            recruitment_ctx: %{
              cluster: __MODULE__,
              epoch: 3,
              node_capabilities: %{materializer: [node()]},
              logs: %{"log_1" => []},
              log_refs: %{"log_1" => :ref},
              create_worker_fn: fn _f, _i, _k, _o -> {:error, :scripted} end
            }
          ],
          overrides
        )
      )
    end

    test "an assignment's death publishes the placeholder, uncovers the tag, and re-recruits" do
      test_pid = self()
      ref = make_ref()
      t = healing_state(test_pid, [])
      t = %{t | assignment_monitors: %{ref => 7}}

      log =
        ExUnit.CaptureLog.capture_log(fn ->
          assert {:noreply, t2} = Server.handle_info({:DOWN, ref, :process, self(), :killed}, t)

          # The gap became keyspace-visible first.
          assert_received {:committed, encoded}
          assert {:ok, mutations} = Transaction.mutations(encoded)

          placeholder_ref = HealValues.encode_materializer_ref(Placeholder.worker_id(), Atom.to_string(node()))
          assert {:set, HealKeys.materializer_key(7), placeholder_ref} in Enum.to_list(mutations)

          # Park, don't forward; and the re-recruit is in flight. (The
          # stub forwards a cast: wait, don't demand prior arrival.)
          assert_receive {:placeholder_got, {:uncovered, 7}}
          assert MapSet.member?(t2.recruiting, 7)
          assert t2.snapshot.materializer_refs[7] == {Placeholder.worker_id(), Atom.to_string(node())}
          assert t2.assignment_monitors == %{}
        end)

      assert log =~ "materializer for tag 7 down"
    end

    test "a superseded healing publish cedes — a newer owner heals, not us" do
      test_pid = self()
      ref = make_ref()

      t = healing_state(test_pid, [])

      superseding_deps =
        Map.merge(t.deps, %{
          get_fn: fn _k, _v -> {:ok, Lock.new_uid()} end,
          commit_fn: fn _p, _e, _t, _o -> flunk("must not commit past a refused fence") end
        })

      t = %{t | deps: superseding_deps, assignment_monitors: %{ref => 7}}

      ExUnit.CaptureLog.capture_log(fn ->
        assert {:stop, :normal, _t} = Server.handle_info({:DOWN, ref, :process, self(), :killed}, t)
      end)
    end

    test "an idle spin-down parks the tag WITHOUT re-recruiting — revival is demand-driven" do
      test_pid = self()
      ref = make_ref()
      t = healing_state(test_pid, [])
      t = %{t | assignment_monitors: %{ref => 7}}

      log =
        ExUnit.CaptureLog.capture_log(fn ->
          assert {:noreply, t2} = Server.handle_info({:DOWN, ref, :process, self(), {:shutdown, :idle}}, t)

          # The placeholder swap is published and the tag uncovered —
          # exactly like a heal — but NO recruit task starts.
          assert_received {:committed, _}
          assert_receive {:placeholder_got, {:uncovered, 7}}
          refute MapSet.member?(t2.recruiting, 7)
          assert t2.snapshot.materializer_refs[7] == {Placeholder.worker_id(), Atom.to_string(node())}

          # The next read's demand is what revives the shard.
          assert {:noreply, t3} = Server.handle_cast({:coverage_demand, 7}, t2)
          assert MapSet.member?(t3.recruiting, 7)
        end)

      assert log =~ "spun down idle"
    end

    test "a degraded idle park falls back to healing — the keyspace still names the corpse and only a recruit corrects it" do
      test_pid = self()
      ref = make_ref()
      t = healing_state(test_pid, [])

      failing_deps = Map.put(t.deps, :commit_fn, fn _p, _e, _t, _o -> {:error, :timeout} end)
      t = %{t | deps: failing_deps, assignment_monitors: %{ref => 7}}

      log =
        ExUnit.CaptureLog.capture_log(fn ->
          assert {:noreply, t2} = Server.handle_info({:DOWN, ref, :process, self(), {:shutdown, :idle}}, t)

          # The publish failed: clients keep routing to the departed
          # worker via the keyspace, so no demand can ever fire — the
          # recruit (whose own publication self-corrects) must start.
          refute Map.has_key?(t2.snapshot.materializer_refs, 7)
          assert MapSet.member?(t2.recruiting, 7)
        end)

      assert log =~ "placeholder publish for tag 7 failed"
    end

    test "a :noconnection DOWN does not heal — the tag is verified, not stampeded" do
      test_pid = self()
      ref = make_ref()
      t = healing_state(test_pid, [])
      t = %{t | assignment_monitors: %{ref => 7}, reverify_interval_ms: 5}

      log =
        ExUnit.CaptureLog.capture_log(fn ->
          assert {:noreply, t2} = Server.handle_info({:DOWN, ref, :process, self(), :noconnection}, t)

          # Nothing destructive happened: no publish, no uncover, no
          # recruit — the (possibly live) worker keeps its assignment...
          refute_received {:committed, _}
          refute_received {:placeholder_got, _}
          refute MapSet.member?(t2.recruiting, 7)
          assert t2.snapshot.materializer_refs[7] == {"wkr_dead", Atom.to_string(node())}

          # ...and verification is armed instead.
          assert_receive {:reverify_assignment, 7}, 100
        end)

      assert log =~ "unreachable; verifying"
    end

    test "persistent unreachability escalates to a heal after consecutive failed verifications" do
      test_pid = self()

      t =
        healing_state(test_pid,
          snapshot: %{shard_layout: %{}, materializer_refs: %{7 => {"wkr_gone", "gone@nowhere"}}}
        )

      t = %{t | reverify_interval_ms: 5}

      log =
        ExUnit.CaptureLog.capture_log(fn ->
          # Two failed pings only re-arm the verification timer...
          assert {:noreply, t} = Server.handle_info({:reverify_assignment, 7}, t)
          assert t.unreachable_counts[7] == 1
          refute_received {:committed, _}
          assert_receive {:reverify_assignment, 7}, 100

          assert {:noreply, t} = Server.handle_info({:reverify_assignment, 7}, t)
          assert t.unreachable_counts[7] == 2

          # ...the third escalates: publish, uncover, re-recruit.
          assert {:noreply, t3} = Server.handle_info({:reverify_assignment, 7}, t)
          assert_received {:committed, _}
          assert_receive {:placeholder_got, {:uncovered, 7}}
          assert MapSet.member?(t3.recruiting, 7)
          assert t3.unreachable_counts == %{}
        end)

      assert log =~ "unreachable after 3 verifications; healing"
    end

    test "a reachable node resets the count and re-arms the monitor; a re-assigned tag has nothing to verify" do
      test_pid = self()

      t =
        healing_state(test_pid,
          snapshot: %{shard_layout: %{}, materializer_refs: %{7 => {"wkr_alive", Atom.to_string(node())}}}
        )

      t = %{t | unreachable_counts: %{7 => 2}}

      assert {:noreply, t2} = Server.handle_info({:reverify_assignment, 7}, t)
      assert t2.unreachable_counts == %{}
      assert Map.values(t2.assignment_monitors) == [7]
      refute_received {:committed, _}

      # A tag re-assigned to the placeholder meanwhile is already
      # healing: verification just stands down.
      t3 = %{
        t
        | snapshot: %{t.snapshot | materializer_refs: %{7 => {Placeholder.worker_id(), Atom.to_string(node())}}}
      }

      assert {:noreply, t4} = Server.handle_info({:reverify_assignment, 7}, t3)
      assert t4.unreachable_counts == %{}
      assert t4.assignment_monitors == %{}
      refute_received {:committed, _}
    end

    test "a dead registered name heals through :noproc — monitor fires immediately, the heal chain runs" do
      test_pid = self()
      t = healing_state(test_pid, [])

      # A monitor on a never-registered local name yields an instant
      # :noproc DOWN — the exact signal a reachable node's dead worker
      # produces at re-arm time.
      ref = Process.monitor(otp_name_for_worker("wkr_dead"))
      assert_receive {:DOWN, ^ref, :process, _, :noproc}

      t = %{t | assignment_monitors: %{ref => 7}}

      ExUnit.CaptureLog.capture_log(fn ->
        assert {:noreply, t2} = Server.handle_info({:DOWN, ref, :process, nil, :noproc}, t)
        assert_received {:committed, _}
        assert_receive {:placeholder_got, {:uncovered, 7}}
        assert MapSet.member?(t2.recruiting, 7)
      end)
    end

    test "a transiently failed healing publish drops the corpse ref — demand recruits, never serves the corpse" do
      test_pid = self()
      ref = make_ref()
      t = healing_state(test_pid, [])

      failing_deps = Map.put(t.deps, :commit_fn, fn _p, _e, _t, _o -> {:error, :timeout} end)
      t = %{t | deps: failing_deps, assignment_monitors: %{ref => 7}}

      log =
        ExUnit.CaptureLog.capture_log(fn ->
          assert {:noreply, t2} = Server.handle_info({:DOWN, ref, :process, self(), :killed}, t)

          # The corpse's ref left the local view even though the keyspace
          # still names it: a racing demand falls to the recruit path
          # instead of handing parked readers a dead callable.
          refute Map.has_key?(t2.snapshot.materializer_refs, 7)

          assert {:noreply, t3} = Server.handle_cast({:coverage_demand, 7}, t2)
          assert MapSet.member?(t3.pending_demands, 7)
          refute_received {:placeholder_got, {:covered, 7, _}}
        end)

      assert log =~ "placeholder publish for tag 7 failed"
    end

    test "the startup sweep monitors live assignments but never the placeholder's own refs" do
      test_pid = self()
      node_string = Atom.to_string(node())

      refs_entries = [
        {HealKeys.materializer_key(0), HealValues.encode_materializer_ref("wkr_sys", node_string)},
        {HealKeys.materializer_key(1), HealValues.encode_materializer_ref(Placeholder.worker_id(), node_string)}
      ]

      assert {:noreply, t} = Server.handle_continue(:startup_sweep, swept_state(refs_entries, test_pid))

      assert map_size(t.assignment_monitors) == 1
      assert t.assignment_monitors |> Map.values() |> Enum.sort() == [0]
    end
  end
end
