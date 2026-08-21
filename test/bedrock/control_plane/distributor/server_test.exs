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
  alias Bedrock.ControlPlane.Distributor.Server
  alias Bedrock.ControlPlane.Distributor.State
  alias Bedrock.DataPlane.Version

  defp scripted_deps(overrides) do
    Map.merge(
      %{
        epoch: 3,
        proxies: [:proxy],
        next_read_version_fn: fn -> {:ok, Version.from_integer(1)} end,
        get_fn: fn _key, _version -> {:error, :not_found} end,
        commit_fn: fn _proxy, _epoch, _encoded, _opts -> {:ok, Version.from_integer(2), 0} end
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
    test "success installs the lock and arms the poll" do
      assert {:noreply, %State{lock: %Lock{}} = t} = Server.handle_continue(:take_lock, state(%{}))
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
end
