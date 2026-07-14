defmodule Bedrock.DataPlane.Materializer.Basalt.LogicTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Materializer.Basalt.Database
  alias Bedrock.DataPlane.Materializer.Basalt.Logic
  alias Bedrock.DataPlane.Materializer.Basalt.State
  alias Bedrock.DataPlane.Version
  alias Bedrock.Test.DataPlane.TransactionTestSupport

  defmodule FakeLog do
    @moduledoc false
    # Minimal log server: replies to `Log.pull/3` calls with the pre-loaded
    # batches (one batch per pull), then keeps replying with empty batches.
    # Notifies the test process when the batches have been drained, which can
    # only happen after the puller has applied the previous batch.
    use GenServer

    def start_link(test_pid, batches), do: GenServer.start_link(__MODULE__, {test_pid, batches})

    @impl true
    def init(state), do: {:ok, state}

    @impl true
    def handle_call({:pull, _start_after, _opts}, _from, {test_pid, [batch | rest]}) do
      {:reply, {:ok, batch}, {test_pid, rest}}
    end

    def handle_call({:pull, _start_after, _opts}, _from, {test_pid, []}) do
      send(test_pid, :fake_log_drained)
      {:reply, {:ok, []}, {test_pid, []}}
    end
  end

  defp random_name, do: String.to_atom("basalt_logic_#{Faker.random_between(0, 10_000)}")

  defp start_state(tmp_dir) do
    otp_name = random_name()
    {:ok, state} = Logic.startup(otp_name, self(), "basalt_logic_test_worker", tmp_dir)
    {otp_name, state}
  end

  defp apply_transaction(state, version_int, writes) do
    version = Version.from_integer(version_int)

    ^version =
      Database.apply_transactions(state.database, [
        TransactionTestSupport.new_log_transaction(version, writes)
      ])

    version
  end

  describe "shutdown/1" do
    @tag :tmp_dir
    test "closes the underlying database", %{tmp_dir: tmp_dir} do
      {otp_name, state} = start_state(tmp_dir)
      dets_table = :"#{otp_name}_db_pkv"

      refute :dets.info(dets_table) == :undefined
      assert :ok = Logic.shutdown(state)
      assert :dets.info(dets_table) == :undefined
    end
  end

  describe "lock_for_recovery/3" do
    @tag :tmp_dir
    test "refuses to lock for an epoch older than the current one", %{tmp_dir: tmp_dir} do
      {_otp_name, state} = start_state(tmp_dir)
      state = %{state | epoch: 10, director: :old_director}

      assert {:error, :newer_epoch_exists} = Logic.lock_for_recovery(state, :new_director, 5)

      Logic.shutdown(state)
    end

    @tag :tmp_dir
    test "locks, records the director and epoch when no puller is running", %{tmp_dir: tmp_dir} do
      {_otp_name, state} = start_state(tmp_dir)
      director = self()

      assert {:ok, %State{mode: :locked, director: ^director, epoch: 3, pull_task: nil}} =
               Logic.lock_for_recovery(state, director, 3)

      Logic.shutdown(state)
    end

    @tag :tmp_dir
    test "stops an active puller when locking", %{tmp_dir: tmp_dir} do
      {_otp_name, state} = start_state(tmp_dir)
      puller = Task.async(fn -> Process.sleep(:infinity) end)
      state = %{state | pull_task: puller}

      assert {:ok, %State{mode: :locked, epoch: 7, pull_task: nil}} =
               Logic.lock_for_recovery(state, self(), 7)

      refute Process.alive?(puller.pid)
      Logic.shutdown(state)
    end
  end

  describe "unlock_after_recovery/3" do
    @tag :tmp_dir
    test "starts a puller that applies pulled transactions to the database", %{tmp_dir: tmp_dir} do
      {_otp_name, state} = start_state(tmp_dir)
      version_1 = Version.from_integer(1)

      transaction = TransactionTestSupport.new_log_transaction(version_1, %{"foo" => "bar"})
      {:ok, fake_log} = FakeLog.start_link(self(), [[transaction]])

      layout = %{
        logs: %{"log_1" => []},
        services: %{"log_1" => %{kind: :log, status: {:up, fake_log}, last_seen: nil}}
      }

      assert {:ok, %State{mode: :running, pull_task: %Task{} = puller} = running_state} =
               Logic.unlock_after_recovery(state, Version.zero(), layout)

      # The fake log drains only after the puller has applied the first batch,
      # so once we see this message the transaction is visible in the database.
      assert_receive :fake_log_drained, 5_000
      assert {:ok, "bar"} = Logic.fetch(running_state, "foo", version_1)
      assert Database.last_committed_version(running_state.database) == version_1

      stopped_state = Logic.stop_pulling(running_state)
      refute Process.alive?(puller.pid)
      Logic.shutdown(stopped_state)
    end

    @tag :tmp_dir
    test "sends {:transactions_applied, version} to the process that started the puller", %{tmp_dir: tmp_dir} do
      {_otp_name, state} = start_state(tmp_dir)
      version_1 = Version.from_integer(1)

      transaction = TransactionTestSupport.new_log_transaction(version_1, %{"foo" => "bar"})
      {:ok, fake_log} = FakeLog.start_link(self(), [[transaction]])

      layout = %{
        logs: %{"log_1" => []},
        services: %{"log_1" => %{kind: :log, status: {:up, fake_log}, last_seen: nil}}
      }

      assert {:ok, running_state} = Logic.unlock_after_recovery(state, Version.zero(), layout)

      # The notification must arrive in the process that called
      # unlock_after_recovery (the server), not in the puller task's own
      # mailbox: the server's handle_info({:transactions_applied, _}, _) is
      # the only code path that notifies waitlisted fetches.
      assert_receive {:transactions_applied, ^version_1}, 5_000

      stopped_state = Logic.stop_pulling(running_state)
      Logic.shutdown(stopped_state)
    end

    @tag :tmp_dir
    test "purges transactions newer than the durable version before pulling", %{tmp_dir: tmp_dir} do
      {_otp_name, state} = start_state(tmp_dir)
      version_1 = apply_transaction(state, 1, %{"keep" => "yes"})
      :ok = Database.ensure_durability_to_version(state.database, version_1)
      apply_transaction(state, 2, %{"drop" => "yes"})

      layout = %{logs: %{}, services: %{}}

      assert {:ok, %State{mode: :running} = running_state} =
               Logic.unlock_after_recovery(state, version_1, layout)

      assert {:ok, "yes"} = Logic.fetch(running_state, "keep", version_1)
      assert {:error, :not_found} = Logic.fetch(running_state, "drop", Version.from_integer(2))

      stopped_state = Logic.stop_pulling(running_state)
      Logic.shutdown(stopped_state)
    end
  end

  describe "fetch/3" do
    @tag :tmp_dir
    test "returns the value committed at or before the requested version", %{tmp_dir: tmp_dir} do
      {_otp_name, state} = start_state(tmp_dir)
      version_1 = apply_transaction(state, 1, %{"foo" => "bar"})

      assert {:ok, "bar"} = Logic.fetch(state, "foo", version_1)
      assert {:ok, "bar"} = Logic.fetch(state, "foo", Version.from_integer(2))
      Logic.shutdown(state)
    end

    @tag :tmp_dir
    test "returns :not_found for a key that was never written", %{tmp_dir: tmp_dir} do
      {_otp_name, state} = start_state(tmp_dir)

      assert {:error, :not_found} = Logic.fetch(state, "missing", Version.zero())
      Logic.shutdown(state)
    end
  end

  describe "try_fetch_or_waitlist/4" do
    @tag :tmp_dir
    test "returns the value when the key is readable at the requested version", %{tmp_dir: tmp_dir} do
      {_otp_name, state} = start_state(tmp_dir)
      version_1 = apply_transaction(state, 1, %{"foo" => "bar"})
      from = {self(), make_ref()}

      assert {:ok, "bar", ^state} = Logic.try_fetch_or_waitlist(state, "foo", version_1, from)
      Logic.shutdown(state)
    end

    @tag :tmp_dir
    test "returns :not_found when the key is missing at a committed version", %{tmp_dir: tmp_dir} do
      {_otp_name, state} = start_state(tmp_dir)
      version_1 = apply_transaction(state, 1, %{"foo" => "bar"})
      from = {self(), make_ref()}

      assert {:error, :not_found, ^state} = Logic.try_fetch_or_waitlist(state, "missing", version_1, from)
      Logic.shutdown(state)
    end

    @tag :tmp_dir
    test "returns :version_too_old when the version has been purged from the window", %{tmp_dir: tmp_dir} do
      {_otp_name, state} = start_state(tmp_dir)
      version_1 = apply_transaction(state, 1, %{"foo" => "bar"})
      version_2 = apply_transaction(state, 2, %{"foo" => "baz"})
      :ok = Database.ensure_durability_to_version(state.database, version_2)
      from = {self(), make_ref()}

      assert {:error, :version_too_old, ^state} = Logic.try_fetch_or_waitlist(state, "foo", version_1, from)
      Logic.shutdown(state)
    end

    @tag :tmp_dir
    test "returns :key_out_of_range without waitlisting", %{tmp_dir: tmp_dir} do
      {_otp_name, state} = start_state(tmp_dir)

      # Database.fetch/3 short-circuits on a database carrying a key_range;
      # mirror that shape while reusing the real MVCC for the version check.
      ranged_database = %{key_range: {"m", "z"}, mvcc: state.database.mvcc}
      ranged_state = %{state | database: ranged_database}
      from = {self(), make_ref()}

      assert {:error, :key_out_of_range, ^ranged_state} =
               Logic.try_fetch_or_waitlist(ranged_state, "a", Version.zero(), from)

      Logic.shutdown(state)
    end

    @tag :tmp_dir
    test "waitlists a fetch for a version that has not been applied yet", %{tmp_dir: tmp_dir} do
      {_otp_name, state} = start_state(tmp_dir)
      future_version = Version.from_integer(5)
      from = {self(), make_ref()}

      assert {:waitlist, %State{} = waitlisted_state} =
               Logic.try_fetch_or_waitlist(state, "foo", future_version, from)

      assert [{_deadline, reply_fn, {"foo", ^future_version}}] =
               Map.fetch!(waitlisted_state.waiting_fetches, future_version)

      assert is_function(reply_fn, 1)
      Logic.shutdown(state)
    end
  end

  describe "notify_waiting_fetches/2" do
    @tag :tmp_dir
    test "replies with the value once the awaited version is applied", %{tmp_dir: tmp_dir} do
      {_otp_name, state} = start_state(tmp_dir)
      future_version = Version.from_integer(5)
      ref = make_ref()

      {:waitlist, waitlisted_state} = Logic.try_fetch_or_waitlist(state, "foo", future_version, {self(), ref})

      apply_transaction(waitlisted_state, 5, %{"foo" => "bar"})
      notified_state = Logic.notify_waiting_fetches(waitlisted_state, future_version)

      assert_receive {^ref, {:ok, "bar"}}
      assert notified_state.waiting_fetches == %{}
      Logic.shutdown(state)
    end

    @tag :tmp_dir
    test "replies with :not_found when the awaited version lands without the key", %{tmp_dir: tmp_dir} do
      {_otp_name, state} = start_state(tmp_dir)
      future_version = Version.from_integer(5)
      ref = make_ref()

      {:waitlist, waitlisted_state} = Logic.try_fetch_or_waitlist(state, "missing", future_version, {self(), ref})

      apply_transaction(waitlisted_state, 5, %{"other" => "value"})
      notified_state = Logic.notify_waiting_fetches(waitlisted_state, future_version)

      assert_receive {^ref, {:error, :not_found}}
      assert notified_state.waiting_fetches == %{}
      Logic.shutdown(state)
    end

    @tag :tmp_dir
    test "leaves fetches waiting on other versions untouched", %{tmp_dir: tmp_dir} do
      {_otp_name, state} = start_state(tmp_dir)
      version_5 = Version.from_integer(5)
      version_9 = Version.from_integer(9)
      ref_5 = make_ref()
      ref_9 = make_ref()

      {:waitlist, state_1} = Logic.try_fetch_or_waitlist(state, "foo", version_5, {self(), ref_5})
      {:waitlist, state_2} = Logic.try_fetch_or_waitlist(state_1, "foo", version_9, {self(), ref_9})

      apply_transaction(state_2, 5, %{"foo" => "bar"})
      notified_state = Logic.notify_waiting_fetches(state_2, version_5)

      assert_receive {^ref_5, {:ok, "bar"}}
      refute_receive {^ref_9, _}
      assert Map.keys(notified_state.waiting_fetches) == [version_9]
      Logic.shutdown(state)
    end
  end

  describe "info/2" do
    @tag :tmp_dir
    test "returns an error tuple for an unsupported fact", %{tmp_dir: tmp_dir} do
      {_otp_name, state} = start_state(tmp_dir)

      assert {:ok, {:error, :unsupported_info}} = Logic.info(state, :bogus_fact)
      Logic.shutdown(state)
    end

    @tag :tmp_dir
    test "returns a map of facts, including errors for unsupported ones", %{tmp_dir: tmp_dir} do
      {otp_name, state} = start_state(tmp_dir)

      assert {:ok, facts} = Logic.info(state, [:kind, :otp_name, :id, :bogus_fact])

      assert facts == %{
               kind: :materializer,
               otp_name: otp_name,
               id: "basalt_logic_test_worker",
               bogus_fact: {:error, :unsupported_info}
             }

      Logic.shutdown(state)
    end
  end
end
