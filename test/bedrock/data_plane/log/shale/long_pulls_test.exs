defmodule Bedrock.DataPlane.Log.Shale.LongPullsTest do
  use ExUnit.Case

  alias Bedrock.DataPlane.Log.Shale.LongPulls

  defp reply_fn(tag) do
    fn reply ->
      send(self(), {tag, reply})
      :ok
    end
  end

  describe "normalize_timeout_to_ms/1" do
    test "returns a value between 10 and 10_000" do
      assert LongPulls.normalize_timeout_to_ms(5) == 10
      assert LongPulls.normalize_timeout_to_ms(15_000) == 10_000
      assert LongPulls.normalize_timeout_to_ms(500) == 500
    end
  end

  describe "notify_waiting_pullers/3" do
    test "notifies the correct pullers and removes them from the map" do
      waiting_pullers = %{
        1 => [{0, reply_fn(1), []}],
        2 => [{0, reply_fn(2), []}]
      }

      version = 1
      transaction = :transaction

      updated_waiting_pullers =
        LongPulls.notify_waiting_pullers(waiting_pullers, version, transaction)

      assert_receive {1, {:ok, [^transaction]}}
      refute_receive {2, {:ok, [^transaction]}}

      assert Map.has_key?(updated_waiting_pullers, version) == false
    end
  end

  describe "try_to_add_to_waiting_pullers/4" do
    test "returns an error if not willing to wait" do
      waiting_pullers = %{}
      montonic_now = :erlang.monotonic_time(:millisecond)
      reply_to_fn = fn _ -> :ok end
      from_version = 1
      opts = []

      assert {:error, :version_too_new} ==
               LongPulls.try_to_add_to_waiting_pullers(
                 waiting_pullers,
                 montonic_now,
                 reply_to_fn,
                 from_version,
                 opts
               )
    end

    test "adds puller to the waiting pullers map" do
      waiting_pullers = %{}
      montonic_now = :erlang.monotonic_time(:millisecond)
      reply_to_fn = fn _ -> :ok end
      from_version = 1
      opts = [willing_to_wait_in_ms: 1000]

      {:ok, updated_waiting_pullers} =
        LongPulls.try_to_add_to_waiting_pullers(
          waiting_pullers,
          montonic_now,
          reply_to_fn,
          from_version,
          opts
        )

      assert Map.has_key?(updated_waiting_pullers, from_version)

      assert Map.get(updated_waiting_pullers, from_version) == [
               {montonic_now + 1000, reply_to_fn, []}
             ]
    end

    test "adds a second puller to the waiting pullers map" do
      waiting_pullers = %{}
      monotonic_now = :erlang.monotonic_time(:millisecond)
      reply_to_fn = fn _ -> :ok end
      from_version = 1
      opts = [willing_to_wait_in_ms: 1000]

      {:ok, updated_waiting_pullers} =
        LongPulls.try_to_add_to_waiting_pullers(
          waiting_pullers,
          monotonic_now,
          reply_to_fn,
          from_version,
          opts
        )

      assert Map.has_key?(updated_waiting_pullers, from_version)

      assert Map.get(updated_waiting_pullers, from_version) == [
               {monotonic_now + 1000, reply_to_fn, []}
             ]

      reply_to_fn_2 = fn _ -> :ok end
      monotonic_now_2 = monotonic_now + :rand.uniform(1000)
      from_version_2 = 1
      opts_2 = [willing_to_wait_in_ms: 1000, some_other_option: :value]

      {:ok, updated_waiting_pullers} =
        LongPulls.try_to_add_to_waiting_pullers(
          updated_waiting_pullers,
          monotonic_now_2,
          reply_to_fn_2,
          from_version_2,
          opts_2
        )

      assert Map.has_key?(updated_waiting_pullers, from_version)

      assert Map.get(updated_waiting_pullers, from_version_2) == [
               {monotonic_now_2 + 1000, reply_to_fn_2, [some_other_option: :value]},
               {monotonic_now + 1000, reply_to_fn, []}
             ]
    end
  end

  describe "process_expired_deadlines_for_waiting_pullers/2" do
    test "removes expired pullers" do
      monotic_now = :erlang.monotonic_time(:millisecond)

      waiting_pullers = %{
        1 => [{monotic_now - 1, fn _ -> :ok end, []}],
        2 => [{monotic_now + 1000, fn _ -> :ok end, []}]
      }

      updated_waiting_pullers =
        LongPulls.process_expired_deadlines_for_waiting_pullers(waiting_pullers, monotic_now)

      assert Map.has_key?(updated_waiting_pullers, 1) == false
      assert Map.has_key?(updated_waiting_pullers, 2)
    end
  end

  describe "determine_timeout_for_next_puller_deadline/2" do
    test "returns the correct timeout" do
      now = :erlang.monotonic_time(:millisecond)

      waiting_pullers = %{
        1 => [{now + 1000, fn _ -> :ok end, []}],
        2 => [{now + 2000, fn _ -> :ok end, []}]
      }

      assert LongPulls.determine_timeout_for_next_puller_deadline(waiting_pullers, now) == 1000
    end

    test "returns nil if there are no pullers" do
      waiting_pullers = %{}
      now = :erlang.monotonic_time(:millisecond)

      assert LongPulls.determine_timeout_for_next_puller_deadline(waiting_pullers, now) == nil
    end
  end
end
