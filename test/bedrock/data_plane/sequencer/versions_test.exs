defmodule Bedrock.DataPlane.Sequencer.VersionsTest do
  use ExUnit.Case
  alias Bedrock.DataPlane.Sequencer.Versions
  alias Bedrock.DataPlane.Sequencer.State

  @epoch 1
  @max_transactions_per_ms 10

  setup do
    now = Versions.monotonic_time_in_ms()

    state = %State{
      epoch: @epoch,
      started_at: now,
      last_timestamp_at: now,
      sequence: 0,
      max_transactions_per_ms: @max_transactions_per_ms
    }

    {:ok, state: state}
  end

  test "next_version increments sequence within the same millisecond", %{state: state} do
    {version1, state1} = Versions.next_version(state)
    {version2, state2} = Versions.next_version(state1)

    assert state1.sequence == 1
    assert state2.sequence == 2
    assert version1 != version2
  end

  test "next_version resets sequence and advances timestamp when max transactions per ms is reached",
       %{state: state} do
    1..5_000
    |> Enum.reduce(
      Map.put(state, :max_transactions_per_ms, 1000),
      fn _i, initial_state ->
        {next_version, new_state} = Versions.next_version(initial_state)

        assert next_version > initial_state.last_committed_version
        %{new_state | last_committed_version: next_version}
      end
    )
  end

  test "spin_until_next_ms spins until the next millisecond" do
    ms = Versions.monotonic_time_in_ms()
    next_ms = Versions.spin_until_next_ms(ms)

    assert next_ms > ms
  end

  test "encode and decode version", %{state: state} do
    {version, _} = Versions.next_version(state)
    {epoch, offset_ms, sequence} = Versions.decode(version)

    assert epoch == state.epoch
    assert offset_ms == Versions.monotonic_time_in_ms() - state.started_at
    assert sequence == 1
  end

  test "pack and unpack values" do
    for value <- [
          0xFF,
          0xFFFF,
          0xFFFFFF,
          0xFFFFFFFF,
          0xFFFFFFFFFF,
          0xFFFFFFFFFFFF,
          0xFFFFFFFFFFFFFF,
          0xFFFFFFFFFFFFFFFF
        ] do
      packed = Versions.pack(value)
      {unpacked, _} = Versions.unpack(packed)
      assert unpacked == value
    end
  end
end
