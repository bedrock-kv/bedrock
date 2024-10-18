defmodule Bedrock.DataPlane.Sequencer.Versions do
  alias Bedrock.DataPlane.Sequencer.State

  import Bedrock.DataPlane.Sequencer.Telemetry, only: [track_versions: 4]

  @spec next_version(State.t()) :: {Bedrock.version(), State.t()}
  def next_version(t) do
    %{last_timestamp_at: last_timestamp_at} = t

    {now, sequence} =
      case monotonic_time_in_ms() - last_timestamp_at do
        0 ->
          sequence = t.sequence + 1

          if sequence < t.max_transactions_per_ms do
            {last_timestamp_at, sequence}
          else
            track_versions(t.epoch, last_timestamp_at, sequence, t.last_committed_version)
            {spin_until_next_ms(last_timestamp_at), 0}
          end

        adv ->
          {last_timestamp_at + adv, 0}
      end

    version = encode(t.epoch, now - t.started_at, sequence)
    {version, %{t | last_timestamp_at: now, sequence: sequence}}
  end

  @spec spin_until_next_ms(ms :: Bedrock.timestamp_in_ms()) :: Bedrock.timestamp_in_ms()
  def spin_until_next_ms(ms) do
    now = monotonic_time_in_ms()

    if now == ms do
      spin_until_next_ms(ms)
    else
      now
    end
  end

  @spec encode(Bedrock.epoch(), Bedrock.timestamp_in_ms(), integer()) :: Bedrock.version()
  def encode(epoch, timestamp, sequence) do
    pack(epoch) <>
      pack(timestamp) <>
      <<sequence::unsigned-big-integer-size(12), 0::unsigned-big-integer-size(4)>>
  end

  @spec monotonic_time_in_ms() :: Bedrock.timestamp_in_ms()
  def monotonic_time_in_ms, do: :erlang.monotonic_time(:millisecond)

  @spec pack(integer()) :: binary()
  def pack(v) when v <= 0xFF, do: <<0x15, v::unsigned-big-integer-size(8)>>
  def pack(v) when v <= 0xFFFF, do: <<0x16, v::unsigned-big-integer-size(16)>>
  def pack(v) when v <= 0xFFFFFF, do: <<0x17, v::unsigned-big-integer-size(24)>>
  def pack(v) when v <= 0xFFFFFFFF, do: <<0x18, v::unsigned-big-integer-size(32)>>
  def pack(v) when v <= 0xFFFFFFFFFF, do: <<0x19, v::unsigned-big-integer-size(40)>>
  def pack(v) when v <= 0xFFFFFFFFFFFF, do: <<0x1A, v::unsigned-big-integer-size(48)>>
  def pack(v) when v <= 0xFFFFFFFFFFFFFF, do: <<0x1B, v::unsigned-big-integer-size(56)>>
  def pack(v), do: <<0x1C, v::unsigned-big-integer-size(64)>>

  @spec decode(Bedrock.version()) ::
          {Bedrock.epoch(), Bedrock.timestamp_in_ms(), sequence :: integer()}
  def decode(value) when is_binary(value) do
    {epoch, rest} = unpack(value)

    {offset_ms, <<sequence::unsigned-big-integer-size(12), _flags::unsigned-big-integer-size(4)>>} =
      unpack(rest)

    {epoch, offset_ms, sequence}
  end

  def unpack(<<0x15, v::unsigned-big-integer-size(8)>> <> r), do: {v, r}
  def unpack(<<0x16, v::unsigned-big-integer-size(16)>> <> r), do: {v, r}
  def unpack(<<0x17, v::unsigned-big-integer-size(24)>> <> r), do: {v, r}
  def unpack(<<0x18, v::unsigned-big-integer-size(32)>> <> r), do: {v, r}
  def unpack(<<0x19, v::unsigned-big-integer-size(40)>> <> r), do: {v, r}
  def unpack(<<0x1A, v::unsigned-big-integer-size(48)>> <> r), do: {v, r}
  def unpack(<<0x1B, v::unsigned-big-integer-size(56)>> <> r), do: {v, r}
  def unpack(<<0x1C, v::unsigned-big-integer-size(64)>> <> r), do: {v, r}
end
