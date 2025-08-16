defmodule Bedrock.DataPlane.Log.Shale.LongPulls do
  @moduledoc false

  alias Bedrock.Internal.WaitingList

  @spec normalize_timeout_to_ms(term()) :: pos_integer()
  def normalize_timeout_to_ms(n), do: n |> normalize_timeout() |> max(10) |> min(10_000)

  @spec normalize_timeout(term()) :: integer()
  defp normalize_timeout(n) when is_integer(n), do: n
  defp normalize_timeout(_), do: 5000

  @spec notify_waiting_pullers(WaitingList.t(), Bedrock.version(), Bedrock.transaction()) ::
          WaitingList.t()
  def notify_waiting_pullers(waiting_pullers, version, transaction) do
    {new_map, entries} = WaitingList.remove_all(waiting_pullers, version)

    # Reply to all waiting pullers for this version
    Enum.each(entries, fn {_deadline, reply_to_fn, _opts} ->
      reply_to_fn.({:ok, [transaction]})
    end)

    new_map
  end

  @spec try_to_add_to_waiting_pullers(
          waiting_pullers :: WaitingList.t(),
          monotonic_now :: integer(),
          reply_to_fn :: (any() -> :ok),
          from_version :: Bedrock.version(),
          opts :: keyword()
        ) ::
          {:error, :version_too_new} | {:ok, updated_waiting_pullers :: WaitingList.t()}
  def try_to_add_to_waiting_pullers(
        waiting_pullers,
        _monotonic_now,
        reply_to_fn,
        from_version,
        opts
      ) do
    {timeout_in_ms, opts} = opts |> Keyword.pop(:willing_to_wait_in_ms)

    if timeout_in_ms == nil do
      # Not willing to wait timeout, so we reply with an error
      {:error, :version_too_new}
    else
      # Add to waiting list with normalized timeout
      timeout_ms = normalize_timeout_to_ms(timeout_in_ms)

      {new_waiting_pullers, _timeout} =
        WaitingList.insert(waiting_pullers, from_version, opts, reply_to_fn, timeout_ms)

      {:ok, new_waiting_pullers}
    end
  end

  @spec process_expired_deadlines_for_waiting_pullers(
          waiting_pullers :: WaitingList.t(),
          _monotic_now :: integer()
        ) :: WaitingList.t()
  def process_expired_deadlines_for_waiting_pullers(waiting_pullers, _monotic_now) do
    {new_waiting_pullers, expired_entries} = WaitingList.expire(waiting_pullers)

    # Reply to expired pullers with empty result
    WaitingList.reply_to_expired(expired_entries, {:ok, []})

    new_waiting_pullers
  end

  @spec determine_timeout_for_next_puller_deadline(WaitingList.t(), integer()) ::
          pos_integer() | nil
  def determine_timeout_for_next_puller_deadline(waiting_pullers, _now) do
    case WaitingList.next_timeout(waiting_pullers) do
      :infinity -> nil
      timeout -> max(1, timeout)
    end
  end
end
