defmodule Bedrock.DataPlane.Log.Shale.LongPulls do
  @spec normalize_timeout_to_ms(term()) :: pos_integer()
  def normalize_timeout_to_ms(n), do: n |> normalize_timeout() |> max(10) |> min(10_000)

  @spec normalize_timeout(term()) :: integer()
  defp normalize_timeout(n) when is_integer(n), do: n
  defp normalize_timeout(_), do: 5000

  @spec notify_waiting_pullers(map(), Bedrock.version(), Bedrock.transaction()) :: map()
  def notify_waiting_pullers(waiting_pullers, version, transaction) do
    {pullers, remaining_waiting_pullers} = Map.pop(waiting_pullers, version)

    unless is_nil(pullers) do
      pullers
      |> Enum.each(fn {_, reply_to_fn, _opts} ->
        reply_to_fn.({:ok, [transaction]})
      end)
    end

    remaining_waiting_pullers
  end

  @spec try_to_add_to_waiting_pullers(
          waiting_pullers :: map(),
          monotonic_now :: integer(),
          reply_to_fn :: (any() -> :ok),
          from_version :: Bedrock.version(),
          opts :: keyword()
        ) ::
          {:error, :version_too_new} | {:ok, updated_waiting_pullers :: map()}
  def try_to_add_to_waiting_pullers(
        waiting_pullers,
        monotonic_now,
        reply_to_fn,
        from_version,
        opts
      ) do
    {timeout_in_ms, opts} = opts |> Keyword.pop(:willing_to_wait_in_ms)

    if timeout_in_ms == nil do
      # Not willing to wait timeout, so we reply with an error
      {:error, :version_too_new}
    else
      # Calculate the deadline for this puller and add it to the waiting pullers
      deadline = monotonic_now + normalize_timeout_to_ms(timeout_in_ms)
      puller = {deadline, reply_to_fn, opts}
      {:ok, Map.update(waiting_pullers, from_version, [puller], &[puller | &1])}
    end
  end

  @spec process_expired_deadlines_for_waiting_pullers(
          waiting_pullers :: map(),
          monotic_now :: integer()
        ) :: map()
  def process_expired_deadlines_for_waiting_pullers(waiting_pullers, monotic_now) do
    waiting_pullers
    |> Enum.map(fn {version, pullers} ->
      pullers
      |> Enum.reduce([], fn
        {deadline, reply_to_fn, _opts}, acc when deadline <= monotic_now ->
          reply_to_fn.({:ok, []})
          acc

        puller, acc ->
          [puller | acc]
      end)
      |> case do
        [] -> nil
        pullers -> {version, pullers}
      end
    end)
    |> Enum.reject(&is_nil/1)
    |> Map.new()
  end

  @spec determine_timeout_for_next_puller_deadline(map(), integer()) ::
          pos_integer() | nil
  def determine_timeout_for_next_puller_deadline(waiting_pullers, now) do
    waiting_pullers
    |> Map.values()
    |> Enum.map(&Enum.min/1)
    |> Enum.min(fn -> nil end)
    |> case do
      nil ->
        nil

      {next_deadline, _, _} ->
        max(1, next_deadline - now)
    end
  end
end
