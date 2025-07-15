defmodule Bedrock.Internal.TimerManagement do
  defmacro __using__(_opts) do
    quote do
      import Bedrock.Internal.TimerManagement,
        only: [cancel_all_timers: 1, cancel_timer: 2, set_timer: 3]
    end
  end

  # Timer Management

  @spec cancel_all_timers(map()) :: map()
  def cancel_all_timers(%{timers: nil} = t), do: t

  def cancel_all_timers(%{} = t) do
    update_in(t.timers, fn timers ->
      timers |> Enum.each(&Process.cancel_timer(&1 |> elem(1)))
      %{}
    end)
  end

  @spec cancel_timer(map(), atom()) :: map()
  def cancel_timer(%{timers: nil} = t, _name), do: t

  def cancel_timer(%{} = t, name) do
    {timer_ref, timers} = Map.pop(t.timers, name)

    case timer_ref do
      nil ->
        t

      _ ->
        Process.cancel_timer(timer_ref)
        put_in(t.timers, timers)
    end
  end

  @spec set_timer(map(), atom(), pos_integer()) :: map()
  def set_timer(%{} = t, name, timeout_in_ms) do
    update_in(
      t.timers,
      fn
        %{^name => _} ->
          raise("Timer #{name} already set")

        timers ->
          Map.put(
            timers || %{},
            name,
            Process.send_after(self(), {:timeout, name}, timeout_in_ms)
          )
      end
    )
  end
end
