defmodule Bedrock.Internal.TimerManagement do
  defmacro __using__(opts) do
    type =
      opts[:type] ||
        quote do
          map()
        end

    quote do
      # Timer Management

      @spec cancel_timer(unquote(type)) :: unquote(type)
      def cancel_timer(%{timer_ref: nil} = t), do: t

      def cancel_timer(%{timer_ref: timer_ref} = t) do
        Process.cancel_timer(timer_ref)
        %{t | timer_ref: nil}
      end

      @spec set_timer(unquote(type), name :: atom(), Bedrock.timeout_in_ms()) :: unquote(type)
      def set_timer(%{timer_ref: nil} = t, name, timeout_in_ms),
        do: %{t | timer_ref: Process.send_after(self(), {:timeout, name}, timeout_in_ms)}
    end
  end
end
