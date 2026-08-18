defmodule Bedrock.Internal.Repo.TransactionContext do
  @moduledoc false

  alias Bedrock.Internal.Time

  @process_key __MODULE__

  @type t :: %__MODULE__{
          builder: pid() | nil,
          deadline: integer() | :infinity | nil,
          timeout_in_ms: timeout() | nil
        }

  defstruct builder: nil,
            deadline: nil,
            timeout_in_ms: nil

  @spec builder(module()) :: pid() | nil
  def builder(repo), do: context(repo).builder

  @spec put_builder(module(), pid()) :: :ok
  def put_builder(repo, builder) when is_pid(builder) do
    update_context(repo, &%{&1 | builder: builder})
  end

  @spec clear_builder(module()) :: :ok
  def clear_builder(repo) do
    case context(repo) do
      %__MODULE__{deadline: nil} -> clear(repo)
      %__MODULE__{} = context -> put_context(repo, %{context | builder: nil})
    end
  end

  @spec clear(module()) :: :ok
  def clear(repo) do
    case Map.delete(contexts(), repo) do
      contexts when map_size(contexts) == 0 ->
        Process.delete(@process_key)
        :ok

      contexts ->
        Process.put(@process_key, contexts)
        :ok
    end
  end

  @spec with_deadline(module(), timeout(), (-> result)) :: result when result: term()
  def with_deadline(repo, timeout_in_ms, fun) when is_function(fun, 0) do
    previous_context = Map.get(contexts(), repo)

    case previous_context || %__MODULE__{} do
      %__MODULE__{deadline: nil} = context ->
        context = %{
          context
          | deadline: deadline_after(timeout_in_ms),
            timeout_in_ms: timeout_in_ms
        }

        put_context(repo, context)

        try do
          fun.()
        after
          restore_context(repo, previous_context)
        end

      %__MODULE__{} ->
        fun.()
    end
  end

  @spec remaining_timeout!(module(), term()) :: timeout()
  def remaining_timeout!(repo, last_reason) do
    case context(repo) do
      %__MODULE__{deadline: nil} ->
        :infinity

      %__MODULE__{deadline: :infinity} ->
        :infinity

      %__MODULE__{deadline: deadline} = context ->
        case deadline - Time.monotonic_now_in_ms() do
          remaining when remaining > 0 -> remaining
          _expired -> raise_timeout!(context, last_reason)
        end
    end
  end

  defp deadline_after(:infinity), do: :infinity

  defp deadline_after(timeout_in_ms) when is_integer(timeout_in_ms) and timeout_in_ms >= 0,
    do: Time.monotonic_now_in_ms() + timeout_in_ms

  defp deadline_after(invalid), do: raise(ArgumentError, "invalid transaction timeout: #{inspect(invalid)}")

  defp raise_timeout!(context, last_reason) do
    raise RuntimeError,
          "Transaction timed out after #{context.timeout_in_ms}ms. Last error: #{inspect(last_reason)}"
  end

  defp context(repo), do: Map.get(contexts(), repo, %__MODULE__{})
  defp contexts, do: Process.get(@process_key, %{})

  defp update_context(repo, update_fn) do
    repo
    |> context()
    |> update_fn.()
    |> then(&put_context(repo, &1))
  end

  defp put_context(repo, context) do
    Process.put(@process_key, Map.put(contexts(), repo, context))
    :ok
  end

  defp restore_context(repo, nil), do: clear(repo)
  defp restore_context(repo, context), do: put_context(repo, context)
end
