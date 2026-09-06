defmodule Bedrock.Test.History.Oracle do
  @moduledoc "Independent sequential model and bounded strict-serializability checker for transaction attempts."
  import Bitwise

  def evaluate(initial, operations) do
    {state, observations} = Enum.reduce(operations, {initial, []}, &apply_operation/2)
    {state, Enum.reverse(observations)}
  end

  def check(initial, history, final) do
    valid =
      length(history) <= 8 and length(Enum.uniq_by(history, & &1.id)) == length(history) and
        Enum.all?(history, &(&1.status in [:committed, :aborted, :unknown]))

    if valid, do: search(initial, history, final, []), else: {:error, :invalid_history}
  end

  defp search(state, [], final, order) when state == final, do: {:ok, Enum.reverse(order)}
  defp search(_state, [], _final, _order), do: {:error, :no_serialization}

  defp search(state, remaining, final, order) do
    Enum.find_value(remaining, {:error, :no_serialization}, fn candidate ->
      eligible =
        not Enum.any?(remaining, fn other ->
          other.id != candidate.id and other.status != :unknown and other.complete < candidate.invoke
        end)

      if eligible do
        rest = Enum.reject(remaining, &(&1.id == candidate.id))

        state
        |> choices(candidate)
        |> Enum.find_value(fn {next, applied} ->
          next_order = if applied, do: [candidate.id | order], else: order

          case search(next, rest, final, next_order) do
            {:ok, _} = accepted -> accepted
            {:error, _} -> nil
          end
        end)
      end
    end)
  end

  defp choices(state, %{status: :aborted}), do: [{state, false}]

  defp choices(state, attempt) do
    {next, observations} = evaluate(state, attempt.ops)
    apply = if observations == attempt.reads, do: [{next, true}], else: []
    if attempt.status == :unknown, do: [{state, false} | apply], else: apply
  end

  defp apply_operation({:put, key, value}, {state, reads}), do: {Map.put(state, key, value), reads}
  defp apply_operation({:clear, key}, {state, reads}), do: {Map.delete(state, key), reads}

  defp apply_operation({:clear_range, first, last}, {state, reads}),
    do: {Map.reject(state, fn {key, _} -> first <= key and key < last end), reads}

  defp apply_operation({:add, key, increment}, {state, reads}) do
    value = rem(number(state[key]) + increment, 1 <<< 64)
    {Map.put(state, key, <<value::64-little>>), reads}
  end

  defp apply_operation({:get, key}, {state, reads}), do: {state, [{:get, key, state[key]} | reads]}

  defp apply_operation({:range, first, last}, {state, reads}),
    do: {state, [{:range, range(state, first, last)} | reads]}

  defp apply_operation({:reserve, {first, last}, key}, {state, reads}) do
    empty = range(state, first, last) == []
    next = if empty, do: Map.put(state, key, "reserved"), else: state
    {next, [{:reserve, empty} | reads]}
  end

  defp apply_operation({:transfer, from, to, amount}, {state, reads}) do
    source = state[from]
    balance = number(source)
    enough = balance >= amount
    destination = if enough, do: state[to], else: :unread

    next =
      if enough,
        do:
          state
          |> Map.put(from, <<balance - amount::64-little>>)
          |> Map.put(to, <<number(state[to]) + amount::64-little>>),
        else: state

    {next, [{:transfer, source, destination, enough} | reads]}
  end

  defp number(nil), do: 0
  defp number(<<value::64-little>>), do: value

  defp range(state, first, last),
    do: state |> Enum.filter(fn {key, _} -> first <= key and key < last end) |> Enum.sort()
end
