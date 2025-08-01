defmodule Bedrock.Test.GenServerTestHelpers do
  @moduledoc """
  Helper macros for testing GenServer call and cast messages.

  These macros provide a clean way to assert on the exact format and content
  of GenServer messages, ensuring complete validation of inter-process communication.
  """

  @doc """
  Assert that a call message was received with pattern matching and optional additional assertions.

  ## Examples

      # Simple exact match
      assert_call_received({:recover_from, :source_log, 100, 200})
      
      # Pattern matching with assertions
      assert_call_received({:recover_from, source, first, last}) do
        assert source == :expected_source
        assert first == 100
        assert last == 200
      end
      
      # With custom timeout
      assert_call_received({:some_call, data}, 500) do
        assert is_binary(data)
      end
  """
  defmacro assert_call_received(pattern) do
    quote do
      assert_receive {:"$gen_call", _from, call_message}, 100
      assert unquote(pattern) = call_message
      call_message
    end
  end

  defmacro assert_call_received(pattern, timeout) when is_integer(timeout) do
    quote do
      assert_receive {:"$gen_call", _from, call_message}, unquote(timeout)
      assert unquote(pattern) = call_message
      call_message
    end
  end

  defmacro assert_call_received(pattern, do: assertions) do
    quote do
      assert_receive {:"$gen_call", _from, call_message}, 100
      assert unquote(pattern) = call_message
      unquote(assertions)
      call_message
    end
  end

  defmacro assert_call_received(pattern, timeout, do: assertions) do
    quote do
      assert_receive {:"$gen_call", _from, call_message}, unquote(timeout)
      assert unquote(pattern) = call_message
      unquote(assertions)
      call_message
    end
  end

  @doc """
  Assert that a cast message was received with pattern matching and optional additional assertions.

  ## Examples

      # Simple exact match
      assert_cast_received({:worker_health, "worker_1", {:ok, pid}})
      
      # Pattern matching with assertions
      assert_cast_received({:worker_health, worker_id, health_status}) do
        assert worker_id == "test_storage_1"
        assert {:ok, pid} = health_status
        assert is_pid(pid)
      end
      
      # With custom timeout
      assert_cast_received({:notification, data}, 500) do
        assert is_map(data)
      end
  """
  defmacro assert_cast_received(pattern) do
    quote do
      assert_receive {:"$gen_cast", cast_message}, 100
      assert unquote(pattern) = cast_message
      cast_message
    end
  end

  defmacro assert_cast_received(pattern, timeout) when is_integer(timeout) do
    quote do
      assert_receive {:"$gen_cast", cast_message}, unquote(timeout)
      assert unquote(pattern) = cast_message
      cast_message
    end
  end

  defmacro assert_cast_received(pattern, do: assertions) do
    quote do
      assert_receive {:"$gen_cast", cast_message}, 100
      assert unquote(pattern) = cast_message
      unquote(assertions)
      cast_message
    end
  end

  defmacro assert_cast_received(pattern, timeout, do: assertions) do
    quote do
      assert_receive {:"$gen_cast", cast_message}, unquote(timeout)
      assert unquote(pattern) = cast_message
      unquote(assertions)
      cast_message
    end
  end
end
