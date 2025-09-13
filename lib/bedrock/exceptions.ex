defmodule Bedrock.StorageError do
  @moduledoc """
  Exception raised when a storage operation fails with a retryable error.

  These errors indicate temporary storage unavailability or timeouts that
  should trigger transaction retry at the TransactionManager level.

  Common reasons:
  - `:unavailable` - No storage servers available for the key range
  - `:timeout` - Storage operation timed out

  These are exceptional conditions that interrupt normal transaction flow
  but can potentially succeed on retry.
  """
  defexception [:reason, :operation, :key, :message]

  @impl true
  def exception(opts) do
    reason = Keyword.fetch!(opts, :reason)
    operation = Keyword.get(opts, :operation)
    key = Keyword.get(opts, :key)

    message = build_message(reason, operation, key)

    %__MODULE__{
      reason: reason,
      operation: operation,
      key: key,
      message: message
    }
  end

  defp build_message(reason, operation, key) do
    base = "Storage error: #{inspect(reason)}"

    with_op = if operation, do: "#{base} during #{operation}", else: base

    if key do
      "#{with_op} for key #{inspect(key)}"
    else
      with_op
    end
  end
end

defmodule Bedrock.TransactionError do
  @moduledoc """
  Exception raised when a transaction operation fails with a non-retryable error.

  These errors indicate programming errors or invalid operations that will
  not succeed on retry and should fail immediately.

  Common reasons:
  - Invalid key format
  - Decode errors
  - Version conflicts that indicate logic errors
  - Other unexpected error conditions

  These errors should not trigger transaction retry as they represent
  fundamental problems that need to be fixed in the calling code.
  """
  defexception [:reason, :operation, :key, :message]

  @impl true
  def exception(opts) do
    reason = Keyword.fetch!(opts, :reason)
    operation = Keyword.get(opts, :operation)
    key = Keyword.get(opts, :key)

    message = build_message(reason, operation, key)

    %__MODULE__{
      reason: reason,
      operation: operation,
      key: key,
      message: message
    }
  end

  defp build_message(reason, operation, key) do
    base = "Transaction error: #{inspect(reason)}"

    with_op = if operation, do: "#{base} during #{operation}", else: base

    if key do
      "#{with_op} for key #{inspect(key)}"
    else
      with_op
    end
  end
end
