defmodule Bedrock.Internal.RepoErrorHandlingTest do
  use ExUnit.Case, async: true

  alias Bedrock.Internal.Repo

  describe "get/3 error handling" do
    test "returns nil for :not_found error" do
      txn =
        spawn(fn ->
          receive do
            {:"$gen_call", from, {:get, _key, _opts}} ->
              GenServer.reply(from, {:error, :not_found})
          end
        end)

      assert Repo.get(txn, "test_key") == nil
    end

    test "returns value for successful get" do
      txn =
        spawn(fn ->
          receive do
            {:"$gen_call", from, {:get, _key, _opts}} ->
              GenServer.reply(from, {:ok, "test_value"})
          end
        end)

      assert Repo.get(txn, "test_key") == "test_value"
    end

    test "throws tuple for :unavailable error" do
      txn =
        spawn(fn ->
          receive do
            {:"$gen_call", from, {:get, _key, _opts}} ->
              GenServer.reply(from, {:failure, :unavailable})
          end
        end)

      {Repo, failed_txn, :retryable_failure, :unavailable} = catch_throw(Repo.get(txn, "test_key"))
      assert failed_txn == txn
    end

    test "throws tuple for :timeout error" do
      txn =
        spawn(fn ->
          receive do
            {:"$gen_call", from, {:get, _key, _opts}} ->
              GenServer.reply(from, {:failure, :timeout})
          end
        end)

      {Repo, failed_txn, :retryable_failure, :timeout} = catch_throw(Repo.get(txn, "test_key"))
      assert failed_txn == txn
    end

    test "throws TransactionError tuple for other errors" do
      txn =
        spawn(fn ->
          receive do
            {:"$gen_call", from, {:get, _key, _opts}} ->
              GenServer.reply(from, {:error, :invalid_key})
          end
        end)

      {module, failed_txn, type, reason, operation, key} = catch_throw(Repo.get(txn, "test_key"))
      assert module == Repo
      assert failed_txn == txn
      assert type == :transaction_error
      assert reason == :invalid_key
      assert operation == :get
      assert key == "test_key"
    end

    test "tuple includes error reason" do
      txn =
        spawn(fn ->
          receive do
            {:"$gen_call", from, {:get, key, _opts}} ->
              send(self(), {:received_key, key})
              GenServer.reply(from, {:failure, :unavailable})
          end
        end)

      {module, failed_txn, error_type, reason} = catch_throw(Repo.get(txn, "specific_test_key"))

      assert module == Repo
      assert failed_txn == txn
      assert error_type == :retryable_failure
      assert reason == :unavailable
    end
  end

  describe "select/3 error handling" do
    test "returns nil for :not_found error" do
      txn =
        spawn(fn ->
          receive do
            {:"$gen_call", from, {:get_key_selector, _selector, _opts}} ->
              GenServer.reply(from, {:error, :not_found})
          end
        end)

      selector = Bedrock.KeySelector.first_greater_than("test_key")
      assert Repo.select(txn, selector) == nil
    end

    test "returns nil for not_found response" do
      txn =
        spawn(fn ->
          receive do
            {:"$gen_call", from, {:get_key_selector, _selector, _opts}} ->
              GenServer.reply(from, {:error, :not_found})
          end
        end)

      selector = Bedrock.KeySelector.first_greater_than("test_key")
      assert Repo.select(txn, selector) == nil
    end

    test "returns key-value tuple for successful select" do
      txn =
        spawn(fn ->
          receive do
            {:"$gen_call", from, {:get_key_selector, _selector, _opts}} ->
              GenServer.reply(from, {:ok, {"resolved_key", "value"}})
          end
        end)

      selector = Bedrock.KeySelector.first_greater_than("test_key")
      assert Repo.select(txn, selector) == {"resolved_key", "value"}
    end

    test "throws tuple for :unavailable error" do
      txn =
        spawn(fn ->
          receive do
            {:"$gen_call", from, {:get_key_selector, _selector, _opts}} ->
              GenServer.reply(from, {:failure, :unavailable})
          end
        end)

      selector = Bedrock.KeySelector.first_greater_than("test_key")

      {Repo, failed_txn, :retryable_failure, :unavailable} = catch_throw(Repo.select(txn, selector))
      assert failed_txn == txn
    end

    test "throws tuple for :timeout error" do
      txn =
        spawn(fn ->
          receive do
            {:"$gen_call", from, {:get_key_selector, _selector, _opts}} ->
              GenServer.reply(from, {:failure, :timeout})
          end
        end)

      selector = Bedrock.KeySelector.first_greater_than("test_key")

      {Repo, failed_txn, :retryable_failure, :timeout} = catch_throw(Repo.select(txn, selector))
      assert failed_txn == txn
    end

    test "tuple includes error reason for selectors" do
      txn =
        spawn(fn ->
          receive do
            {:"$gen_call", from, {:get_key_selector, _selector, _opts}} ->
              GenServer.reply(from, {:failure, :unavailable})
          end
        end)

      selector = Bedrock.KeySelector.first_greater_than("selector_test_key")

      {module, failed_txn, error_type, reason} = catch_throw(Repo.select(txn, selector))

      assert module == Repo
      assert failed_txn == txn
      assert error_type == :retryable_failure
      assert reason == :unavailable
    end
  end

  describe "error classification" do
    test "tuple is thrown for retryable errors" do
      retryable_errors = [:unavailable, :timeout, :version_too_new]

      for error_reason <- retryable_errors do
        txn =
          spawn(fn ->
            receive do
              {:"$gen_call", from, {:get, _key, _opts}} ->
                GenServer.reply(from, {:failure, error_reason})
            end
          end)

        {module, failed_txn, error_type, reason} = catch_throw(Repo.get(txn, "test_key"))

        assert module == Repo
        assert failed_txn == txn
        assert error_type == :retryable_failure
        assert reason == error_reason
      end
    end

    test "TransactionError tuple is thrown for non-retryable errors" do
      non_retryable_errors = [:invalid_key, :permission_denied, :version_too_old]

      for error_reason <- non_retryable_errors do
        txn =
          spawn(fn ->
            receive do
              {:"$gen_call", from, {:get, _key, _opts}} ->
                GenServer.reply(from, {:error, error_reason})
            end
          end)

        {module, failed_txn, type, reason, operation, key} = catch_throw(Repo.get(txn, "test_key"))
        assert module == Repo
        assert failed_txn == txn
        assert type == :transaction_error
        assert reason == error_reason
        assert operation == :get
        assert key == "test_key"
      end
    end
  end
end
