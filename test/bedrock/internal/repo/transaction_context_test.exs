defmodule Bedrock.Internal.Repo.TransactionContextTest do
  use ExUnit.Case, async: true

  alias Bedrock.Internal.Repo.TransactionContext

  defmodule RepoA do
    @moduledoc false
  end

  defmodule RepoB do
    @moduledoc false
  end

  test "stores every Repo context under one process key" do
    TransactionContext.put_builder(RepoA, self())
    TransactionContext.put_builder(RepoB, self())

    assert %{
             RepoA => %TransactionContext{builder: repo_a_builder},
             RepoB => %TransactionContext{builder: repo_b_builder}
           } = Process.get(TransactionContext)

    assert repo_a_builder == self()
    assert repo_b_builder == self()

    TransactionContext.clear(RepoA)

    assert %{RepoB => %TransactionContext{}} = Process.get(TransactionContext)
    refute Map.has_key?(Process.get(TransactionContext), RepoA)
  end

  test "adds a deadline to the existing builder context and restores it afterward" do
    TransactionContext.put_builder(RepoA, self())

    assert :infinity = TransactionContext.remaining_timeout!(RepoA, :timeout)

    TransactionContext.with_deadline(RepoA, 1_000, fn ->
      assert TransactionContext.builder(RepoA) == self()
      assert TransactionContext.remaining_timeout!(RepoA, :timeout) in 1..1_000
    end)

    assert TransactionContext.builder(RepoA) == self()
    assert :infinity = TransactionContext.remaining_timeout!(RepoA, :timeout)
  end

  test "nested work inherits rather than replaces the outer deadline" do
    TransactionContext.with_deadline(RepoA, 1_000, fn ->
      outer_remaining = TransactionContext.remaining_timeout!(RepoA, :timeout)

      TransactionContext.with_deadline(RepoA, 10_000, fn ->
        assert TransactionContext.remaining_timeout!(RepoA, :timeout) <= outer_remaining
      end)
    end)

    assert :infinity = TransactionContext.remaining_timeout!(RepoA, :timeout)
    assert Process.get(TransactionContext) == nil
  end
end
