defmodule Bedrock.DataPlane.TransactionReadConflictShapeTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Transaction
  alias Bedrock.DataPlane.Version

  test "rejects nonempty flat read conflicts instead of dropping them" do
    for extra <- [%{}, %{read_version: Version.from_integer(50)}] do
      assert_raise ArgumentError, ~r/read_conflicts.*read_version/, fn ->
        extra |> Map.put(:read_conflicts, [{"a", "b"}]) |> Transaction.encode()
      end
    end
  end

  test "canonical tuple round-trips read ranges and binary version" do
    reads = [{"a", "b"}, {"c", "c\0"}]
    version = Version.from_integer(50)
    encoded = Transaction.encode(%{read_conflicts: {version, reads}})
    assert {:ok, {^version, ^reads}} = Transaction.read_conflicts(encoded)
    assert {:ok, {{^version, ^reads}, []}} = Transaction.read_write_conflicts(encoded)
  end

  test "compatible empty forms remain no-read transactions" do
    for input <- [%{}, %{read_conflicts: nil}, %{read_conflicts: []}, %{read_conflicts: {nil, []}}] do
      assert {:ok, {nil, []}} = input |> Transaction.encode() |> Transaction.read_conflicts()
    end
  end

  test "nil read version is valid only when there are no read conflicts" do
    assert_raise ArgumentError, ~r/read_version is nil/, fn ->
      Transaction.encode(%{read_conflicts: {nil, [{"a", "b"}]}})
    end

    assert_raise ArgumentError, ~r/read_version is non-nil/, fn ->
      Transaction.encode(%{read_conflicts: {Version.zero(), []}})
    end
  end
end
