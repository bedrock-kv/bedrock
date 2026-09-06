defmodule Bedrock.DataPlane.Log.Shale.DurabilityContractTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Log.Shale.Pushing
  alias Bedrock.DataPlane.Log.Shale.Segment
  alias Bedrock.DataPlane.Log.Shale.State
  alias Bedrock.DataPlane.Log.Shale.Writer
  alias Bedrock.DataPlane.Version
  alias Bedrock.Test.DataPlane.TransactionTestSupport

  @authority %{generation: 1, recovery_id: "transition-test"}

  setup do
    path = Path.join(System.tmp_dir!(), "shale_durability_contract_#{System.unique_integer([:positive])}.log")
    File.write!(path, :binary.copy(<<0>>, 1024))

    on_exit(fn ->
      File.rm(path)
    end)

    {:ok, path: path}
  end

  test "append succeeds only after wal sync", %{path: path} do
    caller = self()

    sync_fun = fn _fd ->
      send(caller, :sync_called)
      :ok
    end

    assert {:ok, writer} = Writer.open(path, Version.zero(), sync_fun: sync_fun)
    transaction = TransactionTestSupport.new_log_transaction(0, %{"k" => "v"})

    assert {:ok, updated_writer} = Writer.append(writer, transaction, Version.from_integer(0))
    assert_receive :sync_called
    assert updated_writer.write_offset > writer.write_offset
    assert :ok = Writer.close(writer)
  end

  test "append returns error and keeps offsets when wal sync fails", %{path: path} do
    assert {:ok, writer} = Writer.open(path, Version.zero(), sync_fun: fn _fd -> {:error, :eio} end)

    transaction = TransactionTestSupport.new_log_transaction(0, %{"k" => "v"})

    assert {:error, :eio} = Writer.append(writer, transaction, Version.from_integer(0))
    assert writer.write_offset == 12
    assert writer.bytes_remaining == 996

    assert :ok = Writer.close(writer)
  end

  test "push does not emit false success ack when wal sync fails", %{path: path} do
    assert {:ok, writer} = Writer.open(path, Version.zero(), sync_fun: fn _fd -> {:error, :eio} end)

    state = %State{
      recovery_authority: @authority,
      mode: :ready,
      last_version: Version.from_integer(0),
      pending_pushes: %{},
      writer: writer,
      active_segment: %Segment{path: path, min_version: Version.zero(), transactions: []}
    }

    transaction = TransactionTestSupport.new_log_transaction(0, %{"k" => "v"})
    # The transition carries no append event and exactly one reply — the
    # error — so no false success acknowledgement can exist.
    assert %{state: ^state, appended: [], replies: [{:tok, {:error, :eio}}], parked?: false} =
             Pushing.push(state, Version.from_integer(0), transaction, :tok)

    assert :ok = Writer.close(writer)
  end
end
