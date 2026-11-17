defmodule Mix.Tasks.Bedrock.ReplayTest do
  # Not async: the task writes through the process-wide `Mix.shell/0`.
  use ExUnit.Case, async: false

  alias Bedrock.DataPlane.Transaction
  alias Bedrock.ObjectStorage
  alias Bedrock.ObjectStorage.Chunk
  alias Bedrock.ObjectStorage.Keys
  alias Bedrock.ObjectStorage.LocalFilesystem
  alias Bedrock.SystemKeys
  alias Bedrock.SystemKeys.Values
  alias Mix.Tasks.Bedrock.Replay

  @end_of_keyspace <<0xFF, 0xFF>>
  @default_layout %{<<0xFF>> => {1, <<>>}, @end_of_keyspace => {0, <<0xFF>>}}

  setup do
    shell = Mix.shell()
    Mix.shell(Mix.Shell.Process)
    on_exit(fn -> Mix.shell(shell) end)

    root = Path.join(System.tmp_dir!(), "replay_task_test_#{:erlang.unique_integer([:positive])}")
    File.mkdir_p!(root)
    on_exit(fn -> File.rm_rf!(root) end)

    {:ok, backend: ObjectStorage.backend(LocalFilesystem, root: root), root: root}
  end

  defp put_slices(backend, shard_tag, versioned_mutations) do
    entries =
      Enum.map(versioned_mutations, fn {version, muts} ->
        {version, Transaction.encode(%{mutations: muts, commit_version: <<version::unsigned-big-64>>})}
      end)

    {:ok, binary} = Chunk.encode(entries)
    {version, _} = List.last(entries)
    :ok = ObjectStorage.put(backend, Keys.chunk_path(shard_tag, version), binary)
  end

  defp put_layout(backend) do
    sets =
      Enum.map(@default_layout, fn {end_key, {tag, start_key}} ->
        {:set, SystemKeys.shard_key(end_key), Values.encode_shard_key_entry(tag, start_key)}
      end)

    put_slices(backend, "0", [{10, sets}])
  end

  defp output do
    assert_receive {:mix_shell, :info, [output]}
    output
  end

  describe "run/1" do
    test "summarizes the reconstructed keyspace", %{backend: backend, root: root} do
      put_layout(backend)
      put_slices(backend, "1", [{20, [{:set, "a", "1"}]}, {30, [{:set, "b", "2"}]}])

      Replay.run(["--path", root])
      out = output()

      assert out =~ "layout: 2 shard(s) recovered from the system shard"
      assert out =~ "shard 1: 1 chunk(s), 2 txn replayed, through version 30"
      assert out =~ "4 key(s) across 2 shard(s); frontier 30, anchor 10"
    end

    test "lists the keys under --verbose, capped by --limit", %{backend: backend, root: root} do
      put_layout(backend)
      put_slices(backend, "1", [{20, [{:set, "a", "1"}, {:set, "b", "2"}]}])

      Replay.run(["--path", root, "--shard", "1", "--verbose", "--limit", "1"])
      out = output()

      assert out =~ ~s(  "a" = "1"  @20 from shard 1)
      refute out =~ ~s("b" = "2")
    end

    test "notes a layout shard that has flushed nothing", %{backend: backend, root: root} do
      put_layout(backend)

      Replay.run(["--path", root])

      assert output() =~ ~s|note: shard(s) the layout names with no chunks at all: "1"|
    end

    test "emits json", %{backend: backend, root: root} do
      put_layout(backend)
      put_slices(backend, "1", [{20, [{:set, "a", "1"}]}])

      Replay.run(["--path", root, "--format", "json", "--verbose"])
      decoded = Jason.decode!(output())

      assert decoded["frontier"] == 20
      assert decoded["anchor"] == 10
      assert %{"key" => "a", "value" => "1", "version" => 20, "shard_tag" => "1"} in decoded["keys"]
    end

    test "prints the moduledoc for --help" do
      Replay.run(["--help"])

      assert output() =~ "Replays the transaction chunks"
    end

    test "refuses to reconstruct a store with no recoverable layout", %{root: root} do
      assert_raise Mix.Error, ~r/the shard layout could not be recovered/, fn -> Replay.run(["--path", root]) end
    end

    test "requires a path that exists" do
      assert_raise Mix.Error, ~r/--path is required/, fn -> Replay.run([]) end
      assert_raise Mix.Error, ~r/not a directory/, fn -> Replay.run(["--path", "/nope/nowhere"]) end
    end

    test "rejects an unknown format", %{backend: backend, root: root} do
      put_layout(backend)

      assert_raise Mix.Error, ~r/unknown format: yaml/, fn -> Replay.run(["--path", root, "--format", "yaml"]) end
    end
  end
end
