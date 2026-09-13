defmodule Bedrock.ObjectStorage.LocalFilesystemPeerTest do
  use ExUnit.Case, async: false

  alias Bedrock.ObjectStorage
  alias Bedrock.ObjectStorage.LocalFilesystem
  alias Bedrock.Test.LocalFilesystemNative, as: Harness

  @moduletag timeout: 60_000

  setup do
    root = Path.join(System.tmp_dir!(), "peer-cas-#{System.pid()}-#{System.unique_integer([:positive])}")
    File.mkdir_p!(root)
    on_exit(fn -> File.rm_rf!(root) end)

    peers =
      for index <- 1..2 do
        name = String.to_atom("cas_#{System.pid()}_#{System.unique_integer([:positive])}_#{index}")
        {:ok, peer, node} = :peer.start_link(%{name: name, connection: :standard_io, args: [~c"+S", ~c"2"]})
        Process.unlink(peer)
        :ok = :peer.call(peer, :code, :add_paths, [:code.get_path()])
        on_exit(fn -> Harness.stop_peer(peer) end)
        {peer, node}
      end

    {:ok, root: root, peers: peers, backend: ObjectStorage.backend(LocalFilesystem, root: root)}
  end

  for connected <- [true, false] do
    test "public CAS has one winner across #{if connected, do: "connected", else: "independent"} BEAMs", %{
      root: root,
      peers: peers,
      backend: backend
    } do
      [{first, _}, {second, second_node}] = peers

      if unquote(connected) do
        assert :peer.call(first, Node, :connect, [second_node])
        assert second_node in :peer.call(first, Node, :list, [])
      else
        assert :peer.call(first, Node, :list, []) == []
        assert :peer.call(second, Node, :list, []) == []
      end

      for round <- 1..3 do
        assert :ok = ObjectStorage.put(backend, "key", "original-#{round}")
        {:ok, _, token} = ObjectStorage.get_with_version(backend, "key")

        gates =
          for {{peer, _}, index} <- Enum.with_index(peers), id <- 1..4 do
            writer_id = index * 4 + id
            gate = Path.join(root, "round-#{round}-#{writer_id}")
            :peer.call(peer, Harness, :start_api, [root, "key", token, writer_id, gate])
            gate
          end

        Enum.each(gates, &Harness.wait_file(&1 <> ".ready"))
        Enum.each(gates, &File.write!(&1 <> ".go", "go"))
        Enum.each(gates, &Harness.wait_file(&1 <> ".result"))
        results = Enum.map(gates, &((&1 <> ".result") |> File.read!() |> :erlang.binary_to_term()))
        assert [{winner, :ok}] = Enum.filter(results, &(elem(&1, 1) == :ok))
        assert Enum.count(results, &(elem(&1, 1) == {:error, :version_mismatch})) == 7
        assert {:ok, :binary.copy(<<winner>>, 1024 * 1024)} == ObjectStorage.get(backend, "key")
        assert {:error, :version_mismatch} = ObjectStorage.put_if_version_matches(backend, "key", token, "stale")
        {:ok, _, fresh} = ObjectStorage.get_with_version(backend, "key")
        assert :ok = ObjectStorage.put_if_version_matches(backend, "key", fresh, "retry")
      end
    end
  end
end
