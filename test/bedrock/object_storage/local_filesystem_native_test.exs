defmodule Bedrock.ObjectStorage.LocalFilesystemNativeTest do
  use ExUnit.Case, async: false

  alias Bedrock.ObjectStorage
  alias Bedrock.ObjectStorage.LocalFilesystem
  alias Bedrock.ObjectStorage.LocalFilesystem.Native
  alias Bedrock.ObjectStorage.LocalFilesystem.NativeTest
  alias Bedrock.Test.LocalFilesystemNative, as: Harness

  setup_all do
    {:ok, native: Harness.build()}
  end

  setup do
    root = Path.join(System.tmp_dir!(), "native-cas-#{System.pid()}-#{System.unique_integer([:positive])}")
    File.mkdir_p!(root)
    on_exit(fn -> File.rm_rf!(root) end)
    backend = ObjectStorage.backend(LocalFilesystem, root: root)
    {:ok, root: root, backend: backend}
  end

  defp scratch(root, value) do
    name = ".bedrock-tmp.test-#{System.unique_integer([:positive])}"
    {:ok, file} = :file.open(Path.join(root, name), [:write, :raw, :binary, :exclusive])
    :ok = :file.write(file, value)
    :ok = :file.sync(file)
    :ok = :file.close(file)
    name
  end

  test "native CAS rechecks a prepared replacement against the current object", %{root: root, backend: backend} do
    :ok = ObjectStorage.put(backend, "key", "original")
    prepared = scratch(root, "replacement")
    :ok = ObjectStorage.put(backend, "key", "intervening")
    assert {:error, :version_mismatch} = Native.mutate(:cas, root, "key", prepared, "original")
    assert {:ok, "intervening"} = ObjectStorage.get(backend, "key")
    assert :ok = Native.mutate(:cas, root, "key", prepared, "intervening")
    assert {:ok, "replacement"} = ObjectStorage.get(backend, "key")
  end

  test "every mutator waits for the same actual lock and publishes only after release", %{root: root, backend: backend} do
    for operation <- [:put, :create, :cas, :delete] do
      :ok = ObjectStorage.put(backend, "key", "original")
      hooks = Path.join(root, "#{operation}-holder")
      Harness.gate(hooks, "acquired")
      holder = Task.async(fn -> NativeTest.mutate(:put, root, "other", scratch(root, "holder"), "", hooks) end)
      Harness.wait_file(Path.join(hooks, "acquired.ready"))
      waiting = Path.join(root, "#{operation}-waiter")
      Harness.gate(waiting, "contended")

      contender =
        Task.async(fn -> NativeTest.mutate(operation, root, "key", scratch(root, "contender"), "original", waiting) end)

      Harness.wait_file(Path.join(waiting, "contended.ready"))
      assert {:ok, "original"} = ObjectStorage.get(backend, "key")
      Harness.release(waiting, "contended")
      Harness.release(hooks, "acquired")
      assert :ok = Task.await(holder)
      expected = if operation == :create, do: {:error, :eexist}, else: :ok
      assert Task.await(contender) == expected

      expected_data =
        case operation do
          :delete -> {:error, :not_found}
          :create -> {:ok, "original"}
          _ -> {:ok, "contender"}
        end

      assert ObjectStorage.get(backend, "key") == expected_data
    end
  end

  test "delete followed by recreation invalidates an already prepared CAS", %{root: root, backend: backend} do
    :ok = ObjectStorage.put(backend, "key", "original")
    prepared = scratch(root, "stale")
    :ok = ObjectStorage.delete(backend, "key")
    assert {:error, :enoent} = Native.mutate(:cas, root, "key", prepared, "original")
    :ok = ObjectStorage.put_if_not_exists(backend, "key", "recreated")
    assert {:error, :version_mismatch} = Native.mutate(:cas, root, "key", prepared, "original")
    assert {:ok, "recreated"} = ObjectStorage.get(backend, "key")
  end

  test "lock and publication failures release descriptors and allow retries", %{root: root, backend: backend} do
    :ok = ObjectStorage.put(backend, "key", "original")
    lock = Path.join(root, ".bedrock-lock")
    File.rm!(lock)
    File.ln_s!("key", lock)
    assert {:error, :eloop} = ObjectStorage.put(backend, "key", "bad")
    File.rm!(lock)

    before_fds = length(File.ls!("/dev/fd"))

    for _ <- 1..100 do
      assert {:error, :enoent} = Native.mutate(:put, root, "key", ".bedrock-tmp.absent", "")
      assert {:error, :einval} = Native.mutate(:put, root, ".bedrock-lock", ".bedrock-tmp.absent", "")
    end

    assert length(File.ls!("/dev/fd")) <= before_fds + 1
    assert :ok = ObjectStorage.put(backend, "key", "retry")
    assert {:ok, "retry"} = ObjectStorage.get(backend, "key")
    assert Enum.to_list(ObjectStorage.list(backend, "")) == ["key"]
  end

  test "killing a caller does not cancel its blocked native operation or strand the lock", %{
    root: root,
    backend: backend
  } do
    :ok = ObjectStorage.put(backend, "key", "original")
    hooks = Path.join(root, "killed-owner")
    Harness.gate(hooks, "acquired")
    prepared = scratch(root, "possibly-published")
    caller = spawn(fn -> NativeTest.mutate(:cas, root, "key", prepared, "original", hooks) end)
    monitor = Process.monitor(caller)
    Harness.wait_file(Path.join(hooks, "acquired.ready"))
    Process.exit(caller, :kill)
    assert {:ok, "original"} = ObjectStorage.get(backend, "key")
    Harness.release(hooks, "acquired")
    assert_receive {:DOWN, ^monitor, :process, ^caller, :killed}, 5_000
    assert :ok = ObjectStorage.put(backend, "completion-fence", "fence")
    # A killed dirty caller has an unknown outcome; observe it before retrying.
    assert {:ok, value, token} = ObjectStorage.get_with_version(backend, "key")
    assert value in ["original", "possibly-published"]
    assert :ok = ObjectStorage.put_if_version_matches(backend, "key", token, "retry")
    assert {:ok, "retry"} = ObjectStorage.get(backend, "key")
  end

  test "directory aliases and case aliases use the same physical directory lock", %{root: root, backend: backend} do
    alias_root = root <> "-alias"
    File.ln_s!(root, alias_root)
    on_exit(fn -> File.rm(alias_root) end)
    :ok = ObjectStorage.put(backend, "key", "original")
    case_alias? = File.read(Path.join(root, "KEY")) == {:ok, "original"}

    if !case_alias?,
      do: IO.puts("Case-alias check inapplicable on this case-sensitive volume; directory-alias check still runs")

    key = if case_alias?, do: "KEY", else: "key"
    hooks = Path.join(root, "alias-owner")
    Harness.gate(hooks, "acquired")
    holder = Task.async(fn -> NativeTest.mutate(:put, root, "key", scratch(root, "changed"), "", hooks) end)
    Harness.wait_file(Path.join(hooks, "acquired.ready"))
    waiting = Path.join(root, "alias-waiter")
    Harness.gate(waiting, "contended")

    follower =
      Task.async(fn -> NativeTest.mutate(:cas, alias_root, key, scratch(root, "stale"), "original", waiting) end)

    Harness.wait_file(Path.join(waiting, "contended.ready"))
    Harness.release(waiting, "contended")
    Harness.release(hooks, "acquired")
    assert :ok = Task.await(holder)
    assert {:error, :version_mismatch} = Task.await(follower)
    assert {:ok, "changed"} = ObjectStorage.get(backend, "key")
  end

  test "whole VM death releases an acquired lock without publishing a short object", %{
    root: root,
    backend: backend,
    native: native
  } do
    :ok = ObjectStorage.put(backend, "key", "original")
    hooks = Path.join(root, "dead-vm")
    Harness.gate(hooks, "publish")
    prepared = scratch(root, "replacement")
    {:ok, peer, _} = :peer.start_link(%{connection: :standard_io, args: [~c"+S", ~c"2"]})
    Process.unlink(peer)
    on_exit(fn -> Harness.stop_peer(peer) end)
    :ok = :peer.call(peer, :code, :add_paths, [:code.get_path()])
    :ok = :peer.call(peer, NativeTest, :load, [Path.rootname(native)])
    os_pid = :peer.call(peer, System, :pid, [])
    :peer.call(peer, Harness, :start_native, [:cas, root, "key", prepared, "original", hooks])
    Harness.wait_file(Path.join(hooks, "publish.ready"))
    assert {:ok, "original"} = ObjectStorage.get(backend, "key")
    assert Regex.match?(~r/\A[0-9]+\z/, os_pid)
    {_, 0} = System.cmd("/bin/sh", ["-c", ~s(kill -KILL "$1"), "bedrock-native-kill", os_pid])
    assert :ok = ObjectStorage.put(backend, "completion-fence", "fence")
    assert {:ok, "original", token} = ObjectStorage.get_with_version(backend, "key")
    assert :ok = ObjectStorage.put_if_version_matches(backend, "key", token, "retry")
    assert {:ok, "retry"} = ObjectStorage.get(backend, "key")
  end

  test "killing a contended caller cannot steal the holder lock and later operations progress", %{
    root: root,
    backend: backend
  } do
    :ok = ObjectStorage.put(backend, "key", "original")
    hooks = Path.join(root, "live-holder")
    Harness.gate(hooks, "acquired")
    holder = Task.async(fn -> NativeTest.mutate(:put, root, "other", scratch(root, "holder"), "", hooks) end)
    Harness.wait_file(Path.join(hooks, "acquired.ready"))
    waiting = Path.join(root, "dead-waiter")
    Harness.gate(waiting, "contended")
    prepared = scratch(root, "unknown")
    caller = spawn(fn -> NativeTest.mutate(:cas, root, "key", prepared, "original", waiting) end)
    monitor = Process.monitor(caller)
    Harness.wait_file(Path.join(waiting, "contended.ready"))
    Process.exit(caller, :kill)
    Harness.release(waiting, "contended")
    assert {:ok, "original"} = ObjectStorage.get(backend, "key")
    Harness.release(hooks, "acquired")
    assert :ok = Task.await(holder)
    assert_receive {:DOWN, ^monitor, :process, ^caller, :killed}, 5_000
    # A live unconditional write is safe regardless of ordering with the killed
    # contender: either it follows that CAS, or causes its locked recheck to fail.
    assert :ok = ObjectStorage.put(backend, "key", "settled")
    assert {:ok, "settled"} = ObjectStorage.get(backend, "key")
  end

  test "CAS keeps exclusion between comparison and publication against an unconditional put", %{
    root: root,
    backend: backend
  } do
    :ok = ObjectStorage.put(backend, "key", "original")
    hooks = Path.join(root, "compared-cas")
    Harness.gate(hooks, "publish")
    cas = Task.async(fn -> NativeTest.mutate(:cas, root, "key", scratch(root, "conditional"), "original", hooks) end)
    Harness.wait_file(Path.join(hooks, "publish.ready"))
    waiting = Path.join(root, "following-put")
    Harness.gate(waiting, "contended")
    put = Task.async(fn -> NativeTest.mutate(:put, root, "key", scratch(root, "unconditional"), "", waiting) end)
    Harness.wait_file(Path.join(waiting, "contended.ready"))
    assert {:ok, "original"} = ObjectStorage.get(backend, "key")
    Harness.release(waiting, "contended")
    Harness.release(hooks, "publish")
    assert :ok = Task.await(cas)
    assert :ok = Task.await(put)
    # The barrier fixes the serial order: CAS publishes, then put publishes.
    assert {:ok, "unconditional"} = ObjectStorage.get(backend, "key")
  end
end
