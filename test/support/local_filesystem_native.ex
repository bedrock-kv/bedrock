defmodule Bedrock.ObjectStorage.LocalFilesystem.NativeTest do
  @moduledoc false
  def load(path), do: :erlang.load_nif(String.to_charlist(path), 0)
  def mutate(_op, _dir, _name, _scratch, _expected, _hooks), do: :erlang.nif_error(:nif_not_loaded)
end

defmodule Bedrock.Test.LocalFilesystemNative do
  @moduledoc false
  alias Bedrock.ObjectStorage.LocalFilesystem.NativeTest

  def build do
    output = Path.join(System.tmp_dir!(), "bedrock-native-test-#{System.pid()}.so")
    include = Path.join([List.to_string(:code.root_dir()), "erts-#{:erlang.system_info(:version)}", "include"])
    flags = if :os.type() == {:unix, :darwin}, do: ["-dynamiclib", "-undefined", "dynamic_lookup"], else: ["-shared"]

    args =
      ["-std=c11", "-fPIC", "-Wall", "-Wextra", "-Werror", "-DBEDROCK_TEST_BARRIERS", "-I#{include}"] ++
        flags ++ ["c_src/local_filesystem_mutation.c", "-o", output]

    {text, 0} = System.cmd(System.get_env("CC", "cc"), args, stderr_to_stdout: true)
    if text != "", do: IO.puts(text)
    :ok = NativeTest.load(Path.rootname(output))
    output
  end

  def wait_file(path, remaining \\ 5_000)

  def wait_file(path, remaining) when remaining > 0 do
    if File.exists?(path) do
      :ok
    else
      Process.sleep(5)
      wait_file(path, remaining - 5)
    end
  end

  def wait_file(path, _), do: raise("timed out waiting for #{path}")

  def gate(root, stage) do
    File.mkdir_p!(root)
    path = Path.join(root, "#{stage}.release")
    File.write!(path, "held")
    # Every gate is canceled on assertion/timeout failure too. Unlink is safe
    # before entry and while waiting; it never waits for a FIFO reader.
    ExUnit.Callbacks.on_exit(fn -> File.rm(path) end)
  end

  def release(root, stage), do: File.rm!(Path.join(root, "#{stage}.release"))

  def stop_peer(peer) do
    :peer.stop(peer)
  catch
    :exit, :noproc -> :ok
    :exit, {:noproc, _} -> :ok
  end

  def start_native(op, root, name, scratch, expected, hooks) do
    spawn(fn -> NativeTest.mutate(op, root, name, scratch, expected, hooks) end)
  end

  def start_api(root, key, token, id, gate) do
    spawn(fn ->
      data = :binary.copy(<<id>>, 1024 * 1024)
      File.write!("#{gate}.ready", "ready")
      wait_file("#{gate}.go")
      backend = Bedrock.ObjectStorage.backend(Bedrock.ObjectStorage.LocalFilesystem, root: root)
      result = Bedrock.ObjectStorage.put_if_version_matches(backend, key, token, data)
      File.write!("#{gate}.result", :erlang.term_to_binary({id, result}))
    end)
  end
end
