alias Bedrock.ObjectStorage
alias Bedrock.ObjectStorage.LocalFilesystem
root = Path.join(System.tmp_dir!(), "bedrock-package-smoke-#{System.pid()}")
backend = ObjectStorage.backend(LocalFilesystem, root: root)
try do
  :ok = ObjectStorage.put_if_not_exists(backend, "key", "initial")
  {:error, :already_exists} = ObjectStorage.put_if_not_exists(backend, "key", "other")
  {:ok, "initial", token} = ObjectStorage.get_with_version(backend, "key")
  :ok = ObjectStorage.put_if_version_matches(backend, "key", token, "conditional")
  {:error, :version_mismatch} = ObjectStorage.put_if_version_matches(backend, "key", token, "stale")
  :ok = ObjectStorage.put(backend, "key", "unconditional")
  {:ok, "unconditional"} = ObjectStorage.get(backend, "key")
  ["key"] = Enum.to_list(ObjectStorage.list(backend, ""))
  :ok = ObjectStorage.delete(backend, "key")
  :ok = ObjectStorage.delete(backend, "key")
  {:error, :not_found} = ObjectStorage.get(backend, "key")
  IO.puts("LocalFilesystem package smoke passed on #{inspect(:os.type())} #{:erlang.system_info(:system_architecture)}")
after
  File.rm_rf!(root)
end
