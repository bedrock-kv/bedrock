defmodule Bedrock.ObjectStorage.ErrorContractTest do
  use ExUnit.Case, async: true

  alias Bedrock.ObjectStorage

  defmodule RawErrorBackend do
    @moduledoc false
    @behaviour ObjectStorage

    @impl true
    def put(_config, _key, _data, _opts), do: {:error, :eacces}

    @impl true
    def get(_config, _key), do: {:error, :enoent}

    @impl true
    def delete(_config, _key), do: {:error, :eperm}

    @impl true
    def list(_config, _prefix, _opts), do: Stream.map([], & &1)

    @impl true
    def put_if_not_exists(_config, _key, _data, _opts), do: {:error, :eexist}

    @impl true
    def get_with_version(_config, _key), do: {:error, {:http_error, 404}}

    @impl true
    def put_if_version_matches(_config, _key, _version_token, _data, _opts), do: {:error, {:http_error, 412}}
  end

  test "normalizes wrapper operations to canonical reasons" do
    backend = ObjectStorage.backend(RawErrorBackend, [])

    assert {:error, :access_denied} = ObjectStorage.put(backend, "key", "value")
    assert {:error, :not_found} = ObjectStorage.get(backend, "key")
    assert {:error, :access_denied} = ObjectStorage.delete(backend, "key")
    assert {:error, :already_exists} = ObjectStorage.put_if_not_exists(backend, "key", "value")
    assert {:error, :not_found} = ObjectStorage.get_with_version(backend, "key")
    assert {:error, :version_mismatch} = ObjectStorage.put_if_version_matches(backend, "key", "token", "value")
  end

  test "normalizes known error tuples and preserves unknown reasons" do
    assert {:error, :not_found} = ObjectStorage.normalize_error({:error, :enoent})
    assert {:error, :access_denied} = ObjectStorage.normalize_error({:error, {:http_error, 403}})
    assert {:error, :version_mismatch} = ObjectStorage.normalize_error({:error, {:precondition_failed, :etag}})
    assert {:error, :custom_backend_error} = ObjectStorage.normalize_error({:error, :custom_backend_error})
  end
end
