defmodule Bedrock.Test.Minio do
  @moduledoc false

  def start_link do
    cur = Process.flag(:trap_exit, true)

    System.put_env("MINIO_ROOT_USER", "minio_key")
    System.put_env("MINIO_ROOT_PASSWORD", "minio_secret")
    System.put_env("MINIO_BROWSER", "off")

    try do
      {:ok, _pid} = MinioServer.start_link(config())
    rescue
      exception in [MatchError] ->
        case exception.term do
          {:error, {:shutdown, {:failed_to_start_child, MuonTrap.Daemon, {:enoent, _}}}} ->
            reraise """
                    Minio binaries not available.

                    Run:
                    MIX_ENV=test mix minio_server.download --arch darwin-arm64 --version latest
                    """,
                    __STACKTRACE__

          _ ->
            reraise exception, __STACKTRACE__
        end
    after
      Process.flag(:trap_exit, cur)
    end
  end

  def config do
    [
      access_key_id: "minio_key",
      secret_access_key: "minio_secret",
      scheme: "http://",
      region: "local",
      host: "127.0.0.1",
      port: 9000,
      console_address: ":9001"
    ]
  end

  def wait_for_ready(timeout \\ 5_000) do
    deadline = System.monotonic_time(:millisecond) + timeout
    wait_for_ready_loop(deadline)
  end

  def initialize_bucket(name) do
    name
    |> ExAws.S3.put_bucket(Keyword.fetch!(config(), :region))
    |> ExAws.request(config())
    |> case do
      {:ok, _} -> :ok
      {:error, {:http_error, 409, _}} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  def recreate_bucket(name) do
    name
    |> ExAws.S3.delete_bucket()
    |> ExAws.request(config())

    initialize_bucket(name)
  end

  def clean_bucket(name) do
    with {:ok, keys} <- list_object_keys(name) do
      Enum.each(keys, fn key ->
        name
        |> ExAws.S3.delete_object(key)
        |> ExAws.request(config())
      end)

      :ok
    end
  end

  defp wait_for_ready_loop(deadline) do
    case :gen_tcp.connect(~c"127.0.0.1", 9000, [:binary, {:active, false}], 100) do
      {:ok, socket} ->
        :ok = :gen_tcp.close(socket)
        :ok

      {:error, _reason} ->
        if System.monotonic_time(:millisecond) < deadline do
          Process.sleep(100)
          wait_for_ready_loop(deadline)
        else
          {:error, :timeout}
        end
    end
  end

  defp list_object_keys(name, continuation_token \\ nil, keys \\ []) do
    opts =
      if continuation_token do
        [continuation_token: continuation_token]
      else
        []
      end

    case name |> ExAws.S3.list_objects_v2(opts) |> ExAws.request(config()) do
      {:ok, %{body: body}} ->
        next_keys =
          keys ++
            (body
             |> Map.get(:contents, [])
             |> Enum.map(&Map.get(&1, :key)))

        if truncated?(body) do
          list_object_keys(name, Map.get(body, :next_continuation_token), next_keys)
        else
          {:ok, next_keys}
        end

      error ->
        error
    end
  end

  defp truncated?(body) do
    case Map.get(body, :is_truncated, false) do
      true -> true
      "true" -> true
      _ -> false
    end
  end
end
