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
end
