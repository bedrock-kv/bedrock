defmodule Bedrock.TestSupport.MinioTest do
  use ExUnit.Case, async: false

  alias Bedrock.Test.Minio

  test "test helper exposes minio availability in application env" do
    assert is_boolean(Application.get_env(:bedrock, :minio_available))
  end

  test "minio helper config uses expected local defaults" do
    config = Minio.config()

    assert config[:host] == "127.0.0.1"
    assert config[:port] == 9000
    assert config[:region] == "local"
    assert config[:scheme] == "http://"
  end

  @tag :s3
  test "minio endpoint is reachable for s3-tagged tests" do
    assert :ok = Minio.wait_for_ready(2_000)
  end
end
