defmodule Bedrock.ObjectStorage.S3CoherentVersionTest do
  use ExUnit.Case, async: true

  alias Bedrock.ObjectStorage.S3

  defmodule ChangingObjectHTTP do
    @moduledoc false
    def request(:get, _url, _body, _headers, _opts),
      do: {:ok, %{status_code: 200, body: "old committed logs", headers: []}}

    def request(:head, _url, _body, _headers, _opts),
      do: {:ok, %{status_code: 200, body: "", headers: [{"ETag", "newer-object-token"}]}}
  end

  defmodule MixedCaseHTTP do
    @moduledoc false
    def request(:get, _url, _body, _headers, _opts),
      do: {:ok, %{status_code: 200, body: "coherent", headers: [{"eTaG", "same-get-token"}]}}
  end

  defmodule EmptyETagHTTP do
    @moduledoc false
    def request(:get, _url, _body, _headers, _opts),
      do: {:ok, %{status_code: 200, body: "coherent", headers: [{"ETag", ""}]}}
  end

  test "mixed-case GET ETag remains paired with its exact body" do
    assert {:ok, "coherent", "same-get-token"} = S3.get_with_version(config(MixedCaseHTTP), "bootstrap")
  end

  test "empty GET ETag fails closed" do
    assert {:error, :missing_version_token} = S3.get_with_version(config(EmptyETagHTTP), "bootstrap")
  end

  defp config(http) do
    [
      bucket: "coherent-bootstrap",
      config: [
        http_client: http,
        access_key_id: "test",
        secret_access_key: "test",
        region: "us-east-1",
        retries: [max_attempts: 1]
      ]
    ]
  end

  test "GET missing ETag cannot borrow a later HEAD token for stale bytes" do
    config = [
      bucket: "coherent-bootstrap",
      config: [
        http_client: ChangingObjectHTTP,
        access_key_id: "test",
        secret_access_key: "test",
        region: "us-east-1",
        retries: [max_attempts: 1]
      ]
    ]

    assert {:error, :missing_version_token} = S3.get_with_version(config, "bootstrap")
  end
end
