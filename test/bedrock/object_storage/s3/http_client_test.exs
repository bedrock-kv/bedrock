defmodule Bedrock.ObjectStorage.S3.HttpClientTest do
  use ExUnit.Case, async: true

  alias Bedrock.ObjectStorage.S3.HttpClient

  describe "normalize_response/1" do
    test "a full response keeps its body" do
      assert {:ok, %{status_code: 200, headers: [{"ETag", ~s("abc")}], body: "payload"}} =
               HttpClient.normalize_response({:ok, 200, [{"ETag", ~s("abc")}], "payload"})
    end

    test "a bodiless HEAD response normalizes with an empty body" do
      # Hackney returns HEAD responses as three-element tuples; the
      # adapter bundled with ex_aws crashes on this shape.
      assert {:ok, %{status_code: 200, headers: [{"ETag", ~s("abc")}], body: ""}} =
               HttpClient.normalize_response({:ok, 200, [{"ETag", ~s("abc")}]})
    end

    test "errors are wrapped for ExAws" do
      assert {:error, %{reason: :timeout}} = HttpClient.normalize_response({:error, :timeout})
    end
  end
end
