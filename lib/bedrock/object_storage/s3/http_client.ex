if Code.ensure_loaded?(:hackney) do
  defmodule Bedrock.ObjectStorage.S3.HttpClient do
    @moduledoc """
    Hackney-backed HTTP client for ExAws that handles HEAD responses.

    Hackney returns HEAD responses as `{:ok, status, headers}` — there is
    no body element — but the adapter bundled with ex_aws only matches the
    four-element shape, so every HEAD request (used for ETag reads and
    conditional-write resolution) crashes. This client is that adapter
    plus the missing clause.
    """
    @behaviour ExAws.Request.HttpClient

    @default_opts [recv_timeout: 30_000]

    @impl true
    def request(method, url, body \\ "", headers \\ [], http_opts \\ []) do
      opts = Application.get_env(:ex_aws, :hackney_opts, @default_opts)

      method
      |> :hackney.request(url, headers, body, http_opts ++ opts)
      |> normalize_response()
    end

    @doc """
    Translates a raw hackney response into ExAws's expected shape. A
    three-element success is a bodiless response (HEAD).
    """
    @spec normalize_response(
            {:ok, non_neg_integer(), list(), binary()}
            | {:ok, non_neg_integer(), list()}
            | {:error, term()}
          ) :: {:ok, %{status_code: non_neg_integer(), headers: list(), body: binary()}} | {:error, %{reason: term()}}
    def normalize_response({:ok, status, headers, body}),
      do: {:ok, %{status_code: status, headers: headers, body: body}}

    def normalize_response({:ok, status, headers}), do: {:ok, %{status_code: status, headers: headers, body: ""}}
    def normalize_response({:error, reason}), do: {:error, %{reason: reason}}
  end
end
