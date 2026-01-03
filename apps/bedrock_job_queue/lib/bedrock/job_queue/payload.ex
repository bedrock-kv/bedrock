defmodule Bedrock.JobQueue.Payload do
  @moduledoc """
  Payload encoding and decoding for job queue items.

  Payloads are stored as JSON-encoded binaries. Binary payloads are passed through
  unchanged for efficiency (assumed to already be encoded).
  """

  @doc """
  Encodes a payload for storage.

  Binary payloads are passed through unchanged.
  Other payloads are JSON encoded.
  """
  @spec encode(term()) :: binary()
  def encode(payload) when is_binary(payload), do: payload
  def encode(payload), do: Jason.encode!(payload)

  @doc """
  Decodes a stored payload for job execution.

  Attempts JSON decode with atom keys. If decode fails (e.g., binary wasn't JSON),
  wraps the raw binary in a map for the job to handle.

  ## Safety Note

  Uses `keys: :atoms!` which only converts keys to atoms if they already exist in
  the atom table. This prevents atom exhaustion from untrusted JSON payloads.
  Unknown keys will cause a decode error, falling back to `%{raw: payload}`.
  """
  @spec decode(binary()) :: term()
  def decode(payload) when is_binary(payload) do
    case Jason.decode(payload, keys: :atoms!) do
      {:ok, decoded} -> decoded
      {:error, _} -> %{raw: payload}
    end
  end

  def decode(payload), do: payload
end
