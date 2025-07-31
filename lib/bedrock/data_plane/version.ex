defmodule Bedrock.DataPlane.Version do
  @moduledoc """
  Version comparison utilities for Bedrock's MVCC system.
  """
  @type t :: Bedrock.version()

  @spec newer?(t(), t()) :: boolean()
  def newer?(0, 0), do: false
  def newer?(0, _last_version), do: false
  def newer?(_version, 0), do: true
  def newer?(version, last_version), do: version > last_version

  @spec older?(t(), t()) :: boolean()
  def older?(0, 0), do: false
  def older?(0, _last_version), do: true
  def older?(_version, 0), do: false
  def older?(version, last_version), do: version < last_version
end
