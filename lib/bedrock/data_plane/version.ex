defmodule Bedrock.DataPlane.Version do
  @type t :: binary() | non_neg_integer()

  def newer?(0, 0), do: false
  def newer?(0, _last_version), do: false
  def newer?(_version, 0), do: true
  def newer?(version, last_version), do: version > last_version

  def older?(0, 0), do: false
  def older?(0, _last_version), do: true
  def older?(_version, 0), do: false
  def older?(version, last_version), do: version < last_version
end
