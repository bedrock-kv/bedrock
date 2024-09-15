defmodule Bedrock.DataPlane.Version do
  @type t :: binary() | non_neg_integer()

  def newer?(:undefined, :undefined), do: false
  def newer?(:undefined, _last_version), do: false
  def newer?(_version, :undefined), do: true
  def newer?(version, last_version), do: version > last_version

  def older?(:undefined, :undefined), do: false
  def older?(:undefined, _last_version), do: true
  def older?(_version, :undefined), do: false
  def older?(version, last_version), do: version < last_version
end
