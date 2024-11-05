defmodule Bedrock.DataPlane.Version do
  @type t :: binary() | non_neg_integer()

  def newer?(:start, :start), do: false
  def newer?(:start, _last_version), do: false
  def newer?(_version, :start), do: true
  def newer?(version, last_version), do: version > last_version

  def older?(:start, :start), do: false
  def older?(:start, _last_version), do: true
  def older?(_version, :start), do: false
  def older?(version, last_version), do: version < last_version
end
