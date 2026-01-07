defmodule Bedrock.DataPlane.Resolver.MetadataAccumulatorDocTest do
  use ExUnit.Case, async: true

  import Bedrock.DataPlane.Resolver.MetadataAccumulator

  alias Bedrock.DataPlane.Resolver.MetadataAccumulator
  alias Bedrock.DataPlane.Version

  def v(n), do: Version.from_integer(n)

  doctest MetadataAccumulator
end
