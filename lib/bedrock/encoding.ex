defmodule Bedrock.Encoding do
  @moduledoc false
  @callback pack(value :: any()) :: binary()
  @callback unpack(packed :: binary()) :: any()
end
