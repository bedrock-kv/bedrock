defmodule Bedrock.DataPlane.Log.Shale do
  @moduledoc false

  use Bedrock.Service.WorkerBehaviour, kind: :log

  alias Bedrock.DataPlane.Log.Shale.Server

  @doc false
  defdelegate child_spec(opts), to: Server
end
