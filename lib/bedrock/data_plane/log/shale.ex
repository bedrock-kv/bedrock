defmodule Bedrock.DataPlane.Log.Shale do
  @moduledoc false

  alias Bedrock.DataPlane.Log.Shale.Server

  use Bedrock.Service.WorkerBehaviour, kind: :log

  @doc false
  defdelegate child_spec(opts), to: Server
end
