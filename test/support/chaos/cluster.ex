defmodule Bedrock.Test.Chaos.Cluster do
  @moduledoc false
  use Bedrock.Cluster, otp_app: :bedrock, name: "chaos"
end

defmodule Bedrock.Test.Chaos.Repo do
  @moduledoc false
  use Bedrock.Repo, cluster: Bedrock.Test.Chaos.Cluster
end

defmodule Bedrock.Test.Chaos.Ops do
  @moduledoc """
  Transactions the harness runs on a peer node.

  These have to be named functions in compiled code rather than closures built
  by the caller: peers get the primary's code paths, which cover `test/support`,
  but `.exs` test files are only ever evaluated in the primary VM. A closure
  defined in a test file has no module on the peer and fails to apply there.
  """

  alias Bedrock.Test.Chaos.Repo

  @spec put(binary(), binary()) :: :ok
  def put(key, value), do: Repo.transact(fn -> Repo.put(key, value) end)

  @spec get(binary()) :: nil | binary()
  def get(key), do: Repo.transact(fn -> Repo.get(key) end)
end
