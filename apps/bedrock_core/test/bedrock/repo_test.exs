defmodule Bedrock.RepoTest do
  use ExUnit.Case, async: true

  alias Bedrock.Repo

  defmodule TestCluster do
    @moduledoc false
    use Bedrock.Cluster, otp_app: :test_app, name: "test_cluster"
  end

  defmodule TestRepo do
    use Repo, cluster: TestCluster
  end
end
