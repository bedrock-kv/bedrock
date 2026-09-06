defmodule Bedrock.Test.RecoveryAuthorityTestSupport do
  @moduledoc false

  alias Bedrock.Service.Manifest
  alias Bedrock.Service.RecoveryControl

  defmodule TestCluster do
    @moduledoc false
    def name, do: "recovery-authority-test-cluster"
    def otp_name(component), do: :"recovery_authority_test_#{component}"
  end

  def authority(generation \\ 1, recovery_id \\ "test-recovery") do
    %{generation: generation, recovery_id: recovery_id}
  end

  def prepare_worker!(path, id, worker, opts \\ []) do
    cluster = Keyword.get(opts, :cluster, TestCluster)
    params = opts |> Keyword.get(:params, %{}) |> Map.put("recovery_authority_protocol", 1)

    File.mkdir_p!(path)
    :ok = RecoveryControl.write(path, RecoveryControl.no_grant(cluster, id, worker))
    manifest = Manifest.new(cluster.name(), id, worker, params)
    :ok = Manifest.write_to_file(manifest, Path.join(path, "manifest.json"))
    cluster
  end
end
