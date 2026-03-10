defmodule Bedrock.DataPlane.Materializer.Olivine.DurabilityContractTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Materializer.Olivine.Database
  alias Bedrock.DataPlane.Version
  alias Bedrock.Internal.ClusterSupervisor

  defp setup_tmp_dir(context) do
    tmp_dir =
      context[:tmp_dir] ||
        Path.join(System.tmp_dir!(), "olivine_durability_contract_#{System.unique_integer([:positive])}")

    File.mkdir_p!(tmp_dir)

    on_exit(fn ->
      File.rm_rf(tmp_dir)
    end)

    {:ok, tmp_dir: tmp_dir}
  end

  defp data_size_in_bytes({data_db, _index_db}), do: data_db.file_offset

  @tag :tmp_dir
  setup context do
    setup_tmp_dir(context)
  end

  test "durable version does not advance when data flush fails", %{tmp_dir: tmp_dir} do
    file_path = Path.join(tmp_dir, "data_flush_failure.dets")
    {:ok, db} = Database.open(:olivine_data_flush_contract, file_path)

    version = Version.from_integer(1)
    {:ok, _locator, db} = Database.store_value(db, "key", version, "value")

    previous_version = Database.durable_version(db)
    {data_db, _index_db} = db

    :ok = :file.close(data_db.file)

    assert {:error, {:data_flush_failed, _reason}} =
             Database.advance_durable_version(db, version, previous_version, data_size_in_bytes(db), [%{}])

    assert Database.durable_version(db) == previous_version
    assert :ok = Database.close(db)
  end

  test "durable version does not advance when index flush fails", %{tmp_dir: tmp_dir} do
    file_path = Path.join(tmp_dir, "index_flush_failure.dets")
    {:ok, db} = Database.open(:olivine_index_flush_contract, file_path)

    version = Version.from_integer(2)
    {:ok, _locator, db} = Database.store_value(db, "key", version, "value")

    previous_version = Database.durable_version(db)
    {_data_db, index_db} = db

    :ok = :file.close(index_db.file)

    assert {:error, {:index_flush_failed, _reason}} =
             Database.advance_durable_version(db, version, previous_version, data_size_in_bytes(db), [%{}])

    assert Database.durable_version(db) == previous_version
    assert :ok = Database.close(db)
  end

  test "durability mode defaults and unsupported values resolve to strict" do
    assert :strict == ClusterSupervisor.durability_mode([])
    assert :strict == ClusterSupervisor.durability_mode(durability: [mode: :unsupported])
    assert :strict == ClusterSupervisor.durability_mode(durability_mode: :unsupported)
    assert :relaxed == ClusterSupervisor.durability_mode(durability_mode: :relaxed)
  end
end
