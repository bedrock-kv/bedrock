defmodule Bedrock.Service.ManifestTest do
  use ExUnit.Case, async: true
  alias Bedrock.Service.Manifest

  defp storage_worker, do: Bedrock.Service.StorageWorker.Basalt
  defp transaction_log_worker, do: Bedrock.Service.TransactionLog.Limestone

  describe "Manifest reading and writing" do
    @tag :tmp_dir
    test "succeeds for a storage worker", %{tmp_dir: tmp_dir} do
      manifest = Manifest.new("test_cluster", "test_id", storage_worker())
      path = Path.join(tmp_dir, "manifest.json")
      :ok = Manifest.write_to_file(manifest, path)

      assert {:ok, ^manifest} = Manifest.load_from_file(path)
    end

    @tag :tmp_dir
    test "succeeds for a transaction log worker", %{tmp_dir: tmp_dir} do
      manifest = Manifest.new("test_cluster", "test_id", transaction_log_worker())
      path = Path.join(tmp_dir, "manifest.json")
      :ok = Manifest.write_to_file(manifest, path)

      assert {:ok, ^manifest} = Manifest.load_from_file(path)
    end

    @tag :tmp_dir
    test "fails when loading a referenced worker module that doesn't exist", %{
      tmp_dir: tmp_dir
    } do
      manifest = Manifest.new("test_cluster", "test_id", Does.Not.Exist)
      path = Path.join(tmp_dir, "manifest.json")
      :ok = Manifest.write_to_file(manifest, path)

      assert {:error, :worker_module_does_not_exist} = Manifest.load_from_file(path)
    end

    @tag :tmp_dir
    test "fails when loading a referenced worker module that doesn't implement Bedrock.Service.WorkerBehaviour",
         %{tmp_dir: tmp_dir} do
      manifest = Manifest.new("test_cluster", "test_id", Bedrock.Cluster)
      path = Path.join(tmp_dir, "manifest.json")
      :ok = Manifest.write_to_file(manifest, path)

      assert {:error, :worker_module_does_not_implement_behaviour} = Manifest.load_from_file(path)
    end

    test "fails when trying to load a file that doesn't exist" do
      assert {:error, :manifest_does_not_exist} = Manifest.load_from_file("does_not_exist.json")
    end

    @tag :tmp_dir
    test "fails when trying to load a file that has invalid contents", %{tmp_dir: tmp_dir} do
      path = Path.join(tmp_dir, "invalid_contents.json")
      File.write(path, "not valid json")

      assert {:error, :manifest_is_invalid} =
               Manifest.load_from_file(path)
    end

    @tag :tmp_dir
    test "fails when trying to load a file that has valid json, but isn't a dictionary", %{
      tmp_dir: tmp_dir
    } do
      path = Path.join(tmp_dir, "not_a_dict.json")
      File.write(path, "[1, 2, 3]")

      assert {:error, :manifest_is_not_a_dictionary} =
               Manifest.load_from_file(path)
    end

    @tag :tmp_dir
    test "fails when trying to load a file that has an invalid worker name", %{
      tmp_dir: tmp_dir
    } do
      path = Path.join(tmp_dir, "not_a_dict.json")
      File.write(path, "{\"cluster\": \"test_cluster\", \"id\": \"test_id\", \"worker\": 1234}")

      assert {:error, :invalid_worker_name} = Manifest.load_from_file(path)
    end

    @tag :tmp_dir
    test "fails when trying to load a file that has an invalid cluster name", %{
      tmp_dir: tmp_dir
    } do
      path = Path.join(tmp_dir, "not_a_dict.json")
      File.write(path, "{\"cluster\": 123, \"id\": \"test_id\", \"worker\": 1234}")

      assert {:error, :invalid_cluster_name} = Manifest.load_from_file(path)
    end

    @tag :tmp_dir
    test "fails when trying to load a file that has an invalid cluster id", %{
      tmp_dir: tmp_dir
    } do
      path = Path.join(tmp_dir, "not_a_dict.json")
      File.write(path, "{\"cluster\": \"test_cluster\", \"id\": 1234, \"worker\": 1234}")

      assert {:error, :invalid_cluster_id} = Manifest.load_from_file(path)
    end
  end
end
