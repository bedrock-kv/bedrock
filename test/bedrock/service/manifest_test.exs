defmodule Bedrock.Service.ManifestTest do
  use ExUnit.Case, async: true

  alias Bedrock.Service.Manifest

  defp storage, do: Bedrock.DataPlane.Storage.Basalt
  defp log, do: Bedrock.DataPlane.Log.Shale

  # Helper function to write manifest to temp file and return path
  defp write_manifest_to_temp_file(manifest, tmp_dir, filename \\ "manifest.json") do
    path = Path.join(tmp_dir, filename)
    :ok = Manifest.write_to_file(manifest, path)
    path
  end

  # Helper function to write JSON content directly to temp file
  defp write_json_to_temp_file(json_content, tmp_dir, filename) do
    path = Path.join(tmp_dir, filename)
    File.write(path, json_content)
    path
  end

  describe "load_from_file/1 success cases" do
    @tag :tmp_dir
    test "succeeds for a storage worker", %{tmp_dir: tmp_dir} do
      manifest = Manifest.new("test_cluster", "test_id", storage())
      path = write_manifest_to_temp_file(manifest, tmp_dir)

      assert {:ok, ^manifest} = Manifest.load_from_file(path)
    end

    @tag :tmp_dir
    test "succeeds for a transaction log worker", %{tmp_dir: tmp_dir} do
      manifest = Manifest.new("test_cluster", "test_id", log())
      path = write_manifest_to_temp_file(manifest, tmp_dir)

      assert {:ok, ^manifest} = Manifest.load_from_file(path)
    end
  end

  describe "load_from_file/1 worker module validation errors" do
    @tag :tmp_dir
    test "fails when referenced worker module doesn't exist", %{tmp_dir: tmp_dir} do
      manifest = Manifest.new("test_cluster", "test_id", Does.Not.Exist)
      path = write_manifest_to_temp_file(manifest, tmp_dir)

      assert {:error, :worker_module_does_not_exist} = Manifest.load_from_file(path)
    end

    @tag :tmp_dir
    test "fails when referenced worker module doesn't implement WorkerBehaviour", %{tmp_dir: tmp_dir} do
      manifest = Manifest.new("test_cluster", "test_id", Bedrock.Cluster)
      path = write_manifest_to_temp_file(manifest, tmp_dir)

      assert {:error, :worker_module_does_not_implement_behaviour} = Manifest.load_from_file(path)
    end
  end

  describe "load_from_file/1 file system errors" do
    test "fails when file doesn't exist" do
      assert {:error, :manifest_does_not_exist} = Manifest.load_from_file("does_not_exist.json")
    end
  end

  describe "load_from_file/1 JSON parsing errors" do
    @tag :tmp_dir
    test "fails when file contains invalid JSON", %{tmp_dir: tmp_dir} do
      path = write_json_to_temp_file("not valid json", tmp_dir, "invalid.json")

      assert {:error, :manifest_is_invalid} = Manifest.load_from_file(path)
    end

    @tag :tmp_dir
    test "fails when JSON is not a dictionary", %{tmp_dir: tmp_dir} do
      path = write_json_to_temp_file("[1, 2, 3]", tmp_dir, "not_a_dict.json")

      assert {:error, :manifest_is_not_a_dictionary} = Manifest.load_from_file(path)
    end
  end

  describe "load_from_file/1 field validation errors" do
    @tag :tmp_dir
    test "fails when worker field is invalid", %{tmp_dir: tmp_dir} do
      json = ~s({"cluster": "test_cluster", "id": "test_id", "worker": 1234})
      path = write_json_to_temp_file(json, tmp_dir, "invalid_worker.json")

      assert {:error, :invalid_worker_name} = Manifest.load_from_file(path)
    end

    @tag :tmp_dir
    test "fails when cluster field is invalid", %{tmp_dir: tmp_dir} do
      json = ~s({"cluster": 123, "id": "test_id", "worker": 1234})
      path = write_json_to_temp_file(json, tmp_dir, "invalid_cluster.json")

      assert {:error, :invalid_cluster_name} = Manifest.load_from_file(path)
    end

    @tag :tmp_dir
    test "fails when id field is invalid", %{tmp_dir: tmp_dir} do
      json = ~s({"cluster": "test_cluster", "id": 1234, "worker": 1234})
      path = write_json_to_temp_file(json, tmp_dir, "invalid_id.json")

      assert {:error, :invalid_cluster_id} = Manifest.load_from_file(path)
    end
  end
end
