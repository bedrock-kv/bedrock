defmodule Bedrock.Service.StorageWorker.Basalt.InitTest do
  use ExUnit.Case, async: true

  alias Bedrock.Service.StorageWorker.Basalt

  def id, do: Faker.UUID.v4()

  describe "Basalt.child_spec/1" do
    @tag :tmp_dir
    test "properly constructs a child spec", %{tmp_dir: tmp_dir} do
      expected_id = id()

      child_spec =
        Basalt.child_spec(
          cluster: "test",
          path: tmp_dir,
          id: expected_id,
          otp_name: :test_storage_engine,
          controller: self()
        )

      assert %{
               id: {Bedrock.Service.StorageWorker.Basalt, ^expected_id},
               start: {
                 GenServer,
                 :start_link,
                 [
                   Bedrock.Service.StorageWorker.Basalt,
                   {:test_storage_engine, pid, ^expected_id, ^tmp_dir},
                   [name: :test_storage_engine]
                 ]
               }
             } = child_spec

      assert(is_pid(pid))
    end
  end

  describe "Basalt" do
    @tag :tmp_dir
    test "lifecycle functions properly", %{tmp_dir: tmp_dir} do
      expected_id = id()

      child_spec =
        Basalt.child_spec(
          cluster: "test",
          path: tmp_dir,
          id: expected_id,
          otp_name: :test_storage_engine,
          controller: self()
        )

      pid = start_supervised!(child_spec)

      assert Process.alive?(pid)
      assert File.exists?(Path.join(tmp_dir, "dets"))
    end
  end
end
