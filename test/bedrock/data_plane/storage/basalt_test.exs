defmodule Bedrock.DataPlane.Storage.BasaltTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Storage.Basalt

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
               id: {Basalt.Server, ^expected_id},
               start: {
                 GenServer,
                 :start_link,
                 [
                   Basalt.Server,
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

      # We expect the worker to eventually send a message to the controller
      # when it's ready to accept requests.
      assert_receive {:"$gen_cast", {:worker_health, ^expected_id, :ok}}, 5_000

      # At this point, a physical database should have been created on disk.
      assert File.exists?(Path.join(tmp_dir, "dets"))
    end
  end

  describe "Basalt.Logic" do
    @tag :tmp_dir
    test "info/2 will return data for all topics it advertises as supported", %{tmp_dir: tmp_dir} do
      {:ok, state} =
        Basalt.Logic.startup(
          :test_storage_engine,
          self(),
          id(),
          tmp_dir
        )

      {:ok, supported_info} = Basalt.Logic.info(state, :supported_info)

      assert {:ok, all_supported_info} = Basalt.Logic.info(state, supported_info)

      assert all_supported_info
             |> Keyword.keys()
             |> Enum.sort() == supported_info |> Enum.sort()
    end
  end
end
