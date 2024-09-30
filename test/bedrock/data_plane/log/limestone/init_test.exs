defmodule Bedrock.DataPlane.Log.Limestone.InitTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Log.Limestone

  def with_id(context) do
    id = Faker.UUID.v4()
    {:ok, context |> Map.put(:id, id)}
  end

  describe "Limestone.child_spec/1" do
    setup :with_id

    @tag :tmp_dir
    test "starts properly", %{id: id, tmp_dir: tmp_dir} do
      child_spec =
        Limestone.child_spec(
          cluster: "test",
          path: tmp_dir,
          id: id,
          otp_name: :test_transaction_log_engine,
          controller: self()
        )

      pid = start_supervised!(child_spec)

      assert Process.alive?(pid)
    end
  end
end
