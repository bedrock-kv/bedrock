defmodule Bedrock.ControlPlane.Config.LogDescriptorTest do
  use ExUnit.Case, async: true
  alias Bedrock.ControlPlane.Config.LogDescriptor

  describe "new/2" do
    test "creates a new LogDescriptor struct with given tags and log_worker_id" do
      tags = [1, 2, 3]
      log_worker_id = :worker_1

      log_descriptor = LogDescriptor.new(tags, log_worker_id)

      assert %LogDescriptor{tags: ^tags, log_worker_id: ^log_worker_id} = log_descriptor
    end

    test "creates a new LogDescriptor struct with empty tags and nil log_worker_id when given empty list and nil" do
      tags = []
      log_worker_id = nil

      log_descriptor = LogDescriptor.new(tags, log_worker_id)

      assert %LogDescriptor{tags: ^tags, log_worker_id: ^log_worker_id} = log_descriptor
    end
  end
end
