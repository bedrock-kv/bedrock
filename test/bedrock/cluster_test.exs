defmodule Bedrock.ClusterTest do
  use ExUnit.Case
  alias Faker

  alias Bedrock.Cluster

  describe "default_descriptor_file_name/0" do
    test "returns the default descriptor file name" do
      assert Cluster.default_descriptor_file_name() == "bedrock.cluster"
    end
  end

  describe "otp_name/2" do
    test "returns the OTP name for the given cluster and name" do
      name = Faker.Lorem.word()
      service = ~w[airport bus_station train_station taxi_stand]a |> Enum.random()

      assert Cluster.otp_name(name, service) == :"bedrock_#{name}_#{service}"
    end
  end
end
