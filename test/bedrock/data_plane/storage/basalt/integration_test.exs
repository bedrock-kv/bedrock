defmodule Bedrock.DataPlane.Storage.Basalt.IntegrationTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Storage.Basalt.Database
  alias Bedrock.DataPlane.TransactionTestSupport
  alias Bedrock.DataPlane.Version

  def random_name, do: String.to_atom("basalt_integration_#{Faker.random_between(0, 10_000)}")

  describe "Transaction sequence application and versioned reads" do
    @tag :tmp_dir
    test "applies sequence of transactions and reads correctly at different versions", %{tmp_dir: tmp_dir} do
      {:ok, db} = Database.open(random_name(), Path.join(tmp_dir, "sequence_test"))

      v1 = Version.from_integer(1)
      v2 = Version.from_integer(2)
      v3 = Version.from_integer(3)
      v4 = Version.from_integer(4)
      v5 = Version.from_integer(5)

      assert ^v1 =
               Database.apply_transactions(db, [
                 TransactionTestSupport.new_log_transaction(v1, %{
                   "user:1" => "alice",
                   "user:2" => "bob",
                   "config:timeout" => "30"
                 })
               ])

      assert ^v2 =
               Database.apply_transactions(db, [
                 TransactionTestSupport.new_log_transaction(v2, %{
                   "user:1" => "alice_updated",
                   "user:3" => "charlie",
                   "config:max_connections" => "100"
                 })
               ])

      assert ^v3 =
               Database.apply_transactions(db, [
                 TransactionTestSupport.new_log_transaction(v3, %{
                   "user:2" => nil,
                   "config:timeout" => "45"
                 })
               ])

      assert ^v4 =
               Database.apply_transactions(db, [
                 TransactionTestSupport.new_log_transaction(v4, %{
                   "user:4" => "diana",
                   "user:5" => "eve",
                   "stats:total_users" => "4"
                 })
               ])

      assert ^v5 =
               Database.apply_transactions(db, [
                 TransactionTestSupport.new_log_transaction(v5, %{
                   "user:3" => nil,
                   "config:max_connections" => "200",
                   "stats:active_users" => "3"
                 })
               ])

      # Test reads progressively as we make transactions durable
      assert :ok = Database.ensure_durability_to_version(db, v1)
      assert {:ok, "alice"} = Database.fetch(db, "user:1", v1)
      assert {:ok, "bob"} = Database.fetch(db, "user:2", v1)
      assert {:error, :not_found} = Database.fetch(db, "user:3", v1)
      assert {:error, :not_found} = Database.fetch(db, "user:4", v1)
      assert {:ok, "30"} = Database.fetch(db, "config:timeout", v1)
      assert {:error, :not_found} = Database.fetch(db, "config:max_connections", v1)

      assert :ok = Database.ensure_durability_to_version(db, v2)
      assert {:ok, "alice_updated"} = Database.fetch(db, "user:1", v2)
      assert {:ok, "bob"} = Database.fetch(db, "user:2", v2)
      assert {:ok, "charlie"} = Database.fetch(db, "user:3", v2)
      assert {:error, :not_found} = Database.fetch(db, "user:4", v2)
      assert {:ok, "30"} = Database.fetch(db, "config:timeout", v2)
      assert {:ok, "100"} = Database.fetch(db, "config:max_connections", v2)

      assert :ok = Database.ensure_durability_to_version(db, v3)
      assert {:ok, "alice_updated"} = Database.fetch(db, "user:1", v3)
      assert {:error, :not_found} = Database.fetch(db, "user:2", v3)
      assert {:ok, "charlie"} = Database.fetch(db, "user:3", v3)
      assert {:ok, "45"} = Database.fetch(db, "config:timeout", v3)
      assert {:ok, "100"} = Database.fetch(db, "config:max_connections", v3)

      assert :ok = Database.ensure_durability_to_version(db, v4)
      assert {:ok, "alice_updated"} = Database.fetch(db, "user:1", v4)
      assert {:error, :not_found} = Database.fetch(db, "user:2", v4)
      assert {:ok, "charlie"} = Database.fetch(db, "user:3", v4)
      assert {:ok, "diana"} = Database.fetch(db, "user:4", v4)
      assert {:ok, "eve"} = Database.fetch(db, "user:5", v4)
      assert {:ok, "4"} = Database.fetch(db, "stats:total_users", v4)
      assert {:error, :not_found} = Database.fetch(db, "stats:active_users", v4)

      assert :ok = Database.ensure_durability_to_version(db, v5)
      assert {:ok, "alice_updated"} = Database.fetch(db, "user:1", v5)
      assert {:error, :not_found} = Database.fetch(db, "user:2", v5)
      assert {:error, :not_found} = Database.fetch(db, "user:3", v5)
      assert {:ok, "diana"} = Database.fetch(db, "user:4", v5)
      assert {:ok, "eve"} = Database.fetch(db, "user:5", v5)
      assert {:ok, "200"} = Database.fetch(db, "config:max_connections", v5)
      assert {:ok, "4"} = Database.fetch(db, "stats:total_users", v5)
      assert {:ok, "3"} = Database.fetch(db, "stats:active_users", v5)

      Database.close(db)
    end

    @tag :tmp_dir
    test "handles complex mutation types (set, clear) correctly", %{tmp_dir: tmp_dir} do
      {:ok, db} = Database.open(random_name(), Path.join(tmp_dir, "complex_test"))

      v1 = Version.from_integer(1)
      v2 = Version.from_integer(2)
      v3 = Version.from_integer(3)

      assert ^v1 =
               Database.apply_transactions(db, [
                 TransactionTestSupport.new_log_transaction(v1, %{
                   "cache:session:1" => "user_alice",
                   "cache:session:2" => "user_bob",
                   "cache:session:3" => "user_charlie",
                   "cache:config:timeout" => "300",
                   "cache:config:max_size" => "1000",
                   "data:user:1" => "alice_data",
                   "data:user:2" => "bob_data",
                   "temp:job:1" => "processing",
                   "temp:job:2" => "queued"
                 })
               ])

      assert ^v2 =
               Database.apply_transactions(db, [
                 TransactionTestSupport.new_log_transaction(v2, %{
                   "cache:session:2" => nil,
                   "cache:config:timeout" => "600",
                   "data:user:3" => "charlie_data",
                   "temp:job:1" => nil
                 })
               ])

      assert ^v3 =
               Database.apply_transactions(db, [
                 TransactionTestSupport.new_log_transaction(v3, %{
                   "cache:session:1" => nil,
                   "cache:sessions_cleared" => "true"
                 })
               ])

      # Test reads progressively as we make transactions durable
      assert :ok = Database.ensure_durability_to_version(db, v1)
      assert {:ok, "user_alice"} = Database.fetch(db, "cache:session:1", v1)
      assert {:ok, "user_bob"} = Database.fetch(db, "cache:session:2", v1)
      assert {:ok, "user_charlie"} = Database.fetch(db, "cache:session:3", v1)
      assert {:ok, "300"} = Database.fetch(db, "cache:config:timeout", v1)
      assert {:ok, "alice_data"} = Database.fetch(db, "data:user:1", v1)
      assert {:ok, "processing"} = Database.fetch(db, "temp:job:1", v1)

      assert :ok = Database.ensure_durability_to_version(db, v2)
      assert {:ok, "user_alice"} = Database.fetch(db, "cache:session:1", v2)
      assert {:error, :not_found} = Database.fetch(db, "cache:session:2", v2)
      assert {:ok, "user_charlie"} = Database.fetch(db, "cache:session:3", v2)
      assert {:ok, "600"} = Database.fetch(db, "cache:config:timeout", v2)
      assert {:ok, "charlie_data"} = Database.fetch(db, "data:user:3", v2)
      assert {:error, :not_found} = Database.fetch(db, "temp:job:1", v2)

      assert :ok = Database.ensure_durability_to_version(db, v3)
      assert {:error, :not_found} = Database.fetch(db, "cache:session:1", v3)
      assert {:error, :not_found} = Database.fetch(db, "cache:session:2", v3)
      assert {:ok, "user_charlie"} = Database.fetch(db, "cache:session:3", v3)
      assert {:ok, "600"} = Database.fetch(db, "cache:config:timeout", v3)
      assert {:ok, "1000"} = Database.fetch(db, "cache:config:max_size", v3)
      assert {:ok, "alice_data"} = Database.fetch(db, "data:user:1", v3)
      assert {:ok, "true"} = Database.fetch(db, "cache:sessions_cleared", v3)

      Database.close(db)
    end

    @tag :tmp_dir
    test "maintains consistency across version pruning", %{tmp_dir: tmp_dir} do
      {:ok, db} = Database.open(random_name(), Path.join(tmp_dir, "pruning_test"))

      v1 = Version.from_integer(1)
      v2 = Version.from_integer(2)
      v3 = Version.from_integer(3)
      v4 = Version.from_integer(4)

      assert ^v1 =
               Database.apply_transactions(db, [
                 TransactionTestSupport.new_log_transaction(v1, %{
                   "key1" => "value1_v1",
                   "key2" => "value2_v1"
                 })
               ])

      assert ^v2 =
               Database.apply_transactions(db, [
                 TransactionTestSupport.new_log_transaction(v2, %{
                   "key1" => "value1_v2",
                   "key3" => "value3_v2"
                 })
               ])

      assert ^v3 =
               Database.apply_transactions(db, [
                 TransactionTestSupport.new_log_transaction(v3, %{
                   "key2" => "value2_v3",
                   "key4" => "value4_v3"
                 })
               ])

      assert ^v4 =
               Database.apply_transactions(db, [
                 TransactionTestSupport.new_log_transaction(v4, %{
                   "key1" => "value1_v4",
                   "key3" => nil
                 })
               ])

      assert :ok = Database.ensure_durability_to_version(db, v2)
      assert v2 = Database.last_durable_version(db)

      # v1 should be pruned but v2, v3, v4 should be available
      assert {:error, :version_too_old} = Database.fetch(db, "key1", v1)
      assert {:ok, "value1_v2"} = Database.fetch(db, "key1", v2)
      assert {:ok, "value1_v2"} = Database.fetch(db, "key1", v3)
      assert {:ok, "value1_v4"} = Database.fetch(db, "key1", v4)

      assert :ok = Database.ensure_durability_to_version(db, v4)
      assert v4 = Database.last_durable_version(db)

      # v2 and v3 should be pruned
      assert {:error, :version_too_old} = Database.fetch(db, "key1", v2)
      assert {:error, :version_too_old} = Database.fetch(db, "key1", v3)
      assert {:ok, "value1_v4"} = Database.fetch(db, "key1", v4)

      assert {:ok, "value1_v4"} = Database.fetch(db, "key1", v4)
      assert {:ok, "value2_v3"} = Database.fetch(db, "key2", v4)
      assert {:error, :not_found} = Database.fetch(db, "key3", v4)
      assert {:ok, "value4_v3"} = Database.fetch(db, "key4", v4)

      Database.close(db)
    end

    @tag :tmp_dir
    test "handles version isolation correctly", %{tmp_dir: tmp_dir} do
      {:ok, db} = Database.open(random_name(), Path.join(tmp_dir, "isolation_test"))

      v1 = Version.from_integer(1)
      v2 = Version.from_integer(2)
      v3 = Version.from_integer(3)

      assert ^v1 =
               Database.apply_transactions(db, [
                 TransactionTestSupport.new_log_transaction(v1, %{
                   "counter" => "1",
                   "user:1" => "alice"
                 })
               ])

      assert ^v2 =
               Database.apply_transactions(db, [
                 TransactionTestSupport.new_log_transaction(v2, %{
                   "counter" => "2",
                   "user:2" => "bob"
                 })
               ])

      assert ^v3 =
               Database.apply_transactions(db, [
                 TransactionTestSupport.new_log_transaction(v3, %{
                   "counter" => "3",
                   "user:3" => "charlie"
                 })
               ])

      assert :ok = Database.ensure_durability_to_version(db, v1)
      assert {:ok, "1"} = Database.fetch(db, "counter", v1)
      assert {:ok, "alice"} = Database.fetch(db, "user:1", v1)
      assert {:error, :not_found} = Database.fetch(db, "user:2", v1)
      assert {:error, :not_found} = Database.fetch(db, "user:3", v1)

      assert :ok = Database.ensure_durability_to_version(db, v2)
      assert {:ok, "2"} = Database.fetch(db, "counter", v2)
      assert {:ok, "alice"} = Database.fetch(db, "user:1", v2)
      assert {:ok, "bob"} = Database.fetch(db, "user:2", v2)
      assert {:error, :not_found} = Database.fetch(db, "user:3", v2)

      assert :ok = Database.ensure_durability_to_version(db, v3)
      assert {:ok, "3"} = Database.fetch(db, "counter", v3)
      assert {:ok, "alice"} = Database.fetch(db, "user:1", v3)
      assert {:ok, "bob"} = Database.fetch(db, "user:2", v3)
      assert {:ok, "charlie"} = Database.fetch(db, "user:3", v3)

      Database.close(db)
    end
  end
end
