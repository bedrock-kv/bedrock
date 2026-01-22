defmodule Bedrock.DataPlane.Materializer.Basalt.KeyspaceTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Materializer.Basalt.Keyspace
  alias Bedrock.DataPlane.Version
  alias Bedrock.Test.DataPlane.TransactionTestSupport

  def new_random_keyspace, do: Keyspace.new(:"keyspace_#{Faker.random_between(0, 10_000)}")

  describe "Keyspace.new/1" do
    test "it returns a new ETS table" do
      assert is_reference(Keyspace.new(:foo))
    end
  end

  describe "Keyspace.close/1" do
    test "closes and deletes the ETS table" do
      keyspace = new_random_keyspace()

      # Verify keyspace exists
      assert :ets.info(keyspace) != :undefined

      # Close it
      assert :ok = Keyspace.close(keyspace)

      # Verify it's gone
      assert :ets.info(keyspace) == :undefined
    end

    test "returns :ok even after closing" do
      keyspace = new_random_keyspace()
      assert :ok = Keyspace.close(keyspace)
    end
  end

  describe "Keyspace.apply_transaction/2" do
    test "adds keys to the space" do
      keyspace = new_random_keyspace()

      assert :ok =
               Keyspace.apply_transaction(
                 keyspace,
                 TransactionTestSupport.new_log_transaction(Version.from_integer(0), %{
                   "a" => "a",
                   "c" => "c",
                   "d" => "d",
                   "e" => "e"
                 })
               )

      version_0 = Version.from_integer(0)

      assert [
               {:last_version, ^version_0},
               {"a", true},
               {"c", true},
               {"d", true},
               {"e", true}
             ] = :ets.tab2list(keyspace)
    end

    test "adds keys to space that already has keys" do
      keyspace = new_random_keyspace()

      assert :ok =
               Keyspace.apply_transaction(
                 keyspace,
                 TransactionTestSupport.new_log_transaction(Version.from_integer(0), %{
                   "a" => "a",
                   "c" => "c",
                   "d" => "d",
                   "e" => "e"
                 })
               )

      assert :ok =
               Keyspace.apply_transaction(
                 keyspace,
                 TransactionTestSupport.new_log_transaction(Version.from_integer(1), %{
                   "f" => "f",
                   "g" => "g",
                   "h" => "h",
                   "i" => "i"
                 })
               )

      version_1 = Version.from_integer(1)

      assert [
               {:last_version, ^version_1},
               {"a", true},
               {"c", true},
               {"d", true},
               {"e", true},
               {"f", true},
               {"g", true},
               {"h", true},
               {"i", true}
             ] = :ets.tab2list(keyspace)
    end

    test "removes keys properly" do
      keyspace = new_random_keyspace()

      :ok =
        Keyspace.apply_transaction(
          keyspace,
          TransactionTestSupport.new_log_transaction(Version.from_integer(0), %{
            "a" => "a",
            "c" => "c",
            "d" => "d",
            "e" => "e"
          })
        )

      assert :ok =
               Keyspace.apply_transaction(
                 keyspace,
                 TransactionTestSupport.new_log_transaction(Version.from_integer(1), %{
                   "c" => nil,
                   "d" => nil
                 })
               )

      version_1 = Version.from_integer(1)

      assert [{:last_version, ^version_1}, {"a", true}, {"c", false}, {"d", false}, {"e", true}] =
               :ets.tab2list(keyspace)
    end
  end

  describe "Keyspace.prune/2" do
    test "succeeds and changes nothing when there are no keys to prune" do
      keyspace = new_random_keyspace()

      :ok =
        Keyspace.apply_transaction(
          keyspace,
          TransactionTestSupport.new_log_transaction(Version.from_integer(0), %{
            "a" => "a",
            "c" => "c",
            "d" => "d",
            "e" => "e"
          })
        )

      assert {:ok, 0} = Keyspace.prune(keyspace)

      version_0 = Version.from_integer(0)

      assert [
               {:last_version, ^version_0},
               {"a", true},
               {"c", true},
               {"d", true},
               {"e", true}
             ] = :ets.tab2list(keyspace)
    end

    test "succeeds and removes keys when there are keys to prune" do
      keyspace = new_random_keyspace()

      :ok =
        Keyspace.apply_transaction(
          keyspace,
          TransactionTestSupport.new_log_transaction(Version.from_integer(0), %{
            "a" => "a",
            "c" => "c",
            "d" => "d",
            "e" => "e"
          })
        )

      :ok =
        Keyspace.apply_transaction(
          keyspace,
          TransactionTestSupport.new_log_transaction(Version.from_integer(1), %{
            "a" => nil,
            "d" => nil
          })
        )

      assert {:ok, 2} = Keyspace.prune(keyspace)

      version_1 = Version.from_integer(1)

      assert [
               {:last_version, ^version_1},
               {"c", true},
               {"e", true}
             ] = :ets.tab2list(keyspace)
    end
  end

  describe "Keyspace.key_exists?/2" do
    test "returns true when key exists" do
      keyspace = new_random_keyspace()

      :ok =
        Keyspace.apply_transaction(
          keyspace,
          TransactionTestSupport.new_log_transaction(Version.from_integer(0), %{
            "a" => "a",
            "c" => "c",
            "d" => "d",
            "e" => "e"
          })
        )

      assert true = Keyspace.key_exists?(keyspace, "a")
    end

    test "returns false when key does not exist" do
      keyspace = new_random_keyspace()

      :ok =
        Keyspace.apply_transaction(
          keyspace,
          TransactionTestSupport.new_log_transaction(Version.from_integer(0), %{
            "a" => "a",
            "c" => "c",
            "d" => "d",
            "e" => "e"
          })
        )

      refute false = Keyspace.key_exists?(keyspace, "q")
    end
  end

  describe "Keyspace.insert_many/2" do
    test "inserts multiple keys into keyspace" do
      keyspace = new_random_keyspace()

      keys = ["key1", "key2", "key3", "key4"]
      assert :ok = Keyspace.insert_many(keyspace, keys)

      # Verify all keys exist
      assert Keyspace.key_exists?(keyspace, "key1")
      assert Keyspace.key_exists?(keyspace, "key2")
      assert Keyspace.key_exists?(keyspace, "key3")
      assert Keyspace.key_exists?(keyspace, "key4")
    end

    test "inserts empty list successfully" do
      keyspace = new_random_keyspace()
      assert :ok = Keyspace.insert_many(keyspace, [])
    end

    test "inserted keys are marked as present" do
      keyspace = new_random_keyspace()

      keys = ["a", "b", "c"]
      Keyspace.insert_many(keyspace, keys)

      # Check the ETS table directly to verify presence markers
      assert [{"a", true}, {"b", true}, {"c", true}] = keyspace |> :ets.tab2list() |> Enum.sort()
    end
  end
end
