defmodule Bedrock.DataPlane.MaterializerSystem.Engine.Basalt.PersistentKeyValuesTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Materializer.Basalt.PersistentKeyValues
  alias Bedrock.DataPlane.Version
  alias Bedrock.Test.DataPlane.TransactionTestSupport

  defp random_file_name do
    random_chars =
      16
      |> :crypto.strong_rand_bytes()
      |> Base.encode16()
      |> String.downcase()

    "#{random_chars}.dets"
  end

  defp with_empty_pkv(context) do
    file_name = random_file_name()
    {:ok, pkv} = file_name |> String.to_atom() |> PersistentKeyValues.open(file_name)

    :ok =
      PersistentKeyValues.apply_transaction(
        pkv,
        TransactionTestSupport.new_log_transaction(Version.zero(), %{})
      )

    on_exit(fn ->
      File.rm!(file_name)
    end)

    {:ok, Map.put(context, :pkv, pkv)}
  end

  describe "Basalt.PersistentKeyValues.open/2" do
    test "opens a persistent key-value store" do
      file_name = random_file_name()

      assert {:ok, pkv} = PersistentKeyValues.open(:test, file_name)

      assert pkv

      assert File.exists?(file_name)
      File.rm!(file_name)
    end
  end

  describe "Basalt.PersistentKeyValues.last_version/1" do
    setup :with_empty_pkv

    test "returns 0 on a newly created key-value store", %{pkv: pkv} do
      zero_version = Version.zero()
      assert ^zero_version = PersistentKeyValues.last_version(pkv)
    end

    test "returns the correct version after storing one transaction", %{pkv: pkv} do
      :ok =
        PersistentKeyValues.apply_transaction(
          pkv,
          TransactionTestSupport.new_log_transaction(Version.zero(), %{"foo" => "bar"})
        )

      zero_version = Version.zero()
      assert ^zero_version = PersistentKeyValues.last_version(pkv)
    end

    test "returns the correct version after storing two transactions", %{pkv: pkv} do
      :ok =
        PersistentKeyValues.apply_transaction(
          pkv,
          TransactionTestSupport.new_log_transaction(Version.zero(), %{"foo" => "bar"})
        )

      :ok =
        PersistentKeyValues.apply_transaction(
          pkv,
          TransactionTestSupport.new_log_transaction(Version.from_integer(1), %{
            "foo" => "baz"
          })
        )

      version_one = Version.from_integer(1)
      assert ^version_one = PersistentKeyValues.last_version(pkv)
    end
  end

  describe "Basalt.PersistentKeyValues.apply_transaction/2" do
    setup :with_empty_pkv

    test "stores key-values correctly", %{pkv: pkv} do
      :ok =
        PersistentKeyValues.apply_transaction(
          pkv,
          TransactionTestSupport.new_log_transaction(Version.zero(), %{"foo" => "bar"})
        )

      assert {:ok, "bar"} = PersistentKeyValues.fetch(pkv, "foo")
    end

    test "overwrites previous value for key", %{pkv: pkv} do
      :ok =
        PersistentKeyValues.apply_transaction(
          pkv,
          TransactionTestSupport.new_log_transaction(Version.zero(), %{"foo" => "bar"})
        )

      :ok =
        PersistentKeyValues.apply_transaction(
          pkv,
          TransactionTestSupport.new_log_transaction(Version.from_integer(1), %{
            "foo" => "baz"
          })
        )

      assert {:ok, "baz"} = PersistentKeyValues.fetch(pkv, "foo")
    end

    test "does not allow older transactions to be written after newer ones", %{pkv: pkv} do
      assert :ok =
               PersistentKeyValues.apply_transaction(
                 pkv,
                 TransactionTestSupport.new_log_transaction(Version.zero(), %{
                   "foo" => "baz"
                 })
               )

      assert :ok =
               PersistentKeyValues.apply_transaction(
                 pkv,
                 TransactionTestSupport.new_log_transaction(Version.from_integer(1), %{
                   "foo" => "baz"
                 })
               )

      assert :ok =
               PersistentKeyValues.apply_transaction(
                 pkv,
                 TransactionTestSupport.new_log_transaction(Version.from_integer(2), %{
                   "foo" => "baz"
                 })
               )

      assert {:error, :version_too_old} ==
               PersistentKeyValues.apply_transaction(
                 pkv,
                 TransactionTestSupport.new_log_transaction(Version.from_integer(1), %{
                   "foo" => "bar"
                 })
               )

      assert {:ok, "baz"} == PersistentKeyValues.fetch(pkv, "foo")
    end
  end

  describe "Basalt.PersistentKeyValues.stream_keys/1" do
    setup :with_empty_pkv

    test "returns correct set of keys", %{pkv: pkv} do
      :ok =
        PersistentKeyValues.apply_transaction(
          pkv,
          TransactionTestSupport.new_log_transaction(Version.from_integer(1), %{
            "foo" => "bar",
            "a" => "1"
          })
        )

      :ok =
        PersistentKeyValues.apply_transaction(
          pkv,
          TransactionTestSupport.new_log_transaction(Version.from_integer(2), %{
            "foo" => "baz",
            "l" => "3"
          })
        )

      :ok =
        PersistentKeyValues.apply_transaction(
          pkv,
          TransactionTestSupport.new_log_transaction(Version.from_integer(3), %{
            "foo" => "biz",
            "j" => "2"
          })
        )

      :ok =
        PersistentKeyValues.apply_transaction(
          pkv,
          TransactionTestSupport.new_log_transaction(Version.from_integer(4), %{
            "foo" => "buz"
          })
        )

      :ok =
        PersistentKeyValues.apply_transaction(
          pkv,
          TransactionTestSupport.new_log_transaction(Version.from_integer(5), %{
            <<0xFF, 0xFF>> => "system_key"
          })
        )

      assert ["a", "foo", "j", "l", <<0xFF, 0xFF>>] ==
               pkv
               |> PersistentKeyValues.stream_keys()
               |> Enum.to_list()
               |> Enum.sort()
    end
  end

  describe "Basalt.PersistentKeyValues.info/2" do
    setup :with_empty_pkv

    test "returns file size in bytes for :size_in_bytes query", %{pkv: pkv} do
      # Should return a non-negative integer
      assert is_integer(PersistentKeyValues.info(pkv, :size_in_bytes))
      assert PersistentKeyValues.info(pkv, :size_in_bytes) >= 0
    end

    test "returns :undefined for any other query", %{pkv: pkv} do
      assert :undefined == PersistentKeyValues.info(pkv, :unknown_query)
      assert :undefined == PersistentKeyValues.info(pkv, :other)
      assert :undefined == PersistentKeyValues.info(pkv, :memory)
    end
  end
end
