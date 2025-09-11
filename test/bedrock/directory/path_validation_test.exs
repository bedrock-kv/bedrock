defmodule Bedrock.Directory.PathValidationTest do
  use ExUnit.Case, async: true

  import Mox

  alias Bedrock.Directory.Layer

  setup :verify_on_exit!

  describe "UTF-8 validation" do
    test "accepts valid UTF-8 strings" do
      _layer = Layer.new(MockRepo)

      # English
      assert Layer.validate_path(["hello", "world"]) == :ok

      # Unicode characters
      assert Layer.validate_path(["café", "naïve"]) == :ok
      assert Layer.validate_path(["日本語", "中文"]) == :ok
      assert Layer.validate_path(["emoji", "🚀"]) == :ok
      assert Layer.validate_path(["Ελληνικά", "Русский"]) == :ok
    end

    test "rejects invalid UTF-8 sequences" do
      _layer = Layer.new(MockRepo)

      # Invalid UTF-8 bytes (avoiding reserved prefixes 0xFE and 0xFF)
      assert {:error, :invalid_utf8_in_path} = Layer.validate_path([<<0x80>>])
      assert {:error, :invalid_utf8_in_path} = Layer.validate_path([<<0xC0, 0x80>>])
      assert {:error, :invalid_utf8_in_path} = Layer.validate_path([<<0xF5, 0x80, 0x80, 0x80>>])

      # Incomplete UTF-8 sequences
      assert {:error, :invalid_utf8_in_path} = Layer.validate_path([<<0xC2>>])
      assert {:error, :invalid_utf8_in_path} = Layer.validate_path([<<0xE0, 0xA0>>])
    end
  end

  describe "input format normalization" do
    test "accepts list of binaries" do
      _layer = Layer.new(MockRepo)

      assert Layer.validate_path(["dir1", "dir2", "dir3"]) == :ok
      assert Layer.validate_path([]) == :ok
    end

    test "accepts single binary as single-element path" do
      _layer = Layer.new(MockRepo)

      assert Layer.validate_path("single") == :ok
    end

    test "accepts tuple and converts to list" do
      _layer = Layer.new(MockRepo)

      assert Layer.validate_path({"dir1", "dir2"}) == :ok
      assert Layer.validate_path({"single"}) == :ok
      assert Layer.validate_path({}) == :ok
    end

    test "rejects invalid formats" do
      _layer = Layer.new(MockRepo)

      assert {:error, :invalid_path_format} = Layer.validate_path(123)
      assert {:error, :invalid_path_format} = Layer.validate_path(:atom)
      assert {:error, :invalid_path_format} = Layer.validate_path(%{})
    end
  end

  describe "special character validation" do
    test "rejects null bytes in path components" do
      _layer = Layer.new(MockRepo)

      assert {:error, :null_byte_in_path} = Layer.validate_path(["hello\x00world"])
      assert {:error, :null_byte_in_path} = Layer.validate_path(["\x00"])
      assert {:error, :null_byte_in_path} = Layer.validate_path(["valid", "also\x00invalid"])
    end

    test "rejects empty path components" do
      _layer = Layer.new(MockRepo)

      assert {:error, :empty_path_component} = Layer.validate_path([""])
      assert {:error, :empty_path_component} = Layer.validate_path(["valid", "", "invalid"])
    end

    test "rejects reserved prefixes in path components" do
      _layer = Layer.new(MockRepo)

      assert {:error, :reserved_prefix_in_path} = Layer.validate_path([<<0xFE, 0x01>>])
      assert {:error, :reserved_prefix_in_path} = Layer.validate_path([<<0xFF, 0x00>>])
      assert {:error, :reserved_prefix_in_path} = Layer.validate_path(["valid", <<0xFE>>])
    end

    test "rejects . and .. as directory names" do
      _layer = Layer.new(MockRepo)

      assert {:error, :invalid_directory_name} = Layer.validate_path(["."])
      assert {:error, :invalid_directory_name} = Layer.validate_path([".."])
      assert {:error, :invalid_directory_name} = Layer.validate_path(["valid", ".", "invalid"])
      assert {:error, :invalid_directory_name} = Layer.validate_path(["valid", "..", "invalid"])
    end
  end

  describe "mixed validation" do
    test "validates all components in path" do
      _layer = Layer.new(MockRepo)

      # First component invalid
      assert {:error, :empty_path_component} = Layer.validate_path(["", "valid"])

      # Middle component invalid
      assert {:error, :null_byte_in_path} = Layer.validate_path(["valid1", "inv\x00alid", "valid2"])

      # Last component invalid
      assert {:error, :invalid_directory_name} = Layer.validate_path(["valid1", "valid2", ".."])
    end

    test "accepts complex valid paths" do
      _layer = Layer.new(MockRepo)

      assert Layer.validate_path(["users", "profiles", "settings", "preferences"]) == :ok
      assert Layer.validate_path(["data", "2024", "01", "metrics"]) == :ok
      assert Layer.validate_path(["app", "v1.0.0", "config"]) == :ok
    end
  end

  describe "non-binary components" do
    test "rejects non-binary components in list" do
      _layer = Layer.new(MockRepo)

      assert {:error, :invalid_path_component} = Layer.validate_path([:atom, "valid"])
      assert {:error, :invalid_path_component} = Layer.validate_path(["valid", 123])
      assert {:error, :invalid_path_component} = Layer.validate_path([nil])
    end
  end
end
