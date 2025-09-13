defmodule Bedrock.Directory.PathValidationTest do
  use ExUnit.Case, async: true

  import Mox

  alias Bedrock.Directory

  setup :verify_on_exit!

  describe "UTF-8 validation" do
    test "accepts valid UTF-8 strings" do
      _layer = Directory.root(MockRepo)

      valid_paths = [
        # English
        ["hello", "world"],
        # Unicode characters
        ["café", "naïve"],
        ["日本語", "中文"],
        ["emoji", "🚀"],
        ["Ελληνικά", "Русский"]
      ]

      for path <- valid_paths do
        assert Directory.validate_path(path) == :ok
      end
    end

    test "rejects invalid UTF-8 sequences" do
      _layer = Directory.root(MockRepo)

      invalid_sequences = [
        # Invalid UTF-8 bytes (avoiding reserved prefixes 0xFE and 0xFF)
        [<<0x80>>],
        [<<0xC0, 0x80>>],
        [<<0xF5, 0x80, 0x80, 0x80>>],
        # Incomplete UTF-8 sequences
        [<<0xC2>>],
        [<<0xE0, 0xA0>>]
      ]

      for sequence <- invalid_sequences do
        assert {:error, :invalid_utf8_in_path} = Directory.validate_path(sequence)
      end
    end
  end

  describe "input format normalization" do
    test "accepts list of binaries" do
      _layer = Directory.root(MockRepo)

      assert Directory.validate_path(["dir1", "dir2", "dir3"]) == :ok
      assert Directory.validate_path([]) == :ok
    end

    test "accepts single binary as single-element path" do
      _layer = Directory.root(MockRepo)

      assert Directory.validate_path("single") == :ok
    end

    test "accepts tuple and converts to list" do
      _layer = Directory.root(MockRepo)

      valid_tuples = [
        {"dir1", "dir2"},
        {"single"},
        {}
      ]

      for tuple <- valid_tuples do
        assert Directory.validate_path(tuple) == :ok
      end
    end

    test "rejects invalid formats" do
      _layer = Directory.root(MockRepo)

      invalid_formats = [123, :atom, %{}]

      for format <- invalid_formats do
        assert {:error, :invalid_path_format} = Directory.validate_path(format)
      end
    end
  end

  describe "special character validation" do
    test "rejects null bytes in path components" do
      _layer = Directory.root(MockRepo)

      null_byte_paths = [
        ["hello\x00world"],
        ["\x00"],
        ["valid", "also\x00invalid"]
      ]

      for path <- null_byte_paths do
        assert {:error, :null_byte_in_path} = Directory.validate_path(path)
      end
    end

    test "rejects empty path components" do
      _layer = Directory.root(MockRepo)

      empty_component_paths = [
        [""],
        ["valid", "", "invalid"]
      ]

      for path <- empty_component_paths do
        assert {:error, :empty_path_component} = Directory.validate_path(path)
      end
    end

    test "rejects reserved prefixes in path components" do
      _layer = Directory.root(MockRepo)

      reserved_prefix_paths = [
        [<<0xFE, 0x01>>],
        [<<0xFF, 0x00>>],
        ["valid", <<0xFE>>]
      ]

      for path <- reserved_prefix_paths do
        assert {:error, :reserved_prefix_in_path} = Directory.validate_path(path)
      end
    end

    test "rejects . and .. as directory names" do
      _layer = Directory.root(MockRepo)

      invalid_directory_paths = [
        ["."],
        [".."],
        ["valid", ".", "invalid"],
        ["valid", "..", "invalid"]
      ]

      for path <- invalid_directory_paths do
        assert {:error, :invalid_directory_name} = Directory.validate_path(path)
      end
    end
  end

  describe "mixed validation" do
    test "validates all components in path" do
      _layer = Directory.root(MockRepo)

      # Test different error positions
      test_cases = [
        {["", "valid"], :empty_path_component},
        {["valid1", "inv\x00alid", "valid2"], :null_byte_in_path},
        {["valid1", "valid2", ".."], :invalid_directory_name}
      ]

      for {path, expected_error} <- test_cases do
        assert {:error, ^expected_error} = Directory.validate_path(path)
      end
    end

    test "accepts complex valid paths" do
      _layer = Directory.root(MockRepo)

      valid_complex_paths = [
        ["users", "profiles", "settings", "preferences"],
        ["data", "2024", "01", "metrics"],
        ["app", "v1.0.0", "config"]
      ]

      for path <- valid_complex_paths do
        assert Directory.validate_path(path) == :ok
      end
    end
  end

  describe "non-binary components" do
    test "rejects non-binary components in list" do
      _layer = Directory.root(MockRepo)

      non_binary_paths = [
        [:atom, "valid"],
        ["valid", 123],
        [nil]
      ]

      for path <- non_binary_paths do
        assert {:error, :invalid_path_component} = Directory.validate_path(path)
      end
    end
  end
end
