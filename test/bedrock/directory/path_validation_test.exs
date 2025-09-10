defmodule Bedrock.Directory.PathValidationTest do
  use ExUnit.Case, async: true

  alias Bedrock.Directory.Layer

  describe "validate_path/1" do
    test "accepts empty list as root path" do
      assert Layer.validate_path([]) == :ok
    end

    test "accepts list of binary strings" do
      assert Layer.validate_path(["users"]) == :ok
      assert Layer.validate_path(["users", "profiles"]) == :ok
      assert Layer.validate_path(["a", "b", "c", "d", "e"]) == :ok
    end

    test "accepts single binary string" do
      assert Layer.validate_path("users") == :ok
    end

    test "accepts tuple of strings" do
      assert Layer.validate_path({"users"}) == :ok
      assert Layer.validate_path({"users", "profiles"}) == :ok
    end

    test "rejects non-binary components in list" do
      assert Layer.validate_path([123]) == {:error, :invalid_path_component}
      assert Layer.validate_path([:atom]) == {:error, :invalid_path_component}
      assert Layer.validate_path([nil]) == {:error, :invalid_path_component}
      assert Layer.validate_path(["valid", 123]) == {:error, :invalid_path_component}
    end

    test "rejects invalid UTF-8 in path components" do
      # Invalid UTF-8 sequence (partial multi-byte character)
      # Overlong encoding of NULL
      invalid_utf8 = <<0xC0, 0x80>>
      assert Layer.validate_path([invalid_utf8]) == {:error, :invalid_utf8_in_path}

      # Another invalid UTF-8 sequence
      # Continuation byte without start byte
      invalid_utf8_2 = <<0x80>>
      assert Layer.validate_path(["valid", invalid_utf8_2]) == {:error, :invalid_utf8_in_path}
    end

    test "rejects empty path components" do
      assert Layer.validate_path([""]) == {:error, :empty_path_component}
      assert Layer.validate_path(["users", ""]) == {:error, :empty_path_component}
      assert Layer.validate_path(["", "users"]) == {:error, :empty_path_component}
    end

    test "rejects null bytes in path components" do
      assert Layer.validate_path(["user\x00name"]) == {:error, :null_byte_in_path}
      assert Layer.validate_path(["\x00"]) == {:error, :null_byte_in_path}
      assert Layer.validate_path(["valid", "invalid\x00"]) == {:error, :null_byte_in_path}
    end

    test "rejects reserved prefix bytes in path components" do
      # 0xFE is reserved for node subspace
      assert Layer.validate_path([<<0xFE, "test">>]) == {:error, :reserved_prefix_in_path}
      # 0xFF is reserved for system keys
      assert Layer.validate_path([<<0xFF, "test">>]) == {:error, :reserved_prefix_in_path}
      assert Layer.validate_path(["valid", <<0xFE>>]) == {:error, :reserved_prefix_in_path}
    end

    test "rejects invalid input types" do
      assert Layer.validate_path(123) == {:error, :invalid_path_format}
      assert Layer.validate_path(:atom) == {:error, :invalid_path_format}
      assert Layer.validate_path(%{}) == {:error, :invalid_path_format}
    end

    test "accepts valid Unicode characters" do
      assert Layer.validate_path(["用户"]) == :ok
      assert Layer.validate_path(["émoji"]) == :ok
      assert Layer.validate_path(["🎉"]) == :ok
      assert Layer.validate_path(["مستخدم"]) == :ok
      assert Layer.validate_path(["пользователь"]) == :ok
    end

    test "accepts paths with special but valid characters" do
      assert Layer.validate_path(["user-name"]) == :ok
      assert Layer.validate_path(["user_name"]) == :ok
      assert Layer.validate_path(["user.name"]) == :ok
      assert Layer.validate_path(["user@example.com"]) == :ok
      assert Layer.validate_path(["path/with/slashes"]) == :ok
    end
  end
end
