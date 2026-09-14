defmodule Mix.Tasks.Bedrock.DumpStorageTest do
  use ExUnit.Case, async: false

  @moduletag :tmp_dir

  test "the task receives the reporter-bearing Olivine startup health and dumps storage", %{tmp_dir: path} do
    storage = Path.join(path, "storage")
    output = Path.join(path, "dump.json")
    File.mkdir_p!(storage)

    # The task deliberately halts on errors, so exercise it in an isolated VM.
    {diagnostics, status} =
      System.cmd(
        "elixir",
        [
          "--erl",
          "+S 2:2",
          "-S",
          "mix",
          "bedrock.dump_storage",
          "--path",
          storage,
          "--format",
          "json",
          "--output",
          output
        ],
        stderr_to_stdout: true
      )

    assert status == 0, diagnostics
    assert Jason.decode!(File.read!(output)) == %{"count" => 0, "entries" => []}
  end
end
