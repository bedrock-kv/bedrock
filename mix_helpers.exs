# Shared helpers for umbrella app mix.exs files.
#
# Usage in app mix.exs:
#   Code.compile_file("mix_helpers.exs", Path.join(__DIR__, "../.."))
#   use Bedrock.MixHelpers, app: :bedrock_shale
#
#   defp deps do
#     [umbrella_dep(:bedrock_core), {:mox, "~> 1.1", only: :test}]
#   end

if !Code.ensure_loaded?(Bedrock.MixHelpers) do
  defmodule Bedrock.MixHelpers do
    @moduledoc false

    {versions, _} = Code.eval_file(Path.join(__DIR__, "versions.exs"))
    @versions versions

    defmacro __using__(opts) do
      app = Keyword.fetch!(opts, :app)
      version = @versions[app]

      quote do
        import Bedrock.MixHelpers

        @version unquote(version)
      end
    end

    def umbrella_dep(app) when is_atom(app) do
      if System.get_env("HEX_PUBLISHING") do
        {app, "~> #{@versions[app]}"}
      else
        {app, in_umbrella: true}
      end
    end
  end
end
