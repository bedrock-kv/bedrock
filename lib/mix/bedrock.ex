defmodule Mix.Bedrock do
  @moduledoc """
  Convenience functions for writing Bedrock related Mix tasks.
  """

  @spec parse_clusters([module_name :: String.t()]) :: [Bedrock.Cluster.t()]
  def parse_clusters([]) do
    Mix.Task.run("app.config")

    apps =
      if apps_paths = Mix.Project.apps_paths() do
        Enum.filter(Mix.Project.deps_apps(), &is_map_key(apps_paths, &1))
      else
        [Mix.Project.config()[:app]]
      end

    apps
    |> Enum.flat_map(fn app ->
      Application.load(app)
      Application.get_env(app, :bedrock_clusters, [])
    end)
    |> Enum.uniq()
    |> case do
      [] ->
        Mix.shell().error("""
        warning: could not find Bedrock clusters in any of the apps: #{inspect(apps)}.

        You can avoid this warning by passing the -r flag or by setting the
        clusters managed by those applications in your config/config.exs:

            config #{inspect(hd(apps))}, bedrock_clusters: [...]
        """)

        []

      clusters ->
        clusters
    end
  end

  @spec parse_clusters([module_name :: String.t()]) :: [Bedrock.Cluster.t()]
  def parse_clusters(module_names) do
    Mix.Task.run("app.config")

    for module_name <- module_names do
      [module_name]
      |> Module.concat()
      |> ensure_module_is_a_cluster!()
    end
  end

  @spec ensure_module_is_a_cluster!(module()) :: Bedrock.Cluster.t()
  defp ensure_module_is_a_cluster!(module) do
    module
    |> Code.ensure_compiled()
    |> check_compiled_correctly!(module)
    |> check_implements_behaviour!()
  end

  @spec check_compiled_correctly!({:module, module} | {:error, term()}, module()) :: module()
  defp check_compiled_correctly!({:module, module}, _module), do: module

  @spec check_compiled_correctly!({:module, module} | {:error, term()}, module()) :: module()
  defp check_compiled_correctly!({:error, error}, module) do
    Mix.raise(
      "Could not load #{inspect(module)}, error: #{inspect(error)}. " <>
        "Please configure your app accordingly or pass a cluster with the -c option."
    )
  end

  @spec check_implements_behaviour!(module()) :: Bedrock.Cluster.t()
  defp check_implements_behaviour!(module) do
    unless implements?(module, Bedrock.Cluster) do
      Mix.raise(
        "Module #{inspect(module)} is not a Bedrock.Cluster. " <>
          "Please configure your app accordingly or pass a cluster with the -c option."
      )
    end

    module
  end

  @spec implements?(module(), behaviour :: atom()) :: boolean()
  defp implements?(module, behaviour) do
    module.__info__(:attributes)
    |> Keyword.get(:behaviour, [])
    |> Enum.member?(behaviour)
  end
end
