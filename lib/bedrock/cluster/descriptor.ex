defmodule Bedrock.Cluster.Descriptor do
  @moduledoc """
  A module for reading and writing cluster descriptors to and from files.

  A cluster descriptor is a file that contains the name of the cluster, the id
  of the cluster, and the names of the coordinator nodes in the cluster.
  """

  defstruct ~w[cluster_name id coordinator_nodes]a

  @type t :: %__MODULE__{
          cluster_name: String.t(),
          coordinator_nodes: [atom()]
        }

  def new(cluster_name, coordinator_nodes),
    do: %__MODULE__{cluster_name: cluster_name, coordinator_nodes: coordinator_nodes}

  @doc """
  Writes the cluster descriptor to a file.
  """
  @spec write_to_file(path_to_file :: Path.t(), %__MODULE__{}) :: :ok
  def write_to_file(path_to_file, %__MODULE__{} = t) do
    file_contents = "#{t.cluster_name}:#{Enum.join(",", t.coordinator_nodes)}"
    File.write!(path_to_file, file_contents, [:write, :utf8])
  end

  @doc """
  Reads the cluster descriptor from a file. Raises an exception if an error
  was encountered.
  """
  @spec read_from_file!(path_to_file :: Path.t()) :: t()
  def read_from_file!(path_to_file) do
    read_from_file(path_to_file)
    |> case do
      {:ok, descriptor} -> descriptor
      {:error, reason} -> raise "Unable to read cluster descriptor: #{inspect(reason)}"
    end
  end

  @doc """
  Reads the cluster descriptor from a file.
  """
  @spec read_from_file(path_to_file :: Path.t()) ::
          {:ok, t()} | {:error, :unable_to_read_file | :invalid_cluster_descriptor}
  def read_from_file(path_to_file) do
    path_to_file
    |> File.read()
    |> case do
      {:ok, file_contents} ->
        file_contents
        |> parse_cluster_file_contents()
        |> case do
          {:error, _reason} = error -> error
          {cluster_name, coordinator_nodes} -> {:ok, new(cluster_name, coordinator_nodes)}
        end

      {:error, _reason} ->
        {:error, :unable_to_read_file}
    end
  end

  def parse_cluster_file_contents(contents),
    do: contents |> String.split(":", trim: true, parts: 2) |> parse_cluster_name_and_rest()

  defp parse_cluster_name_and_rest([cluster_name, joined_coordinator_nodes]),
    do:
      parse_joined_cluster_nodes(
        cluster_name,
        joined_coordinator_nodes |> String.replace("\n", "") |> String.split(",", trim: true)
      )

  defp parse_cluster_name_and_rest(_),
    do: {:error, :invalid_cluster_descriptor}

  defp parse_joined_cluster_nodes(_cluster_name, []),
    do: {:error, :invalid_cluster_descriptor}

  defp parse_joined_cluster_nodes(cluster_name, joined_coordinator_nodes),
    do: {cluster_name, joined_coordinator_nodes |> Enum.map(&String.to_atom/1)}
end
