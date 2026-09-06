defmodule Bedrock.Service.RecoveryControl do
  @moduledoc false

  alias Bedrock.DataPlane.Log.Shale.Segment
  alias Bedrock.DataPlane.Log.Shale.WalFormat
  alias Bedrock.DataPlane.Version
  alias Bedrock.Service.Manifest
  alias Bedrock.Service.RecoveryAuthority

  @file_name ".recovery-authority-v1"
  @magic "BRRA1"
  @version 1

  @enforce_keys [:creation, :phase]
  defstruct [:creation, :phase, :authority, :replay_after, :last_inclusive, :wal_identity, :unlock_intent]

  @type creation :: %{cluster: binary(), service_id: binary(), worker: binary()}
  @type phase :: :no_grant | :locked | :replay_started | :replay_complete | :running
  @type t :: %__MODULE__{
          creation: creation(),
          phase: phase(),
          authority: RecoveryAuthority.t() | nil,
          replay_after: binary() | nil,
          last_inclusive: binary() | nil,
          wal_identity: term(),
          unlock_intent: binary() | nil
        }

  @spec path(Path.t()) :: Path.t()
  def path(worker_path), do: Path.join(worker_path, @file_name)

  @spec validate_artifacts(Path.t()) :: :ok | {:error, {:recovery_authority, term()}}
  def validate_artifacts(worker_path) do
    with :ok <- reject_symlink(worker_path, :worker_directory_is_symlink),
         :ok <- reject_symlink(Path.join(worker_path, "manifest.json"), :manifest_is_symlink) do
      reject_symlink(path(worker_path), :control_record_is_symlink)
    end
  end

  @spec creation(module() | binary(), binary(), module()) :: creation()
  def creation(cluster, service_id, worker) do
    cluster_name = if is_atom(cluster), do: cluster.name(), else: cluster
    %{cluster: cluster_name, service_id: service_id, worker: worker_name(worker)}
  end

  @spec no_grant(module() | binary(), binary(), module()) :: t()
  def no_grant(cluster, service_id, worker),
    do: %__MODULE__{creation: creation(cluster, service_id, worker), phase: :no_grant}

  @spec locked(t(), RecoveryAuthority.input()) :: t()
  def locked(%__MODULE__{} = record, authority),
    do: %{
      record
      | phase: :locked,
        authority: RecoveryAuthority.new!(authority),
        replay_after: nil,
        last_inclusive: nil,
        wal_identity: nil,
        unlock_intent: nil
    }

  @spec replay_started(t(), RecoveryAuthority.input(), binary(), binary()) :: t()
  def replay_started(%__MODULE__{} = record, authority, replay_after, last_inclusive),
    do: replay_started(record, authority, replay_after, last_inclusive, nil)

  @spec replay_started(t(), RecoveryAuthority.input(), binary(), binary(), binary() | nil) :: t()
  def replay_started(%__MODULE__{} = record, authority, replay_after, last_inclusive, unlock_intent),
    do: %{
      record
      | phase: :replay_started,
        authority: RecoveryAuthority.new!(authority),
        replay_after: replay_after,
        last_inclusive: last_inclusive,
        wal_identity: nil,
        unlock_intent: unlock_intent
    }

  @spec replay_complete(t(), map()) :: t()
  def replay_complete(%__MODULE__{} = record, wal_identity),
    do: %{record | phase: :replay_complete, wal_identity: wal_identity}

  @spec running(t()) :: t()
  def running(%__MODULE__{} = record), do: %{record | phase: :running}

  @spec unlock_intent(binary(), list()) :: binary()
  def unlock_intent(durable_version, pull_sources)
      when is_binary(durable_version) and byte_size(durable_version) == 8 and is_list(pull_sources),
      do: :crypto.hash(:sha256, :erlang.term_to_binary({durable_version, pull_sources}, [:deterministic]))

  @spec wal_identity(Path.t(), binary(), keyword()) :: {:ok, map()} | {:error, term()}
  def wal_identity(worker_path, last_version, opts \\ []) do
    allow_suffix? = Keyword.get(opts, :allow_suffix, true)

    with {:ok, names} <- File.ls(worker_path),
         {:ok, wal_names} <- validate_wal_names(worker_path, names, last_version, allow_suffix?),
         {:ok, digest} <- digest_wal_prefix(worker_path, wal_names, last_version, allow_suffix?) do
      {:ok, %{last_version: last_version, files_digest: digest}}
    end
  end

  defp validate_wal_names(path, names, last_version, allow_suffix?) do
    endpoint = Version.to_integer(last_version)

    names
    |> Enum.filter(&String.starts_with?(&1, Segment.file_prefix()))
    |> Enum.sort()
    |> Enum.reduce_while({:ok, []}, fn name, {:ok, acc} ->
      file_path = Path.join(path, name)

      with {:ok, %{type: :regular}} <- File.lstat(file_path),
           {:ok, start_version} <- decode_wal_name(name) do
        cond do
          start_version <= endpoint -> {:cont, {:ok, [name | acc]}}
          allow_suffix? -> {:cont, {:ok, acc}}
          true -> {:halt, {:error, :unexpected_wal_suffix}}
        end
      else
        {:ok, _} -> {:halt, {:error, :unsafe_wal_file}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, relevant} -> {:ok, Enum.reverse(relevant)}
      error -> error
    end
  end

  defp decode_wal_name(name) do
    {:ok, Segment.decode_file_name(name)}
  rescue
    _ -> {:error, :invalid_wal_filename}
  end

  defp digest_wal_prefix(path, names, last_version, allow_suffix?) do
    names
    |> Enum.reduce_while({:ok, :crypto.hash_init(:sha256)}, fn name, {:ok, hash} ->
      file_path = Path.join(path, name)

      with {:ok, %{type: :regular}} <- File.lstat(file_path),
           {:ok, bytes} <- File.read(file_path),
           {:ok, format, entries} <- WalFormat.split(bytes),
           {:ok, prefix} <- wal_entries_through(entries, last_version, [], allow_suffix?) do
        hash = :crypto.hash_update(hash, [name, <<0>>, format.previous_version, prefix])
        {:cont, {:ok, hash}}
      else
        {:ok, _} -> {:halt, {:error, :unsafe_wal_file}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, hash} -> {:ok, :crypto.hash_final(hash)}
      error -> error
    end
  end

  defp wal_entries_through(
         <<version::binary-size(8), size::unsigned-big-32, payload::binary-size(size), crc::unsigned-big-32,
           rest::binary>>,
         last_version,
         acc,
         allow_suffix?
       ) do
    cond do
      WalFormat.eof_version?(version) ->
        {:ok, IO.iodata_to_binary(Enum.reverse(acc))}

      :erlang.crc32(payload) != crc ->
        {:error, :einval}

      version <= last_version ->
        wal_entries_through(rest, last_version, [[version, <<size::32>>, payload, <<crc::32>>] | acc], allow_suffix?)

      allow_suffix? ->
        {:ok, IO.iodata_to_binary(Enum.reverse(acc))}

      true ->
        {:error, :unexpected_wal_suffix}
    end
  end

  defp wal_entries_through(_, _last_version, _acc, _allow_suffix?), do: {:error, :einval}

  @spec data_identity(Path.t(), binary()) :: {:ok, map()} | {:error, File.posix()}
  def data_identity(worker_path, last_version), do: identity(worker_path, last_version, ["data", "idx"])

  defp identity(worker_path, last_version, names) do
    with {:ok, present} <- existing_names(worker_path, names),
         {:ok, digest} <- digest_files(worker_path, present) do
      {:ok, %{last_version: last_version, files_digest: digest}}
    end
  end

  defp existing_names(path, names) do
    names
    |> Enum.reduce_while({:ok, []}, fn name, {:ok, acc} ->
      case File.lstat(Path.join(path, name)) do
        {:ok, %{type: :regular}} -> {:cont, {:ok, [name | acc]}}
        {:ok, %{type: :symlink}} -> {:halt, {:error, :eloop}}
        {:ok, _} -> {:halt, {:error, :einval}}
        {:error, :enoent} -> {:cont, {:ok, acc}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, present} -> {:ok, Enum.sort(present)}
      error -> error
    end
  end

  defp digest_files(path, names) do
    names
    |> Enum.reduce_while({:ok, :crypto.hash_init(:sha256)}, fn name, {:ok, hash} ->
      file_path = Path.join(path, name)

      with {:ok, %{type: :regular}} <- File.lstat(file_path),
           {:ok, bytes} <- File.read(file_path) do
        {:cont, {:ok, :crypto.hash_update(hash, [name, <<0>>, bytes])}}
      else
        {:ok, _} -> {:halt, {:error, :einval}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, hash} -> {:ok, :crypto.hash_final(hash)}
      error -> error
    end
  end

  @spec validate_creation(t(), module() | binary(), binary(), module()) :: :ok | {:error, :creation_identity_mismatch}
  def validate_creation(%__MODULE__{creation: creation}, cluster, service_id, worker) do
    if creation == creation(cluster, service_id, worker), do: :ok, else: {:error, :creation_identity_mismatch}
  end

  @spec encode(t(), pos_integer()) :: binary()
  def encode(%__MODULE__{} = record, version \\ @version) do
    payload = {version, record}
    bytes = :erlang.term_to_binary(payload, [:deterministic])
    @magic <> :crypto.hash(:sha256, bytes) <> bytes
  end

  @spec decode(binary()) :: {:ok, t()} | {:error, :corrupt | :future_version}
  def decode(<<@magic, checksum::binary-size(32), bytes::binary>>) do
    with true <- secure_equal(checksum, :crypto.hash(:sha256, bytes)),
         {@version, %__MODULE__{} = record} <- :erlang.binary_to_term(bytes, [:safe]),
         :ok <- validate(record) do
      {:ok, record}
    else
      {version, _} when is_integer(version) and version > @version -> {:error, :future_version}
      _ -> {:error, :corrupt}
    end
  rescue
    _ -> {:error, :corrupt}
  end

  def decode(_), do: {:error, :corrupt}

  @spec load(Path.t()) :: {:ok, t()} | {:error, :missing | :corrupt | :future_version | File.posix()}
  def load(worker_path) do
    case File.read(path(worker_path)) do
      {:ok, bytes} -> decode(bytes)
      {:error, :enoent} -> {:error, :missing}
      {:error, reason} -> {:error, reason}
    end
  end

  @spec validate_prepared(Path.t(), module() | binary(), binary(), module()) ::
          {:ok, t()} | {:error, {:recovery_authority, term()}}
  def validate_prepared(worker_path, cluster, service_id, worker) do
    with :ok <- validate_artifacts(worker_path),
         {:ok, manifest} <- load_manifest(worker_path),
         :ok <- validate_manifest(manifest, service_id, worker),
         {:ok, record} <- load_marked_record(worker_path),
         true <- manifest.cluster == cluster_name(cluster) || {:error, :creation_identity_mismatch},
         :ok <- validate_creation(record, manifest.cluster, service_id, worker) do
      {:ok, record}
    else
      {:error, {:recovery_authority, _} = reason} -> {:error, reason}
      {:error, :creation_identity_mismatch} -> {:error, {:recovery_authority, :creation_identity_mismatch}}
      _ -> {:error, {:recovery_authority, :unprepared_worker_directory}}
    end
  end

  defp load_manifest(worker_path) do
    case Manifest.load_from_file(Path.join(worker_path, "manifest.json")) do
      {:ok, manifest} -> {:ok, manifest}
      _ -> {:error, {:recovery_authority, :unprepared_worker_directory}}
    end
  end

  defp validate_manifest(%Manifest{id: id, worker: worker, params: %{"recovery_authority_protocol" => 1}}, id, worker),
    do: :ok

  defp validate_manifest(%Manifest{params: %{"recovery_authority_protocol" => version}}, _id, _worker)
       when is_integer(version) and version > 1, do: {:error, {:recovery_authority, :future_protocol}}

  defp validate_manifest(_, _, _), do: {:error, {:recovery_authority, :unprepared_worker_directory}}

  defp load_marked_record(worker_path) do
    case load(worker_path) do
      {:ok, record} -> {:ok, record}
      {:error, :missing} -> {:error, {:recovery_authority, :missing_after_migration}}
      {:error, reason} -> {:error, {:recovery_authority, reason}}
    end
  end

  @spec write(Path.t(), t()) :: :ok | {:error, term()}
  def write(worker_path, %__MODULE__{} = record) do
    with :ok <- validate(record),
         :ok <- reject_symlink(worker_path, :worker_directory_is_symlink),
         :ok <- reject_symlink(path(worker_path), :control_record_is_symlink) do
      atomic_write(path(worker_path), encode(record))
    end
  end

  @spec atomic_write(Path.t(), iodata()) :: :ok | {:error, term()}
  def atomic_write(path, bytes) do
    scratch = path <> ".tmp.#{System.unique_integer([:positive])}"

    case :file.open(String.to_charlist(scratch), [:write, :binary, :raw, :exclusive]) do
      {:ok, file} ->
        result =
          try do
            with :ok <- :file.write(file, bytes) do
              :file.sync(file)
            end
          after
            _ = :file.close(file)
          end

        with :ok <- result,
             :ok <- File.rename(scratch, path) do
          case sync_directory(Path.dirname(path)) do
            :ok -> :ok
            {:error, reason} -> {:error, {:post_publish_sync_failed, reason}}
          end
        else
          {:error, _reason} = error ->
            _ = File.rm(scratch)
            error
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp sync_directory(path) do
    case :file.open(String.to_charlist(path), [:read, :raw, :directory]) do
      {:ok, file} ->
        result = :file.sync(file)
        _ = :file.close(file)
        result

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp validate(%__MODULE__{creation: creation, phase: phase} = record) do
    if valid_creation?(creation) and phase in [:no_grant, :locked, :replay_started, :replay_complete, :running] do
      validate_phase(record)
    else
      {:error, :corrupt}
    end
  end

  defp validate(_), do: {:error, :corrupt}

  defp valid_creation?(%{cluster: cluster, service_id: id, worker: worker}) do
    valid_identity?(cluster) and valid_identity?(id) and valid_identity?(worker)
  end

  defp valid_creation?(_), do: false

  defp valid_identity?(value), do: is_binary(value) and byte_size(value) > 0

  defp validate_phase(%__MODULE__{
         phase: :no_grant,
         authority: nil,
         replay_after: nil,
         last_inclusive: nil,
         wal_identity: nil,
         unlock_intent: nil
       }), do: :ok

  defp validate_phase(%__MODULE__{
         phase: :locked,
         authority: authority,
         replay_after: nil,
         last_inclusive: nil,
         wal_identity: nil,
         unlock_intent: nil
       }), do: authority |> RecoveryAuthority.new() |> normalize_validation()

  defp validate_phase(%__MODULE__{
         phase: :replay_started,
         authority: authority,
         replay_after: from,
         last_inclusive: to,
         wal_identity: nil,
         unlock_intent: unlock_intent
       }) do
    validate_replay_fields(authority, from, to, unlock_intent, true)
  end

  defp validate_phase(%__MODULE__{
         phase: :replay_complete,
         authority: authority,
         replay_after: from,
         last_inclusive: to,
         wal_identity: %{last_version: last, files_digest: digest},
         unlock_intent: unlock_intent
       }) do
    with :ok <- validate_replay_fields(authority, from, to, unlock_intent, true),
         true <- last == to and valid_digest?(digest) do
      :ok
    else
      _ -> {:error, :corrupt}
    end
  end

  # Once running, compaction and window advancement can temporarily move the
  # physical durable floor behind the recovery boundary while rebuilding the
  # database. The identity still binds the exact bytes and their exact version;
  # ordering against replay_after is no longer a valid invariant.
  defp validate_phase(%__MODULE__{
         phase: :running,
         authority: authority,
         replay_after: from,
         last_inclusive: to,
         wal_identity: %{last_version: last, files_digest: digest},
         unlock_intent: unlock_intent
       }) do
    with :ok <- validate_replay_fields(authority, from, to, unlock_intent, false),
         true <- last == to and valid_digest?(digest) do
      :ok
    else
      _ -> {:error, :corrupt}
    end
  end

  defp validate_phase(_), do: {:error, :corrupt}

  defp validate_replay_fields(authority, from, to, unlock_intent, ordered?) do
    valid_versions? = valid_version?(from) and valid_version?(to)
    valid_order? = not ordered? or from <= to

    if valid_versions? and valid_order? and valid_unlock_intent?(unlock_intent) do
      authority |> RecoveryAuthority.new() |> normalize_validation()
    else
      {:error, :corrupt}
    end
  end

  defp valid_version?(version), do: is_binary(version) and byte_size(version) == 8
  defp valid_digest?(digest), do: is_binary(digest) and byte_size(digest) == 32
  defp valid_unlock_intent?(nil), do: true
  defp valid_unlock_intent?(intent), do: is_binary(intent) and byte_size(intent) == 32

  defp normalize_validation({:ok, _}), do: :ok
  defp normalize_validation({:error, _}), do: {:error, :corrupt}

  defp worker_name(worker), do: worker |> Module.split() |> Enum.join(".")
  defp cluster_name(cluster) when is_atom(cluster), do: cluster.name()
  defp cluster_name(cluster), do: cluster

  defp reject_symlink(path, reason) do
    case File.lstat(path) do
      {:ok, %{type: :symlink}} -> {:error, {:recovery_authority, reason}}
      {:ok, _} -> :ok
      {:error, :enoent} -> :ok
      {:error, file_reason} -> {:error, {:recovery_authority, {:unable_to_inspect_path, file_reason}}}
    end
  end

  defp secure_equal(left, right), do: :crypto.hash_equals(left, right)
end
