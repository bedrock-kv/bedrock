defmodule Bedrock.ObjectStorage.LocalFilesystemAtomicityTest do
  use ExUnit.Case, async: true

  alias Bedrock.ObjectStorage
  alias Bedrock.ObjectStorage.LocalFilesystem

  # Readers of this store are written against S3's contract: one atomic
  # PUT, so an object is complete or absent, and put-if-not-exists claims
  # a key only by publishing a whole object. A local backend that creates
  # the key first and fills it second can strand a permanently short
  # object under a key nothing may ever rewrite — put_if_not_exists is
  # the only writer for both chunks and snapshots, and both callers read
  # :already_exists as success.

  setup do
    root = Path.join(System.tmp_dir!(), "atomicity_test_#{:erlang.unique_integer([:positive])}")
    File.mkdir_p!(root)
    on_exit(fn -> File.rm_rf!(root) end)

    {:ok, backend: ObjectStorage.backend(LocalFilesystem, root: root), root: root}
  end

  defp all_files(root) do
    root
    |> Path.join("**")
    |> Path.wildcard(match_dot: true)
    |> Enum.filter(&File.regular?/1)
    |> Enum.map(&Path.relative_to(&1, root))
    |> Enum.sort()
  end

  describe "no intermediate state is observable" do
    test "a completed put leaves exactly one file", %{backend: backend, root: root} do
      :ok = ObjectStorage.put(backend, "c/0/obj", "payload")

      assert all_files(root) == ["c/0/obj"],
             "scratch files must not survive a successful put"
    end

    test "a completed put_if_not_exists leaves exactly one file", %{backend: backend, root: root} do
      :ok = ObjectStorage.put_if_not_exists(backend, "c/0/obj", "payload")

      assert all_files(root) == ["c/0/obj"]
    end

    test "scratch files are never visible to list/3", %{backend: backend, root: root} do
      :ok = ObjectStorage.put(backend, "c/0/obj", "payload")

      # A scratch file left behind by a killed writer, in the same
      # directory the real object lives in.
      File.write!(Path.join(root, "c/0/.bedrock-tmp.obj.999"), "half a pay")

      assert backend |> ObjectStorage.list("c/") |> Enum.to_list() == ["c/0/obj"]
    end

    test "scratch files are never visible to get/2", %{backend: backend, root: root} do
      File.mkdir_p!(Path.join(root, "c/0"))
      File.write!(Path.join(root, "c/0/.bedrock-tmp.obj.999"), "half a pay")

      assert {:error, :not_found} = ObjectStorage.get(backend, "c/0/obj")
    end

    # Scratch names must be arbitrated by the filesystem, not by a
    # node-local counter. A root directory is routinely shared by more
    # than one node — the default one is derived from the system tmp dir
    # and carries nothing node-specific — and :erlang.unique_integer/1
    # repeats freely across VMs. Two writers on one scratch name would
    # interleave into a single inode and publish the splice.
    test "concurrent writers to one key never splice their payloads", %{backend: backend, root: root} do
      payloads = for i <- 1..16, do: String.duplicate(<<?a + i>>, 64_000)

      results =
        payloads
        |> Task.async_stream(&ObjectStorage.put_if_not_exists(backend, "c/0/contended", &1),
          max_concurrency: 16,
          ordered: false
        )
        |> Enum.map(fn {:ok, result} -> result end)

      assert Enum.count(results, &(&1 == :ok)) == 1,
             "exactly one writer may claim the key"

      assert Enum.all?(results, &(&1 in [:ok, {:error, :already_exists}])),
             "a lost race is :already_exists, never a torn-write error: #{inspect(Enum.uniq(results))}"

      {:ok, published} = ObjectStorage.get(backend, "c/0/contended")

      assert published in payloads,
             "the published object must be exactly one writer's payload, not a splice"

      assert all_files(root) == ["c/0/contended"]
    end

    test "concurrent writers to distinct keys all succeed", %{backend: backend, root: root} do
      results =
        1..16
        |> Task.async_stream(&ObjectStorage.put_if_not_exists(backend, "c/0/k#{&1}", "payload-#{&1}"),
          max_concurrency: 16,
          ordered: false
        )
        |> Enum.map(fn {:ok, result} -> result end)

      assert Enum.all?(results, &(&1 == :ok)), "distinct keys must not contend: #{inspect(results)}"
      assert length(all_files(root)) == 16
    end
  end

  describe "put_if_not_exists/4 claims a key only with a whole object" do
    test "refuses a key that already holds an object", %{backend: backend} do
      :ok = ObjectStorage.put_if_not_exists(backend, "c/0/obj", "first")

      assert {:error, :already_exists} = ObjectStorage.put_if_not_exists(backend, "c/0/obj", "second")
      assert {:ok, "first"} = ObjectStorage.get(backend, "c/0/obj")
    end

    # The key point: a leftover scratch file is NOT a claim. Under the
    # old create-then-write shape the claim was the empty target file
    # itself, which no retry could ever displace.
    test "succeeds when only a stale scratch file is present", %{backend: backend, root: root} do
      File.mkdir_p!(Path.join(root, "c/0"))
      File.write!(Path.join(root, "c/0/.bedrock-tmp.obj.999"), "wreckage")

      assert :ok = ObjectStorage.put_if_not_exists(backend, "c/0/obj", "payload")
      assert {:ok, "payload"} = ObjectStorage.get(backend, "c/0/obj")
    end

    # Re-putting an existing object is the ordinary idempotent outcome for
    # chunks and snapshots, so the rejection must not cost a full payload
    # write and fsync first.
    test "rejecting a taken key does not write the payload", %{backend: backend, root: root} do
      :ok = ObjectStorage.put_if_not_exists(backend, "c/0/obj", "first")
      before = all_files(root)

      assert {:error, :already_exists} =
               ObjectStorage.put_if_not_exists(backend, "c/0/obj", String.duplicate("x", 1_000_000))

      assert all_files(root) == before, "a rejected claim must leave no scratch file behind"
      assert {:ok, "first"} = ObjectStorage.get(backend, "c/0/obj")
    end
  end

  describe "write failures surface instead of raising" do
    # ENOSPC is the realistic trigger. An unwritable parent directory
    # reaches the same code path: the scratch open fails, and the caller
    # must get an error rather than a MatchError — and must not leave a
    # claimed key behind.
    test "an unwritable directory returns an error and claims nothing", %{backend: backend, root: root} do
      dir = Path.join(root, "c/0")
      File.mkdir_p!(dir)
      on_exit(fn -> File.chmod(dir, 0o755) end)
      :ok = File.chmod(dir, 0o500)

      assert {:error, _reason} = ObjectStorage.put_if_not_exists(backend, "c/0/obj", "payload")

      :ok = File.chmod(dir, 0o755)
      assert all_files(root) == [], "a failed write must leave no key and no scratch file"

      # And the key is still claimable once the fault clears.
      assert :ok = ObjectStorage.put_if_not_exists(backend, "c/0/obj", "payload")
    end

    test "put/4 on an unwritable directory returns an error", %{backend: backend, root: root} do
      dir = Path.join(root, "c/0")
      File.mkdir_p!(dir)
      on_exit(fn -> File.chmod(dir, 0o755) end)
      :ok = File.chmod(dir, 0o500)

      assert {:error, _reason} = ObjectStorage.put(backend, "c/0/obj", "payload")
    end

    test "put/4 leaves the previous object intact when the new write fails", %{backend: backend, root: root} do
      :ok = ObjectStorage.put(backend, "c/0/obj", "original")
      dir = Path.join(root, "c/0")
      on_exit(fn -> File.chmod(dir, 0o755) end)
      :ok = File.chmod(dir, 0o500)

      assert {:error, _reason} = ObjectStorage.put(backend, "c/0/obj", "replacement")

      :ok = File.chmod(dir, 0o755)

      assert {:ok, "original"} = ObjectStorage.get(backend, "c/0/obj"),
             "a failed overwrite must not destroy the object it was replacing"
    end
  end

  describe "reclaiming scratch files left by a killed writer" do
    # A process killed between the scratch write and the publish leaves
    # its scratch file behind at full payload size. list/3 already hides
    # it (tested above), but hiding it is not reclaiming it — nothing
    # freed the disk space. bedrock-ck3.

    # Plants a scratch file using the writer's exact naming scheme
    # (see open_scratch/2 in local_filesystem.ex), backdated to `age`
    # seconds old.
    defp plant_scratch(root, key, age_seconds) do
      dir = Path.join(root, Path.dirname(key))
      File.mkdir_p!(dir)

      scratch =
        Path.join(
          dir,
          ".bedrock-tmp.#{Path.basename(key)}.#{node()}.#{System.pid()}.#{:erlang.unique_integer([:positive])}"
        )

      File.write!(scratch, :binary.copy(<<0>>, 1024))
      File.touch!(scratch, System.os_time(:second) - age_seconds)
      scratch
    end

    test "a scratch file older than the reap window is deleted by the next write into its directory", %{
      backend: backend,
      root: root
    } do
      stale = plant_scratch(root, "c/0/obj", 7_200)

      assert File.exists?(stale), "the wreckage must exist before the fix can be judged"

      :ok = ObjectStorage.put(backend, "c/0/other", "payload")

      refute File.exists?(stale),
             "a scratch file old enough to predate any plausible in-flight write must be reclaimed"
    end

    test "a fresh scratch file is left alone by a write into its directory", %{backend: backend, root: root} do
      fresh = plant_scratch(root, "c/0/obj", 5)

      :ok = ObjectStorage.put(backend, "c/0/other", "payload")

      assert File.exists?(fresh), "a scratch file young enough to be a live writer's must not be touched"
    end

    test "reclaiming stale scratch files never removes a real object", %{backend: backend, root: root} do
      :ok = ObjectStorage.put(backend, "c/0/obj", "payload")
      stale = plant_scratch(root, "c/0/obj", 7_200)

      :ok = ObjectStorage.put(backend, "c/0/other", "payload")

      refute File.exists?(stale)
      assert {:ok, "payload"} = ObjectStorage.get(backend, "c/0/obj")
    end
  end
end
