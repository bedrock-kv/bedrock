defmodule Bedrock.DataPlane.Log.Shale.DemuxControlTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Demux
  alias Bedrock.DataPlane.Log.Shale.DemuxControl
  alias Bedrock.DataPlane.Log.Shale.State
  alias Bedrock.ObjectStorage
  alias Bedrock.ObjectStorage.LocalFilesystem

  # DemuxControl is the single place a log's Demux is started from, which
  # is precisely why the cut interval has to travel through it: the log's
  # WAL roll boundary and the Demux's cut boundary are the same boundary,
  # and nothing else is positioned to keep them equal.
  @moduletag :tmp_dir

  defp state_in_tmp_dir(%{tmp_dir: dir}, overrides) do
    struct!(
      %State{
        cluster: Bedrock.Cluster,
        path: dir,
        object_storage: ObjectStorage.backend(LocalFilesystem, root: Path.join(dir, "object_storage"))
      },
      overrides
    )
  end

  describe "start/1" do
    test "hands the Demux the log's configured cut interval", ctx do
      interval = div(Demux.Server.default_cut_interval_us(), 5)
      state = state_in_tmp_dir(ctx, cut_interval_us: interval)

      {:ok, demux} = DemuxControl.start(state)
      on_exit(fn -> DemuxControl.teardown(demux) end)

      assert :sys.get_state(demux).cut_interval_us == interval
    end

    test "falls back to the default when the log has no configured interval", ctx do
      state = state_in_tmp_dir(ctx, cut_interval_us: nil)

      {:ok, demux} = DemuxControl.start(state)
      on_exit(fn -> DemuxControl.teardown(demux) end)

      assert :sys.get_state(demux).cut_interval_us == Demux.Server.default_cut_interval_us()
    end
  end
end
