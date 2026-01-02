defmodule Bedrock.JobQueue.Test.Jobs do
  @moduledoc """
  Test job modules for consumer integration tests.
  """

  alias Bedrock.JobQueue.Job

  defmodule SuccessJob do
    @moduledoc "Always returns :ok"
    use Job, topic: "test:success"

    @impl true
    def perform(_args), do: :ok
  end

  defmodule SuccessWithResultJob do
    @moduledoc "Returns {:ok, result}"
    use Job, topic: "test:success_with_result"

    @impl true
    def perform(args), do: {:ok, args}
  end

  defmodule FailingJob do
    @moduledoc "Always returns {:error, reason}"
    use Job, topic: "test:fail"

    @impl true
    def perform(_args), do: {:error, :intentional_failure}
  end

  defmodule DiscardJob do
    @moduledoc "Returns {:discard, reason}"
    use Job, topic: "test:discard"

    @impl true
    def perform(_args), do: {:discard, :invalid_data}
  end

  defmodule SnoozeJob do
    @moduledoc "Returns {:snooze, delay}"
    use Job, topic: "test:snooze"

    @impl true
    def perform(%{delay: delay}), do: {:snooze, delay}
    def perform(_args), do: {:snooze, 5000}
  end

  defmodule SlowJob do
    @moduledoc "Sleeps longer than timeout"
    use Job, topic: "test:slow", timeout: 100

    @impl true
    def perform(_args) do
      Process.sleep(500)
      :ok
    end
  end

  defmodule CrashingJob do
    @moduledoc "Raises an exception"
    use Job, topic: "test:crash"

    @impl true
    def perform(_args) do
      raise "intentional crash"
    end
  end

  defmodule NotifyingJob do
    @moduledoc "Sends message to test process before returning"
    use Job, topic: "test:notify"

    @impl true
    def perform(%{test_pid: pid, result: result}) do
      send(pid, {:job_executed, self()})
      result
    end
  end
end
