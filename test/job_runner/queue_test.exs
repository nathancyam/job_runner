defmodule JobRunner.QueueTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias JobRunner.Queue

  setup do
    {pid, _logs} =
      with_log(fn ->
        worker_sup =
          start_supervised!(
            {DynamicSupervisor, name: __MODULE__.WorkerSupervisor, strategy: :one_for_one}
          )

        start_supervised!({Queue, name: __MODULE__, pool_size: 1, worker_supervisor: worker_sup})
      end)

    {:ok, pid: pid}
  end

  test "default config has pool_size of 5" do
    config = Queue.default_config()
    assert config.pool_size == 5
  end

  test "queue starts workers processes according to pool_size", %{pid: pid} do
    state = :sys.get_state(pid)
    assert map_size(state.tasks_in_progress) == 0

    workers = :pg.get_members(pid)
    assert length(workers) == state.config.pool_size
  end

  test "enqueue and process tasks", %{pid: pid} do
    test_pid = self()

    task_fun = fn ->
      send(test_pid, :test_complete)
      :ok
    end

    Queue.async_enqueue(pid, task_fun)

    assert_receive :test_complete
  end

  test "recovers workers that crash", %{pid: pid} do
    worker = :pg.get_members(pid) |> List.first()
    Process.exit(worker, :kill)

    Process.sleep(100)

    workers = :pg.get_members(pid)
    assert length(workers) == 1
  end

  test "recovers tasks from crashed workers during task execution", %{pid: pid} do
    test_pid = self()
    agent = start_supervised!({Agent, fn -> 0 end})

    poison_task = fn ->
      state = Agent.get_and_update(agent, &{&1, fn s -> s + 1 end})

      if state == 0 do
        raise "Simulated task failure"
      end

      send(test_pid, :task_completed)
      :ok
    end

    logs =
      capture_log(fn ->
        Queue.async_enqueue(pid, poison_task)
        assert_receive :task_completed
      end)

    assert logs =~ "Simulated task failure"
  end

  test "replies with task result synchronously", %{pid: pid} do
    task_fun = fn -> 42 end

    result = Queue.enqueue(pid, task_fun)
    assert result == 42
  end
end
