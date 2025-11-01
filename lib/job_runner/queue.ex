defmodule JobRunner.Queue do
  use GenServer

  require Logger

  alias JobRunner.Worker

  defmodule State do
    defstruct queue: :queue.new(), config: %{}, tasks_in_progress: %{}

    def recover_and_requeue(%State{} = state, worker_pid) when is_pid(worker_pid) do
      {task, tasks_in_progress} = Map.pop(state.tasks_in_progress, worker_pid)

      queue =
        case task do
          nil -> state.queue
          task -> :queue.in(task, state.queue)
        end

      %{state | queue: queue, tasks_in_progress: tasks_in_progress}
    end
  end

  def default_config,
    do: %{
      pool_size: 5
    }

  def start_link(opts) do
    {name, opts} = Keyword.pop(opts, :name)
    dbg(opts)
    GenServer.start_link(__MODULE__, Map.new(opts), name: name)
  end

  @impl GenServer
  def init(opts) when is_map(opts) do
    queue_pid = self()

    for _ <- 1..opts.pool_size do
      {:ok, _worker_pid} =
        DynamicSupervisor.start_child(
          opts.worker_supervisor,
          {JobRunner.Worker,
           [
             on_start: fn worker_pid ->
               :pg.join(queue_pid, worker_pid)
               :ok
             end
           ]}
        )
    end

    Logger.info("Started worker pool with size #{opts.pool_size}")

    # Monitor the process group for worker joins and leaves
    # so we can react to worker restarts.
    :pg.monitor(queue_pid)

    {:ok, %State{config: opts}}
  end

  def enqueue(pid, task) when is_pid(pid) and is_function(task),
    do: GenServer.cast(pid, {:enqueue, task})

  @impl GenServer
  def handle_cast({:enqueue, task}, state) when is_function(task) do
    queue = :queue.in(task, state.queue)
    send(self(), :dequeue)
    {:noreply, %{state | queue: queue}}
  end

  @impl GenServer
  def handle_info(:dequeue, %{queue: queue, tasks_in_progress: tasks_in_progress} = state) do
    busy_workers = Map.keys(tasks_in_progress)

    # Are there any available workers by checking the process group members?
    case :pg.get_members(self()) -- busy_workers do
      [] ->
        {:noreply, state}

      available_workers ->
        worker = Enum.random(available_workers)

        case :queue.out(queue) do
          {{:value, task}, new_queue} ->
            tasks_in_progress = Map.put(tasks_in_progress, worker, task)
            state = %{state | queue: new_queue, tasks_in_progress: tasks_in_progress}

            # Set the tasks progress state before running the tasks to minimize
            # race conditions where the worker fails faster than we update the state.
            {:noreply, state, {:continue, {:run_task, worker, task}}}

          {:empty, _} ->
            {:noreply, state}
        end
    end
  end

  @impl GenServer
  def handle_info({:task_complete, worker_pid, monitor}, state) do
    # Task was completed successfully, demonitor the worker process and
    # remove it from the tasks in progress.
    Process.demonitor(monitor)
    tasks_in_progress = Map.delete(state.tasks_in_progress, worker_pid)
    {:noreply, %{state | tasks_in_progress: tasks_in_progress}, {:continue, :dequeue}}
  end

  def handle_info({:DOWN, _ref, :process, worker_pid, _reason}, state) do
    # Worker process crashed, retrieve the task it was working on (if any)
    # and enqueue it. The worker should be restarted by the supervisor.
    state = State.recover_and_requeue(state, worker_pid)
    {:noreply, state, {:continue, :dequeue}}
  end

  def handle_info(
        {_ref, :join, _group, _joining_pids},
        %{queue: queue, tasks_in_progress: tasks_in_progress} = state
      ) do
    # When a worker joins the queue process group, this implies that a new
    # worker was started as one crashed previously. We send the queue process
    # to process any pending tasks now that we have a worker available.
    #
    # TODO: Maybe add some threshold here to only start processing if we have enough workers
    if map_size(tasks_in_progress) == 0 and :queue.len(queue) > 0 do
      send(self(), :dequeue)
    end

    {:noreply, state}
  end

  def handle_info({_ref, :leave, _group, _leaving_pids}, state) do
    # Since we monitor each worker when assigning it as task, there isn't
    # any need to do anything here.
    {:noreply, state}
  end

  @impl true
  def handle_continue({:run_task, worker_pid, task}, state) do
    queue_pid = self()
    monitor = Process.monitor(worker_pid)

    Worker.work_on_task(worker_pid, fn ->
      result = task.()
      send(queue_pid, {:task_complete, worker_pid, monitor})
      result
    end)

    {:noreply, state}
  end

  def handle_continue(:dequeue, state), do: handle_info(:dequeue, state)
end
