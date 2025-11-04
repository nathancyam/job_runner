defmodule JobRunner.Queue do
  @moduledoc """
  A GenServer-based job queue that starts and manages a pool of worker processes to
  execute tasks asynchronously.
  """

  use GenServer

  require Logger

  alias JobRunner.Worker

  defmodule State do
    @moduledoc false

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
      pool_size: 5,
      temporary_max_workers: 10,
      temporary_worker_idle_timeout: to_timeout(second: 3)
    }

  def start_link(opts) do
    {name, opts} = Keyword.pop(opts, :name)
    GenServer.start_link(__MODULE__, Map.new(opts), name: name)
  end

  @impl GenServer
  def init(opts) when is_map(opts) do
    for _ <- 1..opts.pool_size do
      {:ok, _worker_pid} = start_worker(opts.worker_supervisor, :permanent, opts)
    end

    Logger.info("Started worker pool with size #{opts.pool_size}")

    # Monitor the process group for leaving workers (crashed workers),
    # so we can react to worker restarts.
    :pg.monitor(self())
    :pg.monitor({self(), :temporary_workers})

    {:ok, %State{config: opts}}
  end

  defp start_worker(worker_supervisor, worker_type, config)
       when worker_type in [:permanent, :temporary] do
    queue_pid = self()

    DynamicSupervisor.start_child(
      worker_supervisor,
      {JobRunner.Worker,
       [
         on_start: fn worker_pid ->
           :pg.join(queue_pid, worker_pid)

           if worker_type == :temporary do
             :pg.join({queue_pid, :temporary_workers}, worker_pid)
             {:ok, %{type: worker_type, idle_timeout: config.temporary_worker_idle_timeout}}
           else
             {:ok, %{type: worker_type, idle_timeout: nil}}
           end
         end
       ]}
    )
  end

  @doc """
  Enqueue a task to be executed asynchronously by the queue.
  """
  def async_enqueue(pid, task) when is_pid(pid) and is_function(task),
    do: GenServer.cast(pid, {:enqueue, task})

  @doc """
  Enqueue a task to be executed by the queue and wait for its result.
  Default timeout is 5 seconds.
  """
  def enqueue(pid, task, timeout \\ 5_000) when is_pid(pid) and is_function(task),
    do: GenServer.call(pid, {:enqueue, task}, timeout)

  @impl GenServer
  def handle_call({:enqueue, task}, from, state) when is_function(task) do
    queue = :queue.in({task, from}, state.queue)
    send(self(), :dequeue)
    {:noreply, %{state | queue: queue}}
  end

  @impl GenServer
  def handle_cast({:enqueue, task}, state) when is_function(task) do
    queue = :queue.in({task, nil}, state.queue)
    send(self(), :dequeue)
    {:noreply, %{state | queue: queue}}
  end

  @impl GenServer
  def handle_info(:dequeue, %{queue: queue, tasks_in_progress: tasks_in_progress} = state) do
    busy_workers = Map.keys(tasks_in_progress)

    # Are there any available workers in the process group?
    case :pg.get_members(self()) -- busy_workers do
      [] ->
        if can_spawn_temp_workers?(state) do
          {:noreply, state, {:continue, :start_temp_worker}}
        else
          {:noreply, state}
        end

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
    # remove it from the tasks in progress. Now that a worker is free, we
    # can try to dequeue another task.
    Process.demonitor(monitor)
    tasks_in_progress = Map.delete(state.tasks_in_progress, worker_pid)
    {:noreply, %{state | tasks_in_progress: tasks_in_progress}, {:continue, :dequeue}}
  end

  def handle_info({:DOWN, _ref, :process, worker_pid, _reason}, state) do
    # Worker process crashed, retrieve the task it was working on (if any)
    # and enqueue it. The worker should be restarted by the supervisor and
    # re-join the process group. This will trigger the `:join` message and
    # start processing any pending tasks.
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

    {task_fun, maybe_from} = task

    Worker.work_on_task(worker_pid, fn ->
      result = task_fun.()
      send(queue_pid, {:task_complete, worker_pid, monitor})

      if not is_nil(maybe_from) do
        GenServer.reply(maybe_from, result)
      end

      result
    end)

    {:noreply, state}
  end

  def handle_continue(:dequeue, state), do: handle_info(:dequeue, state)

  def handle_continue(:start_temp_worker, state) do
    %{config: %{temporary_max_workers: max_temp_workers, worker_supervisor: sup}} = state

    current_temp_workers =
      length(:pg.get_members({self(), :temporary_workers}))

    if current_temp_workers < max_temp_workers do
      {:ok, pid} = start_worker(sup, :temporary, state.config)

      Logger.info(
        "Started temporary worker #{inspect(pid)}. Current temporary workers: #{current_temp_workers + 1}"
      )
    end

    {:noreply, state, {:continue, :dequeue}}
  end

  defp can_spawn_temp_workers?(%State{config: %{temporary_max_workers: max_workers}} = _state) do
    current_temp_workers =
      length(:pg.get_members({self(), :temporary_workers}))

    current_temp_workers < max_workers
  end
end
