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

    @type task :: {function(), GenServer.from() | nil}

    @type t :: %__MODULE__{
            queue: :queue.queue(task()),
            config: map(),
            tasks_in_progress: %{optional(pid()) => task()},
            last_temp_worker_spawned_at: DateTime.t() | nil
          }

    defstruct queue: :queue.new(),
              config: %{},
              tasks_in_progress: %{},
              last_temp_worker_spawned_at: nil

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
      temporary_worker_idle_timeout: to_timeout(second: 3),
      temporary_worker_spawn_interval: 500,
      queue_high_watermark: 50
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
    case get_random_available_worker(state) do
      nil ->
        {:noreply, state}

      worker ->
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

    next_continue =
      if can_spawn_temp_workers?(state) do
        :start_temp_worker
      else
        :dequeue
      end

    {:noreply, %{state | tasks_in_progress: tasks_in_progress}, {:continue, next_continue}}
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
    if map_size(tasks_in_progress) == 0 and :queue.len(queue) > 0 do
      send(self(), :dequeue)
    end

    {:noreply, state}
  end

  def handle_info({_ref, :leave, _group, _leaving_pids}, state) do
    # Since we monitor each worker when assigning a task, there isn't any need
    # to do anything here.
    {:noreply, state}
  end

  @impl true
  def handle_continue({:run_task, worker_pid, task}, state) do
    queue_pid = self()
    monitor = Process.monitor(worker_pid)

    {task_fun, maybe_from} = task

    # When there's an available worker after assigning a task, dequeue
    # the next one to to keep the workers busy. This is especially important
    # when we have temporary workers that need to be kept active to prevent
    # them from shutting down due to idle timeouts.
    if is_pid(get_random_available_worker(state)) do
      send(self(), :dequeue)
    end

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

  def handle_continue(:start_temp_worker, %State{} = state) do
    %{config: %{worker_supervisor: sup}} = state

    {:ok, _pid} = start_worker(sup, :temporary, state.config)

    {:noreply, %{state | last_temp_worker_spawned_at: DateTime.utc_now(:millisecond)},
     {:continue, :dequeue}}
  end

  @spec start_worker(any(), :permanent | :temporary, map()) ::
          {:ok, pid()} | {:error, any()}
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

  @spec get_random_available_worker(State.t()) :: pid() | nil
  defp get_random_available_worker(%{tasks_in_progress: tasks_in_progress} = _state) do
    busy_workers = Map.keys(tasks_in_progress)

    case :pg.get_members(self()) -- busy_workers do
      [] -> nil
      available_workers -> Enum.random(available_workers)
    end
  end

  @spec can_spawn_temp_workers?(State.t()) :: boolean()
  defp can_spawn_temp_workers?(
         %State{
           queue: queue,
           config: %{
             temporary_max_workers: max_workers,
             queue_high_watermark: high_watermark,
             temporary_worker_spawn_interval: interval
           },
           last_temp_worker_spawned_at: time_since_last_spawn
         } = _state
       ) do
    enough_time_passed? =
      is_nil(time_since_last_spawn) or
        DateTime.diff(DateTime.utc_now(:millisecond), time_since_last_spawn, :millisecond) >
          interval

    below_max_workers? = length(:pg.get_members({self(), :temporary_workers})) < max_workers

    above_high_watermark? = :queue.len(queue) > high_watermark

    enough_time_passed? and below_max_workers? and above_high_watermark?
  end
end
