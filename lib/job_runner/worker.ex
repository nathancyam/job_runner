defmodule JobRunner.Worker do
  use GenServer, restart: :transient

  require Logger

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  @impl GenServer
  def init(opts) do
    maybe_worker_type =
      if on_start = Keyword.get(opts, :on_start) do
        on_start.(self())
      end

    worker_type =
      case maybe_worker_type do
        {:ok, :temporary} ->
          Process.flag(:trap_exit, true)
          :temporary

        _ ->
          :default
      end

    {:ok, %{tasks_completed: 0, worker_type: worker_type}}
  end

  def work_on_task(worker_pid, task) when is_function(task) do
    GenServer.cast(worker_pid, {:work_on_task, task})
  end

  @impl GenServer
  def handle_cast({:work_on_task, task}, state) do
    task.()
    {:noreply, %{state | tasks_completed: state.tasks_completed + 1}}
  end

  @impl GenServer
  def handle_info({:EXIT, _pid, reason}, state) do
    {:stop, reason, state}
  end

  @impl GenServer
  def terminate(_reason, state) do
    Logger.info(
      "Worker #{state.worker_type} terminating after completing #{state.tasks_completed} tasks"
    )

    :ok
  end
end
