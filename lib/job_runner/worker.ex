defmodule JobRunner.Worker do
  use GenServer

  require Logger

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  @impl GenServer
  def init(opts) do
    if on_start = Keyword.get(opts, :on_start) do
      on_start.(self())
    end

    {:ok, %{tasks_completed: 0}}
  end

  def work_on_task(worker_pid, task) when is_function(task) do
    GenServer.cast(worker_pid, {:work_on_task, task})
  end

  @impl GenServer
  def handle_cast({:work_on_task, task}, state) do
    task.()
    {:noreply, %{state | tasks_completed: state.tasks_completed + 1}}
  end
end
