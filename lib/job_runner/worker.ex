defmodule JobRunner.Worker do
  use GenServer, restart: :transient

  require Logger

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  @impl GenServer
  def init(opts) do
    {:ok, worker_config} =
      if on_start = Keyword.get(opts, :on_start) do
        on_start.(self())
      end

    worker_type =
      case worker_config do
        %{type: :temporary} ->
          Process.flag(:trap_exit, true)
          :temporary

        _ ->
          :permanent
      end

    {:ok,
     %{
       tasks_completed: 0,
       worker_type: worker_type,
       config: worker_config,
       started_at: DateTime.utc_now()
     }}
  end

  def work_on_task(worker_pid, task) when is_function(task) do
    GenServer.cast(worker_pid, {:work_on_task, task})
  end

  @impl GenServer
  def handle_cast({:work_on_task, task}, state) do
    %{worker_type: worker_type} = state
    task.()

    new_state = %{state | tasks_completed: state.tasks_completed + 1}

    case worker_type do
      :temporary ->
        # Temporary workers shut down when in an inactive state.
        jitter = Enum.random(50..500)
        {:noreply, new_state, state.config.idle_timeout + jitter}

      :permanent ->
        {:noreply, new_state}
    end
  end

  @impl GenServer
  def handle_info({:EXIT, _pid, reason}, state) do
    {:stop, reason, state}
  end

  def handle_info(:timeout, state) do
    Logger.info(%{
      message: "Temporary worker shutting down after idle timeout",
      alive_for: DateTime.diff(DateTime.utc_now(), state.started_at, :second),
      completed_tasks: state.tasks_completed
    })

    {:stop, :normal, state}
  end

  @impl GenServer
  def terminate(reason, state) do
    case reason do
      {%error_struct{message: message}, _stacktrace} ->
        Logger.warning(%{
          worker_type: state.worker_type,
          message:
            "Worker terminated unexpectedly from #{inspect(error_struct)}: #{inspect(message)}",
          completed_tasks: state.tasks_completed
        })

      reason ->
        Logger.info(%{
          worker_type: state.worker_type,
          message: "Worker terminated with reason: #{reason}",
          completed_tasks: state.tasks_completed
        })
    end

    :ok
  end
end
