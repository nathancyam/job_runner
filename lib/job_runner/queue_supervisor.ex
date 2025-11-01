defmodule JobRunner.QueueSupervisor do
  use Supervisor

  def start_link([queue_name]) do
    Supervisor.start_link(
      __MODULE__,
      %{
        queue_name: queue_name
      },
      name: {:via, Registry, {JobRunner.Registry, {:queue_supervisor, queue_name}}}
    )
  end

  @impl Supervisor
  def init(%{queue_name: queue_name}) when is_binary(queue_name) do
    queue_name = {:via, Registry, {JobRunner.Registry, {:queue, queue_name}}}
    supervisor_name = {:via, Registry, {JobRunner.Registry, {:worker_supervisor, queue_name}}}

    children = [
      {JobRunner.WorkerSupervisor, name: supervisor_name},
      {JobRunner.Queue, pool_size: 5, name: queue_name, worker_supervisor: supervisor_name}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
