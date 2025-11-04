defmodule JobRunner.QueueSupervisor do
  use Supervisor

  @registry JobRunner.Registry

  def start_link(opts) do
    {name, opts} = Keyword.pop(opts, :queue_name)
    {config, _opts} = Keyword.pop(opts, :config)

    Supervisor.start_link(
      __MODULE__,
      %{
        queue_name: name,
        config: config
      },
      name: {:via, Registry, {@registry, {:queue_supervisor, name}}}
    )
  end

  @impl Supervisor
  def init(%{queue_name: queue_name, config: config}) when is_binary(queue_name) do
    queue_name = {:via, Registry, {@registry, {:queue, queue_name}}}
    supervisor_name = {:via, Registry, {@registry, {:worker_supervisor, queue_name}}}

    children = [
      {JobRunner.WorkerSupervisor, name: supervisor_name},
      {JobRunner.Queue,
       [
         pool_size: config.pool_size,
         temporary_max_workers: config.temporary_max_workers,
         temporary_worker_idle_timeout: config.temporary_worker_idle_timeout,
         name: queue_name,
         worker_supervisor: supervisor_name
       ]}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
