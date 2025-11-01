defmodule JobRunner.WorkerSupervisor do
  use DynamicSupervisor

  def start_link(opts) do
    {name, opts} = Keyword.pop(opts, :name)
    DynamicSupervisor.start_link(__MODULE__, opts, name: name)
  end

  def init(_init_arg) do
    DynamicSupervisor.init(
      strategy: :one_for_one,
      max_restarts: 100,
      max_seconds: 10
    )
  end
end
