defmodule JobRunner do
  @moduledoc """
  Documentation for `JobRunner`.
  """

  @doc """
  Hello world.

  ## Examples

      iex> JobRunner.hello()
      :world

  """
  def hello do
    :world
  end

  def start_queue(
        queue_name,
        config \\ JobRunner.Queue.default_config()
      ) do
    merged_config = Map.merge(JobRunner.Queue.default_config(), config)

    :ok =
      case DynamicSupervisor.start_child(
             JobRunner.QueuesSupervisor,
             {JobRunner.QueueSupervisor, [queue_name: queue_name, config: merged_config]}
           ) do
        {:ok, _pid} ->
          :ok

        {:error, {:already_started, _pid}} ->
          :ok

        unknown ->
          unknown
      end

    case Registry.lookup(JobRunner.Registry, {:queue, queue_name}) do
      [] ->
        {:error, :queue_not_found}

      [{pid, _value}] ->
        {:ok, pid}
    end
  end

  def demo(queue_pid, tasks, opts \\ [force_failure?: false]) do
    for i <- 1..tasks do
      task =
        fn ->
          if opts[:force_failure?] do
            raise "Forced failure for task #{i}"
          end

          if Enum.random(1..20) == 1 do
            raise "Simulated task failure for task #{i}"
          end

          :timer.sleep(Enum.random(1000..3000))
          IO.puts("Task #{i} completed")
          :ok
        end

      JobRunner.Queue.async_enqueue(queue_pid, task)
    end
  end
end
