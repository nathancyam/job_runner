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

  def demo(tasks, opts \\ [force_failure?: false]) do
    for i <- 1..tasks do
      task =
        fn ->
          if opts[:force_failure?] do
            raise "Forced failure for task #{i}"
          end

          time = Enum.random(1000..5000)

          if Enum.random(1..6) == 6 do
            raise "Simulated task failure for task #{i}"
          end

          :timer.sleep(time)
          IO.puts("Task #{i} completed")
          :ok
        end

      JobRunner.Queue.enqueue(task)
    end
  end
end
