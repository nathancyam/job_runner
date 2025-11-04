high_load = %{
  config: %{
    pool_size: 10,
    temporary_max_workers: 1_000,
    temporary_worker_idle_timeout: to_timeout(second: 5),
    temporary_worker_spawn_interval: 50,
    queue_high_watermark: 100
  },
  tasks: 10_000
}

do_test = fn load ->
  %{config: config, tasks: tasks} = load
  :observer.start()
  {:ok, worker} = JobRunner.start_queue("worker", config)
  JobRunner.demo(worker, tasks)
  {:ok, worker}
end

do_sync_test = fn worker ->
  :ok =
    JobRunner.Queue.enqueue(worker, fn ->
      :timer.sleep(500)
      :ok
    end)
end
