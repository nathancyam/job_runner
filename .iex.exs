high_load = %{
  config: %{
    pool_size: 10,
    temporary_max_workers: 200,
    temporary_worker_idle_timeout: to_timeout(second: 5),
    temporary_worker_spawn_interval: 50,
    queue_high_watermark: 100
  },
  tasks: 1_000
}

do_test = fn load ->
  %{config: config, tasks: tasks} = load
  :observer.start()
  {:ok, worker} = JobRunner.start_queue("worker", config)
  JobRunner.demo(worker, tasks)
end
