--1.c
	--The Default Option and the one we chose
ALTER SYSTEM SET max_worker_processes TO '8'
----
    --DEFAULT OPTION
ALTER SYSTEM SET max_parallel_workers_per_gather TO '2'
    --The Option that we chose
ALTER SYSTEM SET max_parallel_workers_per_gather TO '8'
----
	--The Default Option and the one we chose
ALTER SYSTEM SET dynamic_shared_memory_type TO 'windows'
----
	--Reload PostgreSQL Configuration
SELECT pg_reload_conf();