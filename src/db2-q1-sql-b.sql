--1.b
	-- DEFAULT OPTION
ALTER SYSTEM SET shared_buffers TO '128MB'
----	
    -- SEVERAL OPTIONS
ALTER SYSTEM SET shared_buffers TO '2GB' 
ALTER SYSTEM SET shared_buffers TO '2560MB'
ALTER SYSTEM SET shared_buffers TO '3277MB'
----
    -- The Option that we chose
ALTER SYSTEM SET shared_buffers TO '128MB' 
----
	--Reload PostgreSQL Configuration
SELECT pg_reload_conf();