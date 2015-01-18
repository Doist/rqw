**rqw** stands for Redis Queue Worker

This program monitors redis queue organized as sorted set, where item score is
a timestamp and spawn/terminate workers consuming this queue. Queue length is
read with `ZCOUNT queue_key 0 current_unix_timestamp` command.

	Usage of rqw:
	  -d=false: prefix output with source code addresses
	  -delay=15s: delay between checks (min. 1s)
	  -max=10: max number of workers
	  -queue="": queue name
	  -redis="localhost:6379": redis instance address
	  -threshold=0: min queue size to spawn workers
	  -worker="": path to worker program

Example:

	rqw -max 4 \
		-queue "lifecycle:queue" \
		-redis "redis-host.local:6379" \
		-worker "/usr/local/lib/rqw/lifecycle"
