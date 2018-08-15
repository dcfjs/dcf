Roles:

Client:
Run scripts, generate tasks

Master:
Manage worker & tasks & rdd, handle clients.

Worker:
Save rdd, run tasks

DataType:
String
Number
Array<T>
Pair<K, V>

RDD Storage:

MEMORY_ONLY: save only in memory.
DISK_ONLY: save only in disk, read chunks before task started (compute only happens in memory).

Chunk:
One piece of data. Chunk should not exceed 64M.

Document:
One data of any DataType

Crash handling:
Crash will cause current task failed, without any protection. any of worker/master crashed will cause every of them restarted.

Enviornment/Addons:
Should be set up for each master & worker, not for client/task.
