# MPI-SubScheduler
MPI Sub-Scheduler in Python, for running a batch of small jobs

# How to use

```python
from subscheduler import SubScheduler

sched = SubScheduler()

@sched.worker(list(range(6)))
def foo(task_id):
    print(f"task_id {task_id}")
    return task_id


if __name__ == "__main__":
    sched()

``` 

# How to run

```bash
mpirun --use-hwthread-cpus --oversubscribe -n 5 python main.py
```

