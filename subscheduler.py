from mpi4py import MPI


comm = MPI.COMM_WORLD


class SubScheduler:
    def __call__(self):
        rank = comm.Get_rank()
        if rank == 0:
            self.schedule(self.task_ids)
        else:
            self.worker_(self.func)
    

    def worker(self, task_ids):
        self.task_ids = task_ids
        def wrapper(func):
            self.func = func
            return func
        return wrapper
    

    def schedule(self, tasks):
        size = comm.Get_size()
        workers = list(range(1, size))
        finished_worker = 0

        for worker in workers:
            if len(tasks) != 0:
                comm.send(tasks.pop(0), dest=worker)
            else:
                comm.send(None, dest=worker)
                finished_worker += 1
        
        while finished_worker < size -1:
            result = comm.recv(source=MPI.ANY_SOURCE)
            worker = result["worker"]

            if len(tasks) != 0:
                task = tasks.pop(0)
                comm.send(task, dest=worker)
            else:
                comm.send(None, dest=worker)
                finished_worker += 1


    def worker_(self, func):
        rank = comm.Get_rank()
        while True:
            task_id = comm.recv(source=0)
            if task_id is None:
                break
            value = func(task_id)
            comm.send({"worker": rank, "task_id": task_id, "value": value, "state":0}, dest=0)


"""
sched = SubScheduler()

@sched.worker(list(range(6)))
def foo(task_id):
    print(f"task_id {task_id}")
    return task_id


if __name__ == "__main__":
    sched()
"""