import subprocess
import pathlib
import json
import multiprocessing
import shlex

def get_jobs() -> list[str]:
    with open("jobs") as fd:
        jobs = fd.read().split("\n")
    return jobs

def init_pool_processes(the_lock):
    global lock
    lock = the_lock

class Runner:
    def run(self, job, is_locked: bool = True):
        database_path = pathlib.Path("storage", "database.json")
        stderr_path = pathlib.Path("storage", "stderr")
        stdout_path = pathlib.Path("storage", "stdout")
        
        if is_locked: lock.acquire()
        with open(database_path) as fd:
            if job in json.load(fd).values():
                if is_locked: lock.release()
                return
        if is_locked: lock.release()

        result = subprocess.run(shlex.split(job), stdout=subprocess.PIPE)

        if is_locked: lock.acquire()

        with open(database_path) as fd:
            database = json.load(fd)
            
        if len(database) == 0:
            index = 0
        else:
            index = max([int(i) for i in database.keys()]) + 1

        if result.stdout != None:
            stdout_path.joinpath(str(index)).write_bytes(result.stdout)
        if result.stderr != None:
            stderr_path.joinpath(str(index)).write_bytes(result.stderr)

        database[str(index)] = job
        with open(database_path, "w") as fd:
            json.dump(database, fd, indent=4)
        
        if is_locked: lock.release()
    
    def start(self, serial: bool):
        if serial:
            for job in get_jobs():
                self.run(job, False)
        
        else:
            lock = multiprocessing.Lock()
            pool = multiprocessing.Pool(initializer=init_pool_processes, initargs=(lock,))
            pool.map(self.run, get_jobs())
            pool.close()
            pool.join()

def main():
    r = Runner()
    r.start(False)

if __name__ == '__main__':
    main()