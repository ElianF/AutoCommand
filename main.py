import subprocess
import pathlib
import json
import multiprocessing
import shlex

def get_jobs() -> list[str]:
    with open("jobs") as fd:
        jobs = fd.read().split("\n")
    return jobs

def run(job: str):
    database_path = pathlib.Path("storage", "database.json")
    stderr_path = pathlib.Path("storage", "stderr")
    stdout_path = pathlib.Path("storage", "stdout")

    result = subprocess.run(shlex.split(job), stdout=subprocess.PIPE)
    
    with open(database_path) as fd:
        database = json.load(fd)
    if job in database.values():
        return

    if len(database) == 0:
        index = 0
    else:
        index = max([int(i) for i in database.keys()]) + 1

    database[str(index)] = job
    with open(database_path, "w") as fd:
        database = json.dump(database, fd, indent=4)

    if result.stdout != None:
        stdout_path.joinpath(str(index)).write_bytes(result.stdout)
    if result.stderr != None:
        stderr_path.joinpath(str(index)).write_bytes(result.stderr)

def main(serial:bool):

    if serial:
        procs = 1
    else:
        procs = 2
        
    # with multiprocessing.Pool(procs) as pool:
    #     pool.map(run, get_jobs())
    for job in get_jobs():
        run(job)

if __name__ == '__main__':
    main(True)