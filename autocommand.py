import subprocess
import pathlib
import json
import multiprocessing
import argparse

def get_jobs() -> list[str]:
    with open("jobs") as fd:
        jobs = fd.read().strip().split("\n")
    return jobs

def init_pool_processes(the_lock):
    global lock
    lock = the_lock

class Runner:
    def run(self, job, is_parallel: bool = True):
        database_path = pathlib.Path("storage", "database.json")
        stderr_path = pathlib.Path("storage", "stderr")
        stdout_path = pathlib.Path("storage", "stdout")
        
        if is_parallel: lock.acquire()
        with open(database_path) as fd:
            if job in json.load(fd).values():
                if is_parallel: lock.release()
                return
        if is_parallel: lock.release()

        exception = None
        try:
            modified_job = job
            if self.mode == 'merge':
                modified_job += " 2>&1"
            elif self.mode == 'swap':
                modified_job += " 3>&2 2>&1 1>&3"
            if self.step_time:
                modified_job += " | while IFS= read -r line; do printf '%s.%s %s\n' $(date +%s) $(date +%N) \"$line\"; done"
            if self.runtime:
                modified_job = "time " + modified_job
            result = subprocess.run(modified_job, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, timeout=self.timeout)
        
        except FileNotFoundError as e:
            exception = e
        except subprocess.TimeoutExpired as e:
            exception = e

        if is_parallel: lock.acquire()

        with open(database_path) as fd:
            database = json.load(fd)
            
        if len(database) == 0:
            index = 0
        else:
            index = max([int(i) for i in database.keys()]) + 1

        if exception == None:
            if self.mode == 'swap':
                stdout_path.joinpath(str(index)).write_bytes(result.stderr)
                stderr_path.joinpath(str(index)).write_bytes(result.stdout)
            else:
                stdout_path.joinpath(str(index)).write_bytes(result.stdout)
                stderr_path.joinpath(str(index)).write_bytes(result.stderr)
        else:
            stdout_path.joinpath(str(index)).write_bytes(b'')
            stderr_path.joinpath(str(index)).write_text(f'[AUTOCOMMAND_ERROR] {exception}')

        database[str(index)] = job
        with open(database_path, "w") as fd:
            json.dump(database, fd, indent=4)
        
        if is_parallel: lock.release()
    
    def start(self, args):
        self.step_time = args.step_time
        self.runtime = args.total_time
        self.timeout = float(args.duration)
        self.mode = args.mode

        jobs = filter(lambda job: job.startswith(args.filter), get_jobs())

        if not args.parallel:
            for job in jobs:
                self.run(job, False)
        
        else:
            lock = multiprocessing.Lock()
            pool = multiprocessing.Pool(initializer=init_pool_processes, initargs=(lock,))
            pool.map(self.run, jobs)
            pool.close()
            pool.join()

def main():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="subparsers")
    run_parser = subparsers.add_parser("run")
    run_parser.add_argument('-p', '--parallel', action='store_true', help="Whether to run the command in parallel")
    run_parser.add_argument('-s', '--step-time', action='store_true', help="Whether to log the time for each line of stdout per job")
    run_parser.add_argument('-t', '--total-time', action='store_true', help="Whether to log the total runtime of each job")
    run_parser.add_argument('-d', '--duration', help="How long the job is allowed to run before intervention")
    run_parser.add_argument('-m', '--mode', default='merge', choices=['swap', 'merge', 'normal'], help="Whether to merge stderr into stdout")
    run_parser.add_argument('-f', '--filter', default='', help="How the executed lines should look like at the beginning")
    clear_parser = subparsers.add_parser("clear")

    args = parser.parse_args()

    if args.subparsers == 'clear':
        for path in pathlib.Path("storage").rglob("./std*/*"):
            path.unlink()
        with open(pathlib.Path("storage", "database.json"), "w") as fd:
            json.dump(dict(), fd, indent=4)

    elif args.subparsers == 'run':
        r = Runner()
        r.start(args)

if __name__ == '__main__':
    main()