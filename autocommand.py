import datetime
import subprocess
import pathlib
import json
import multiprocessing
import argparse
import re
from matplotlib import pyplot as plt
import numpy as np

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
            if job in map(lambda d: d['job'], json.load(fd).values()):
                if is_parallel: lock.release()
                return
        if is_parallel: lock.release()

        exception = None
        try:
            print(job)
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

        database[str(index)] = {
            'job': job,
            'terminated': exception == None
        }
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

def analyse_step():
    regex = re.compile(r'\[src(?:\\|\/)main.rs:\d+:\d+] format!\("{} = (?:valid|forbidden)", atom\.to_string\(\)\) = "(\w+)\(.+\) = (valid|forbidden)"')
    with open(pathlib.Path("storage", "database.json")) as fd:
        database = json.load(fd)
    for index, entry in database.items():
        if not entry['terminated']:
            continue
        analysis = pathlib.Path("storage", "analysis", "steps", str(index))
        stderr = pathlib.Path("storage", "stderr", str(index)).read_text().strip().split('\n')

        i = 0
        xs = dict()
        for line1, line2 in zip(stderr[:-1], stderr[1:]):
            try:
                timestamp1, _ = line1.split(" ", maxsplit=1)
                timestamp1 = datetime.datetime.fromtimestamp(float(timestamp1))
                timestamp2, line = line2.split(" ", maxsplit=1)
                timestamp2 = datetime.datetime.fromtimestamp(float(timestamp2))
                diff = timestamp2 - timestamp1
                predicate = regex.match(line).group(1)
                valid = regex.match(line).group(2) == 'valid'
                xs.setdefault(predicate, [list(), list(), list()])
                xs[predicate][0].append(i)
                xs[predicate][1].append(float(diff.total_seconds()))
                xs[predicate][2].append(valid)
                i += 1
            except ValueError:
                break
            except AttributeError:
                continue
        
        if len(xs) == 0:
            continue
        plt.close()
        for ((predicate, (x, diff, valid)), color) in zip(xs.items(), plt.rcParams['axes.prop_cycle'].by_key()['color']):
            plt.plot(np.array(x)[valid], np.array(diff)[valid], marker='.', linestyle='None', markerfacecolor=None, color=color, label=f'{predicate} (valid)')
            if not all(valid):
                plt.plot(np.array(x)[np.invert(valid)], np.array(diff)[np.invert(valid)], marker='.', linestyle='None', markerfacecolor='None', color=color, label=f'{predicate} (forbidden)')
        plt.yscale('log')
        plt.legend()
        plt.savefig(analysis)

def analyse_total():
    regex = re.compile(r'(?:\d+\.\d+ )?(\d+\.\d+)user (\d+\.\d+)system (\d+:\d+\.\d+)elapsed (\d+)%CPU')
    # regex = re.compile(r'(?:\d+\.\d+ )?(\d+\.\d+)user (\d+\.\d+)system (\d+:\d+\.\d+)elapsed (\d+)%CPU.+?\n(?:\d+\.\d+ )?(\d+)inputs\+(\d+)outputs \((\d+)major\+(\d+)minor\)pagefaults (\d+)swaps')
    with open(pathlib.Path("storage", "database.json")) as fd:
        database = json.load(fd)
    with open(pathlib.Path("data", "buckets.json")) as fd:
        buckets = json.load(fd)
    
    translation = {
        'gringo': 'gringo',
        './data/dlv-2.1.2-linux-x86_64': 'dlv',
        './data/grounder': 'ours',
        'java': 'alpha'
    }
    xs = dict()
    for index, entry in database.items():
        if not entry['terminated']:
            continue
        stderr = pathlib.Path("storage", "stderr", str(index)).read_text().strip()
        stdout = pathlib.Path("storage", "stdout", str(index)).read_text().strip()
        characteristica = entry['job'].rsplit(' ', maxsplit=1)[-1]
        reasoner = entry['job'].split(' ', maxsplit=1)[0]

        for file in [stderr, stdout]:
            try:
                user, _system, _elapsed, _cpu = next(regex.finditer(file)).groups()
                success = False
                for test, subtests in buckets.items():
                    for subtest, files in subtests.items():
                        if characteristica in files:
                            xs.setdefault(test, dict()).setdefault(translation[reasoner], dict()).setdefault(subtest, list()).append(float(user))
                            success = True
                            break
                    if success:
                        break

            except AttributeError as e:
                continue
            else:
                break
    
    for test, reasoners in xs.items():
        plt.close()
        for reasoner, subtests in reasoners.items():
            x = list()
            y = list()
            for subtest, results in subtests.items():
                if len(results) == 0:
                    continue
                x.append(int(subtest))
                y.append(sum(results)/len(results))
            plt.plot(*zip(*sorted(zip(x, y), key=lambda d: d[0])), label=reasoner)
        plt.yscale('log')
        plt.title(test)
        plt.legend()
        plt.savefig(pathlib.Path("storage", "analysis", "totals", test))

def main():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="subparsers")
    run_parser = subparsers.add_parser("run")
    run_parser.add_argument('-p', '--parallel', action='store_true', help="Whether to run the command in parallel")
    run_parser.add_argument('-s', '--step-time', action='store_true', help="Whether to log the time for each line of stdout per job")
    run_parser.add_argument('-t', '--total-time', action='store_true', help="Whether to log the total runtime of each job")
    run_parser.add_argument('-d', '--duration', default='1800', help="How long the job is allowed to run before intervention")
    run_parser.add_argument('-m', '--mode', default='merge', choices=['swap', 'merge', 'normal'], help="Whether to merge stderr into stdout")
    run_parser.add_argument('-f', '--filter', default='', help="How the executed lines should look like at the beginning")
    subparsers.add_parser("clear")
    subparsers.add_parser("compact")
    analyse_parser = subparsers.add_parser("analyse")

    args = parser.parse_args()

    if args.subparsers == 'compact':
        with open(pathlib.Path("storage", "database.json")) as fd:
            database = json.load(fd)
        remaining_jobs = get_jobs()
        for entry in database.values():
            if not entry['terminated']:
                forbidden_file = entry['job'].rsplit(' ', maxsplit=1)[-1]
                for job in get_jobs():
                    if job.find(forbidden_file) != -1 and job in remaining_jobs:
                        remaining_jobs.remove(job)
        with open("jobs", "w") as fd:
            fd.write('\n'.join(remaining_jobs))

        args.subparsers = 'clear'
    
    if args.subparsers == 'clear':
        for path in pathlib.Path("storage").rglob("./*/*"):
            if path.is_file():
                path.unlink()
        with open(pathlib.Path("storage", "database.json"), "w") as fd:
            json.dump(dict(), fd, indent=4)
    
    if args.subparsers == 'analyse':
        analyse_step()
        analyse_total()

    if args.subparsers == 'run':
        r = Runner()
        r.start(args)

if __name__ == '__main__':
    main()