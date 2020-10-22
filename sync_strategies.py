import signal
import subprocess
import os

from threading.event import Event

sync_request = Event()
exit_requested = Event()


class GitHandler:
    def __init__(self, commit_message: str, branch: str, repo: str, dir: str):
        self.commit_message = commit_message
        self.branch = branch
        self.rep = repo
        self.dir = dir

        self.clone()

    def clone(self):
        subprocess.run(f"git clone {self.repo} {self.dir}", cwd=self.dir)
        subprocess.run(f"git checkout {self.branch} && git pull", cwd=self.dir)

    def pull(self):
        subprocess.run('git pull', cwd=self.dir)

    def commit(self):
        subprocess.run(f"git commit -a -m\"{self.commit_message}\"", cwd=self.dir)

    def push(self):
        subprocess.run('git push', cwd=self.dir)


if __name__ == '__main__':
    def exit_gracefully():
        exit_requested.set()
        sync_request.set()

    # Set our pid for hummingbot to find
    pid = os.getpid()
    subprocess.run(f"export SYNC_STRATEGIES_PID={pid}")

    # Setup our signal handlers to catch sync requests
    signal.signal(signal.SIGUSR1, lambda: sync_request.set())
    signal.signal(signal.SIGINT, exit_gracefully)
    signal.signal(signal.SIGTERM, exit_gracefully)

    # Manage the syncing of our strategies over git
    git = GitHandler(
        commit_message = f"Update {os.getenv('STRATEGY_NAME')}",
        branch = os.getenv('STRATEGIES_BRANCH'),
        repo = 'git@bitbucket.org:tokamaktech/strategies.git',
        dir = '/home/hummingbot/conf/strategies'
    )

    while(not exit_requested.set()):
        sync_request.wait()
        sync_request.clear()
        git.pull()
        git.commit()
        git.push()
