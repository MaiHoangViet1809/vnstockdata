import os
import subprocess
import logging
from typing import Optional

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class GitPusher:
    """
    Encapsulates a simple `git push` command.
    """

    def __init__(self, repo_path: Optional[str] = None):
        """
        :param repo_path: Path to the Git repository. If None, use the current working directory.
        """
        if repo_path is None:
            repo_path = os.getcwd()
        self.repo_path = os.path.abspath(repo_path)
        logger.debug(f"GitPusher initialized for repo at: {self.repo_path}")

    def _run_command(self, cmd: list[str]):
        logger.debug(f"Running command: {' '.join(cmd)} in {self.repo_path}")
        try:
            result = subprocess.run(
                cmd,
                cwd=self.repo_path,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                check=True,
            )
            logger.info("Command output: %s", result.stdout.strip())
            return result
        except subprocess.CalledProcessError as e:
            logger.error("Git command failed: %s", e.stderr.strip())
            raise RuntimeError(
                f"Command {' '.join(cmd)} exited {e.returncode}: {e.stderr.strip()}"
            ) from e

    def push(self, remote: str = "origin", branch: str = "main") -> None:
        """
        Perform `git add .`, `git commit -m "autocommit"` (if there are changes), then `git push`.
        """
        status_cmd = ["git", "status", "--porcelain"]
        logger.debug("Checking repo status...")
        status_out = self._run_command(status_cmd).stdout

        if status_out.strip():
            logger.info("Uncommitted changes detected. Staging and committing...")
            self._run_command(["git", "add", "."])

            commit_msg = f"autocommit: {os.getenv('USER', '<user>')} @ {os.popen('date').read().strip()}"
            self._run_command(["git", "commit", "-m", commit_msg])
        else:
            logger.info("No changes to commit.")

        logger.info("Pushing to %s/%s", remote, branch)
        self._run_command(["git", "push", remote, branch])
