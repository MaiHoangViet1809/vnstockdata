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
        :param repo_path: Path to the Git repository. If None, uses the current working directory.
        """
        if repo_path is None:
            repo_path = os.getcwd()
        self.repo_path = os.path.abspath(repo_path)
        logger.debug(f"GitPusher initialized for repo at: {self.repo_path}")

    def _run_command(self, cmd: list[str]) -> None:
        """
        Run a command via subprocess.run, raising on failure.
        """
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
        except subprocess.CalledProcessError as e:
            logger.error("Git command failed: %s", e.stderr.strip())
            raise RuntimeError(
                f"Command {' '.join(cmd)} exited {e.returncode}: {e.stderr.strip()}"
            ) from e

        logger.info("Command output: %s", result.stdout.strip())

    def push(self, remote: str = "origin", branch: str = "main") -> None:
        """
        Perform `git add .`, `git commit -m "autocommit"` (if there are changes), then `git push`.
        """
        # 1. Check for uncommitted changes
        status_cmd = ["git", "status", "--porcelain"]
        logger.debug("Checking repo status...")
        try:
            status_out = subprocess.run(
                status_cmd,
                cwd=self.repo_path,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                check=True,
            ).stdout
        except subprocess.CalledProcessError as e:
            logger.error("Failed to check git status: %s", e.stderr.strip())
            raise

        if status_out.strip():
            # There are changes, so stage and commit them
            logger.info("Uncommitted changes detected. Staging and committing...")
            self._run_command(["git", "add", "."])
            # Use a timestamped auto‐commit or raise if you need a custom message
            commit_msg = f"autocommit: {os.getenv('USER', '<user>')} @ {os.popen('date').read().strip()}"
            self._run_command(["git", "commit", "-m", commit_msg])
        else:
            logger.info("No changes to commit.")

        # 2. Push
        logger.info("Pushing to %s/%s", remote, branch)
        self._run_command(["git", "push", remote, branch])
