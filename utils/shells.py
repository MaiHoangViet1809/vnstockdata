from subprocess import Popen, PIPE, STDOUT


def get_platform():
    from sys import platform
    return {
        "linux": "linux",
        "linux2": "linux",
        "darwin": "mac",
        "win32": "windows",
    }.get(platform, "others")


def run_sh(command, stream_stdout=True, return_log=True, executable: str | None = None, stream_callback = print):
    platform = get_platform()
    if platform == "linux":
        executable = "/bin/bash"

    full_log = []
    process = None
    try:
        process = Popen(command,
                        shell=True,
                        stdout=PIPE,
                        stderr=STDOUT,
                        encoding='utf-8',
                        errors='replace',
                        executable=executable)
        while True:
            realtime_output = process.stdout.readline()
            if realtime_output == '' and process.poll() is not None:
                break
            else:
                line_log = realtime_output.strip()
                if stream_stdout and str(line_log).strip():
                    stream_callback(line_log)
                if return_log:
                    full_log.append(line_log)

    finally:
        process.kill()
        if return_log:
            return process.returncode, full_log
        else:
            return process.returncode
