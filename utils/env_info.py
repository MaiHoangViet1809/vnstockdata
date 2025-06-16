
def get_platform():
    """
    Get the platform/operating system of the host.

    Returns:
        str: The platform name. Possible values are:
             - "linux" for Linux systems
             - "mac" for macOS
             - "windows" for Windows
             - "others" for unknown platforms

    Examples:
        ```python
        platform = get_platform()
        print(f"Running on: {platform}")
        ```
    """
    from sys import platform
    return {
        "linux": "linux",
        "linux2": "linux",
        "darwin": "mac",
        "win32": "windows",
    }.get(platform, "others")
