"""
karapace - version

Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
import importlib.util
import os
import subprocess


def save_version(new_ver, old_ver, version_file):
    if not new_ver:
        return False
    version_file = os.path.join(os.path.dirname(__file__), version_file)
    if not old_ver or new_ver != old_ver:
        with open(version_file, mode="w", encoding="utf8") as fp:
            fp.write('"""{}"""\n__version__ = "{}"\n'.format(__doc__, new_ver))
    return True


def get_project_version(version_file: str) -> str:
    version_file_full_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), version_file)
    module_spec = importlib.util.spec_from_file_location("verfile", version_file_full_path)
    module = importlib.util.module_from_spec(module_spec)
    file_ver = getattr(module, "__version__", None)

    version = os.getenv("KARAPACE_VERSION")
    if version is None:
        os.chdir(os.path.dirname(__file__) or ".")
        try:
            git_out = subprocess.check_output(
                ["git", "describe", "--always", "--tags"], stderr=getattr(subprocess, "DEVNULL", None)
            )
        except (OSError, subprocess.CalledProcessError):
            pass
        else:
            git_ver = git_out.splitlines()[0].strip().decode("utf-8")
            if "." not in git_ver:
                git_ver = "0.0.1-0-unknown-{}".format(git_ver)
            version = git_ver

    if save_version(version, file_ver, version_file):
        return version

    if not file_ver:
        raise Exception("version not available from git or from file {!r}".format(version_file))

    return file_ver


if __name__ == "__main__":
    import sys

    get_project_version(sys.argv[1])
