import typing
import os
import pathlib

SYSTEM_FILES = [
    ".DS_Store",
    "._.DS_Store",
    "Thumbs.db",

]

class PathScanner:
    def __init__(self):
        self.slipped_paths = set()

    def scan_path(self, path: str) -> typing.Iterable[pathlib.Path]:
        for root, dirs, files in os.walk(path, followlinks=False):
            if any(root.startswith(s) for s in self.slipped_paths):
                continue
            if ".git" in dirs:
                dirs.remove(".git")
            for sub_directory in dirs:
                if sub_directory in self.slipped_paths or \
                        os.path.join(path, sub_directory) in self.slipped_paths:
                    dirs.remove(sub_directory)
            dirs.sort()
            files.sort()
            for f in files:
                if f in SYSTEM_FILES:
                    continue
                file_path = pathlib.Path(os.path.join(root, f))
                if file_path.is_symlink():
                    continue
                yield file_path