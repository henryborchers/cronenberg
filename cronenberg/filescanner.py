import os.path
import pathlib
import typing

from dataclasses import dataclass


@dataclass(frozen=True)
class FileData:
    size: int
    path: str
    filename: str
    #
    # @property
    # def size(self):
    #     file = pathlib.Path(os.path.join(self.path, self.filename))
    #     print(file.absolute())
    #     print(self.path)
    #     return file.stat().st_size
    #     # return 3
    #     # return "d"

def scan_file(root, file: pathlib.Path) -> FileData:
    stat = file.stat()
    return FileData(size=stat.st_size,
                    filename=file.name,
                    path=str(file.parent.relative_to(root)))