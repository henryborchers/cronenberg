import abc
import contextlib
from types import TracebackType
from typing import Optional, Type


class DuplicateReportGenerator(contextlib.AbstractContextManager):
    def __init__(self, filename: str):
        self.filename = filename

    @abc.abstractmethod
    def add_duplicates(self, source, duplicates):
        pass

class DuplicateReportCSV(DuplicateReportGenerator):

    def add_duplicates(self, source, duplicates):
        if self.filename is not None:
            with open(self.filename, "a") as out_file:
                out_file.write(f"{source}, {','.join(duplicates)} \n")

    def __exit__(self, __exc_type: Optional[Type[BaseException]],
                 __exc_value: Optional[BaseException],
                 __traceback: Optional[TracebackType]) -> Optional[bool]:
        return super().__exit__(__exc_type, __exc_value, __traceback)