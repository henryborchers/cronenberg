import abc
import contextlib
import os
import pathlib
import sqlite3
from types import TracebackType
from typing import Optional, Type


class DuplicateReportGenerator(contextlib.AbstractContextManager):
    def __init__(self, filename: str):
        self.filename = filename

    @abc.abstractmethod
    def add_duplicates(self, source, duplicates):
        pass


class DuplicateReportSqlite(DuplicateReportGenerator):

    def __init__(self, filename: str, ):
        super().__init__(filename)
        self._con = None

        # self.strategy

    def init_tables(self):
        cur = self._con.cursor()
        cur.execute('DROP TABLE IF EXISTS files')
        cur.execute('''
            CREATE TABLE match_files
            (path text, name TEXT, size INTEGER )
            ''')

        cur.execute('''
            CREATE TABLE mapped_files
            (path TEXT, name TEXT , match_id INTEGER, FOREIGN KEY(match_id) REFERENCES mapped_files(ROWID))
            ''')
        # self.strategy.init_tables(cur)
        self._con.commit()

    def add_duplicates(self, source, duplicates):
        cur = self._con.cursor()
        # path, file_name
        cur.execute(
            "INSERT INTO match_files VALUES (?, ?, ?)",
            (str(source.parent), source.name, source.stat().st_size)
        )
        mapped_id = cur.lastrowid
        for duplicate in duplicates:
            file_ = pathlib.Path(duplicate)

            cur.execute(
                "INSERT INTO mapped_files VALUES (?, ?, ?)",
                (str(file_.parent), file_.name, mapped_id)
            )
        self._con.commit()

    def __enter__(self):
        if os.path.exists(self.filename):
            os.remove(self.filename)
        self._con = sqlite3.connect(self.filename)
        self.init_tables()
        return self

    def __exit__(self, __exc_type: Optional[Type[BaseException]],
                 __exc_value: Optional[BaseException],
                 __traceback: Optional[TracebackType]) -> Optional[bool]:
        if self._con is not None:
            self._con.commit()
            self._con.close()
        return super().__exit__(__exc_type, __exc_value, __traceback)


class DuplicateReportCSV(DuplicateReportGenerator):

    def add_duplicates(self, source, duplicates):
        if self.filename is not None:
            with open(self.filename, "a") as out_file:
                out_file.write(f"{source}, {','.join(duplicates)} \n")

    def __enter__(self):
        if self.filename is not None:
            if os.path.exists(self.filename):
                os.remove(self.filename)
        return self

    def __exit__(self, __exc_type: Optional[Type[BaseException]],
                 __exc_value: Optional[BaseException],
                 __traceback: Optional[TracebackType]) -> Optional[bool]:
        return super().__exit__(__exc_type, __exc_value, __traceback)