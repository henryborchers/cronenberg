__all__ = ["add_to_csv_file"]

import os
import pathlib
import sqlite3
import typing
import csv
import contextlib
from typing import Optional, Type
import abc
import functools


def add_to_csv_file(file_name: str, data: typing.Mapping[str, typing.Any]) -> None:
    with open(file_name, "a") as f:
        writer = csv.writer(f)
        writer.writerow(data.values())


class DataSchema(abc.ABC):

    @abc.abstractmethod
    def init_tables(self, cursor):
        """Create new tables."""

    @abc.abstractmethod
    def add_file(self, cursor, file_name, data):
        """Generate new file entry"""

    @abc.abstractmethod
    def add_files(self, cursor,
                  records: typing.List[typing.Tuple[str, str, int]]):
        pass

    @abc.abstractmethod
    def record_exists(self, cursor, file_name, data) -> bool:
        """Check if record already exists"""

    @abc.abstractmethod
    def any_already_exists(self, cursor, file_names: typing.List[str]) -> typing.List[str]:
        """Find matching records"""

    @abc.abstractmethod
    def records(self, cursor) -> typing.Iterator[typing.List[typing.Tuple[str, str, int]]]:
        """Return all the records"""

    @abc.abstractmethod
    def find_matches(self, cur, file_name) -> typing.List[str]:
        pass


class DataSchema1(DataSchema):

    def find_matches(self, cursor: sqlite3.Cursor, file_name: str) -> typing.Set[str]:
        stats = os.stat(file_name)
        # "SELECT * FROM files WHERE name = ? AND path = ? ",
        file_path = pathlib.Path(file_name)
        cursor.execute('SELECT * FROM files WHERE name = ? AND size = ?', (file_path.name, stats.st_size))
        matches: typing.Set[str] = set()
        for match_file_name, match_path, match_size in cursor.fetchall():
            matches.add(os.path.join(match_path, match_file_name))
        return matches

    def init_tables(self, cursor):

        cursor.execute('DROP TABLE IF EXISTS files')
        cursor.execute('''
            CREATE TABLE files
            (name text, path text, size number)
            ''')

    def add_files(self, cursor, records: typing.List[typing.Tuple[str, str, int]]):
        cursor.executemany('INSERT INTO files VALUES (?, ?, ?)',
                           records
                           )

    def add_file(self, cursor, file_name, data):
        cursor.execute('INSERT INTO files VALUES (?, ?, ?)',
                       (file_name, str(data.path), data.size))

    @functools.lru_cache()
    def record_exists(self, cursor, file_name, data) -> bool:
        cursor.execute(
            "SELECT * FROM files WHERE name = ? AND path = ? ",
            (file_name, data.path))
        return cursor.fetchone() is not None

    def any_already_exists(self, cursor, file_names: typing.List[pathlib.Path]) -> typing.List[pathlib.Path]:
        # s = "SELECT * FROM files WHERE name IN ({0}) AND path IN ({0})".format(', '.join('?' for _ in file_names))
        # n = ([f.name for f in file_names], [f.root for f in file_names])
        # cursor.execute(s, n
        #     # "SELECT * FROM files WHERE name IN ({0})".format(', '.join('?' for _ in file_names)),
        #     # (f.name for f in file_names)
        # )
        # cursor.execute(
        #     "SELECT * FROM files WHERE name in ? AND path in ?",
        #     ((f.name for f in file_names), (f.root for f in file_names)))
        # res = cursor.fetchall()
        res = set()
        for f in file_names:
            cursor.execute(
                "SELECT * FROM files WHERE name = ? AND path = ? ",
                (f.name, f.root))
            r = cursor.fetchone()
            if r is not None:
                res.add(f)
        return list(res)

    def records(self, cursor) -> typing.Iterator[
        typing.List[typing.Tuple[str, str, int]]]:

        for r in cursor.execute("SELECT * FROM files ORDER BY path "):
            yield r


class SQLiteWriter(contextlib.AbstractContextManager):
    def __init__(self, filename: str, schema_strategy):
        self.filename = filename
        self._con = None
        self.strategy: DataSchema = schema_strategy

    def __enter__(self):
        self._con = sqlite3.connect(self.filename)

        return self

    def __exit__(self, exc_type: Optional[Type[BaseException]],
                 exc_value: Optional[BaseException],
                 traceback) -> Optional[bool]:
        if self._con is not None:
            self._con.commit()
            self._con.close()
        return None

    def init_tables(self):

        cur = self._con.cursor()
        self.strategy.init_tables(cur)
        self._con.commit()

    def add_files(self,
                  records: typing.List[typing.Tuple[str, str, int]]):
        cur = self._con.cursor()
        # todo check if any files exists in the database already
        self.strategy.add_files(cur, records)

    def add_file(self, file_name, data):
        cur = self._con.cursor()
        if not self.already_exists(cur, file_name, data):
            print(file_name)
            self.strategy.add_file(cur, file_name, data)
        else:
            print(f"skipping {file_name}")

    @functools.lru_cache()
    def already_exists(self, cur, file_name, data) -> bool:
        return self.strategy.record_exists(cur, file_name, data)

    def any_already_exists(self, file_names: typing.List[str]) -> typing.List[str]:
        cur = self._con.cursor()
        return self.strategy.any_already_exists(cur, file_names)
        # todo: any_already_exists

    def get_records(self):
        cur = self._con.cursor()
        yield from self.strategy.records(cur)

    def find_matches(self, file_names):
        cur = self._con.cursor()
        return self.strategy.find_matches(cur, file_names)
