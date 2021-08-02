__all__ = ["add_to_csv_file"]

import hashlib
import os
import pathlib
import sqlite3
import typing
import csv
import contextlib
from time import sleep
from typing import Optional, Type
import abc
import functools


def add_to_csv_file(file_name: str, data: typing.Mapping[str, typing.Any]) -> None:
    with open(file_name, "a") as f:
        writer = csv.writer(f)
        writer.writerow(data.values())


class DataSchema(abc.ABC):

    @abc.abstractmethod
    def init_tables(self, cursor: sqlite3.Cursor):
        """Create new tables."""

    @abc.abstractmethod
    def add_file(self, cursor: sqlite3.Cursor, file_name: str, data):
        """Generate new file entry"""

    @abc.abstractmethod
    def add_files(self,
                  cursor: sqlite3.Cursor,
                  records: typing.List[typing.Tuple[str, str, int]],
                  source=None):
        pass

    @abc.abstractmethod
    def record_exists(self, cursor: sqlite3.Cursor, file_name: str, data) -> bool:
        """Check if record already exists"""

    @abc.abstractmethod
    def any_already_exists(self, cursor: sqlite3.Cursor, file_names: typing.List[str]) -> typing.List[str]:
        """Find matching records"""

    @abc.abstractmethod
    def records(self, cursor: sqlite3.Cursor) -> typing.Iterator[typing.List[typing.Tuple[str, str, int]]]:
        """Return all the records"""

    @abc.abstractmethod
    def find_matches(self, cur: sqlite3.Cursor, file_name: str) -> typing.List[str]:
        pass


class DataSchema1(DataSchema):

    def find_matches(self, cursor: sqlite3.Cursor, file_name: str) -> typing.Set[str]:
        stats = os.stat(file_name)
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

    def add_files(self,
                  cursor,
                  records: typing.List[typing.Tuple[str, str, int]],
                  source=None):
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


class DataSchema2(DataSchema1):
    pass

    def init_tables(self, cursor: sqlite3.Cursor):
        cursor.execute('DROP TABLE IF EXISTS metadata')
        cursor.execute('CREATE TABLE metadata (version number)')
        cursor.execute('INSERT INTO metadata VALUES (2)')

        cursor.execute('DROP TABLE IF EXISTS files')
        cursor.execute('''
                    CREATE TABLE files
                    (source text, name text, path text, size number, md5 text)
                    ''')


    def add_files(self, cursor: sqlite3.Cursor,
                  records: typing.List[typing.Tuple[str, str, str, int]],
                  source=None):

        cursor.executemany(
            '''
            INSERT INTO files (source, name, path, size) 
            VALUES (?, ?, ?, ?)
            ''',
            records
        )
    #

    def records(self, cursor: sqlite3.Cursor) -> typing.Iterator[
        typing.List[typing.Tuple[str, str, int]]]:

        yield from cursor.execute(
            "SELECT name, path, size FROM files ORDER BY path "
        )

    def find_matches(self, cursor: sqlite3.Cursor, file_name: str) -> typing.Set[typing.Tuple[str, str]]:
        comparison = FileNameSizeMd5Comparison(cursor)
        return comparison.find_matches(file_name)
        #
        # stats = os.stat(file_name)
        # file_path = pathlib.Path(file_name)
        # cursor.execute(
        #     'SELECT name,path, size FROM files WHERE name = ? AND size = ?',
        #     (file_path.name, stats.st_size)
        # )
        # matches: typing.Set[str] = set()
        # for match_file_name, match_path, match_size in cursor.fetchall():
        #     matches.add(os.path.join(match_path, match_file_name))
        # return matches


class SQLiteReader(contextlib.AbstractContextManager):
    def __init__(self, filenames: typing.List[str], schema_strategy):
        self.filenames = filenames
        self._con = None
        self.strategy: DataSchema = schema_strategy

    def __enter__(self):
        self._con = [sqlite3.connect(filename) for filename in self.filenames]
        return self

    def __exit__(self, exc_type: Optional[Type[BaseException]],
                 exc_value: Optional[BaseException],
                 traceback) -> Optional[bool]:
        if self._con is not None:
            for s in self._con:
                s.commit()
                s.close()
        return None

    def find_matches(self, file_names):
        matches = []
        for con in self._con:
            cur = con.cursor()
            matches += self.strategy.find_matches(cur, file_names)

        return matches

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
                  records: typing.List[typing.Tuple[str, str, int]],
                  source: typing.Optional[str] = None
                  ):
        cur = self._con.cursor()
        # todo check if any files exists in the database already
        self.strategy.add_files(cur, records, source)

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
        for r in self.strategy.records(cur):
            yield r
        # yield from self.strategy.records(cur)

    def find_matches(self, file_names):
        cur = self._con.cursor()
        return self.strategy.find_matches(cur, file_names)

class AbsFileMatchFinderStrategy(abc.ABC):
    @abc.abstractmethod
    def find_matches(self, file_name: str) -> typing.Set[str]:
        pass


class FileNameSizeComparison(AbsFileMatchFinderStrategy):
    def __init__(self, cursor):
        self.cursor = cursor

    def find_matches(self, file_name: str) -> typing.Set[str]:
        stats = os.stat(file_name)
        file_path = pathlib.Path(file_name)
        self.cursor.execute(
            'SELECT name,path, size FROM files WHERE name = ? AND size = ?',
            (file_path.name, stats.st_size)
        )
        matches: typing.Set[str] = set()
        for match_file_name, match_path, match_size in self.cursor.fetchall():
            matches.add(os.path.join(match_path, match_file_name))
        return matches


class FileNameSizeMd5Comparison(AbsFileMatchFinderStrategy):
    def __init__(self, cursor):
        self.cursor = cursor

    def find_matches(self, file_name: str) -> typing.Set[typing.Tuple[str, str]]:
        stats = os.stat(file_name)
        file_path = pathlib.Path(file_name)

        file_md5 = None
        matches: typing.Set[typing.Tuple[str, str]] = set()
        for match_source, match_file_name, match_path, match_size, match_md5 in self.cursor.execute(
                '''
                SELECT source, name, path, size, md5 
                FROM files 
                WHERE name = ? AND size = ?
                ''',
                (file_path.name, stats.st_size)
        ):
            if match_md5 is None:
                if \
                        not os.path.exists(os.path.join(match_source, match_path, match_file_name)) or \
                        not os.path.isfile(os.path.join(match_source, match_path, match_file_name)):
                    continue
                match_md5 = self.get_md5(os.path.join(match_source, match_path, match_file_name))

                self.update_match_hash(match_path, match_file_name, match_md5)

            try:
                if file_md5 is None:
                    file_md5 = self.get_md5(file_name)
            except PermissionError as e:
                print(f"unable to validate {e.filename}")
                return set()
            if match_md5 == file_md5:
                matches.add((match_source, os.path.join(match_path, match_file_name)))

        return matches

    def update_match_hash(self, path, file_name, md5_hash):
        update_attempts = 2
        for attempt_number in range(update_attempts):
            try:
                self.cursor.execute(
                    '''
                    UPDATE files
                    SET md5 = ?
                    WHERE path=? AND name=?
                    ''',
                    (md5_hash, path, file_name)
                )
                # self.cursor.commit()
                break
            except sqlite3.OperationalError as e:
                if attempt_number + 1 < update_attempts:
                    print(e)
                    print("Sleeping for 1 second and trying again")
                    sleep(1)
                else:
                    print(f"Unable cache hash value for {file_name}")

    @staticmethod
    def get_md5(file_path: str) -> str:
        assert os.path.exists(file_path), file_path
        assert os.path.isfile(file_path), file_path
        print(f"Calculating md5 for {file_path}")
        with open(file_path, "rb") as f:
            file_hash = hashlib.md5()
            while chunk := f.read(8192):
                file_hash.update(chunk)

        return file_hash.hexdigest()
