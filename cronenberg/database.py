import abc
import itertools
import os.path
import sqlite3
import functools
# from typing import Optional, Type
import typing
import contextlib
from cronenberg import recorder
DEFAULT_FILE_SYSTEM_MAP_DATA_SCHEME = recorder.DataSchema2()


class SQLiteReportWriter(contextlib.AbstractContextManager):
    def __init__(self, filename: str, schema_strategy):
        self.filename = filename
        self._con = None
        self.strategy: ReportDataSchema = schema_strategy

    def __enter__(self):

        self._con = sqlite3.connect(self.filename)
        self.init_tables()
        return self

    def __exit__(self, exc_type: typing.Optional[typing.Type[BaseException]],
                 exc_value: typing.Optional[BaseException],
                 traceback) -> typing.Optional[bool]:
        if self._con is not None:
            self._con.commit()
            self._con.close()
        return None

    def init_tables(self):

        cur = self._con.cursor()
        self.strategy.init_tables(cur)
        self._con.commit()

    def add_file_duplication_match(self, file_name, matches):
        # file_size = matches
        cur = self._con.cursor()
        try:
            for hash_value, instances in matches.items():
                file_sizes = {i.size for i in instances}
                if len(file_sizes) > 1:
                    raise AttributeError(f"All instances should have the same file size, got {file_sizes}")

                source = {i.source for i in instances}
                if len(source) > 1:
                    raise AttributeError(f"All instances should have the same source, got {source}")

                self.strategy.add_match(cur, file_name, file_sizes.pop(), hash_value, instances)
        finally:
            self._con.commit()


class ReportDataSchema(abc.ABC):
    @abc.abstractmethod
    def init_tables(self, cursor):
        pass

    @abc.abstractmethod
    def add_match(self, cursor, file_name, file_size, hash_value, matches):
        pass


class DupReportDataSchema(ReportDataSchema):
    def init_tables(self, cursor):
        cursor.execute('DROP TABLE IF EXISTS metadata')
        cursor.execute('CREATE TABLE metadata (version number)')
        cursor.execute('INSERT INTO metadata VALUES (1)')

        cursor.execute('DROP TABLE IF EXISTS files')
        cursor.execute('''
                    CREATE TABLE files
                    (name TEXT NOT NULL , size INTEGER NOT NULL , md5 TEXT NOT NULL, fileid INTEGER PRIMARY KEY )
                    ''')

        cursor.execute('DROP TABLE IF EXISTS file_instances')
        cursor.execute('''
                    CREATE TABLE file_instances(
                    file_source INTEGER, source text, path text,
                    FOREIGN KEY(file_source) REFERENCES files(fileid))
                    ''')

    def add_match(self, cursor, file_name, file_size, hash_value, matches):

        cursor.execute('INSERT INTO files(name,size,md5) VALUES (?,?,?)', (file_name, file_size, hash_value))
        file_id = cursor.lastrowid
        data = []
        for value in matches:
            data.append((file_id, value.source, value.path))
        cursor.executemany('INSERT INTO file_instances(file_source, source, path) VALUES (?,?,?)', data)

    @staticmethod
    @contextlib.contextmanager
    def _open_database(db_source):
        _conn = sqlite3.connect(db_source)
        yield _conn
        _conn.close()
    def get_dups_from_database_file(self, source):
        conn: sqlite3.Connection
        with self._open_database(source) as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("SELECT fileid, source, path, name, md5, size from file_instances JOIN main.files f on f.fileid = file_instances.file_source ORDER BY fileid")
                files_with_dups = cursor.fetchall()
                sorted_dups = itertools.groupby(files_with_dups, key=lambda x: x[0])
                for group_id, file_group in sorted_dups:
                    dups = []
                    for _group_id, source, path, name, hash_value, size in file_group:
                        dups.append((source, path))
                    yield (name, hash_value, size), dups
            finally:
                cursor.close()
        return []

    def remove_file_instance(self, database_file, source, path, file_name):
        conn: sqlite3.Connection
        with self._open_database(database_file) as conn:
            cursor = conn.cursor()
            try:
                pass
                cursor.execute("""DELETE FROM file_instances
    WHERE file_source IN (
      SELECT file_source FROM file_instances fi
      INNER JOIN files f
        ON (F.fileid = fi.file_source)
      WHERE path=? and name=? and source=?
    );
                """, (path, file_name, source))
            finally:
                cursor.close()


def update_dups_database_report(writer, file_name, matching_files):
    writer.add_file_duplication_match(file_name, matching_files)