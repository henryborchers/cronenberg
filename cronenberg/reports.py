import abc
import contextlib
import logging
import os
import pathlib
import sqlite3
import time
import typing
from types import TracebackType
from typing import Optional, Type


class DuplicateReportGenerator(contextlib.AbstractContextManager):
    def __init__(self, filename: str):
        self.filename = filename

    @abc.abstractmethod
    def add_duplicates(self, source, duplicates):
        pass


class DuplicateReportSqlite(DuplicateReportGenerator):

    class Record(typing.NamedTuple):
        filename: str
        local_file: str
        mapped_file: str

    def __init__(self, filename: str, ):
        super().__init__(filename)
        self._con = None

        # self.strategy

    def remove_local_files(
            self,
            files: typing.Iterable[str]
    ) -> typing.Set[str]:

        logger = logging.getLogger('cronenberg')
        logger.debug(f"Pruning files {files}")
        cur = self._con.cursor()

        pruned: typing.Set[str] = set()
        for f in files:
            logger.debug(f"Removing from database: {f}")
            path, file_name = os.path.split(f)
            cur.execute(
                """
                DELETE FROM match_files WHERE path = ? AND name = ?
                """,
                (path, file_name)
            )
            pruned.add(f)
        return pruned

    def duplicates(self) -> typing.Iterable[Record]:
        logger = logging.getLogger('cronenberg')
        cur = self._con.cursor()
        logger.debug("Retrieving records of duplicates")
        cur.execute(
            '''
            SELECT COUNT(*)
            FROM mapped_files join match_files mf on mapped_files.match_id = mf.ROWID
            '''
        )
        s = cur.fetchone()
        number_of_matches = s[0]
        start_time = time.time()

        for i, result in enumerate(
                cur.execute(
                    '''
                    SELECT 
                        mapped_files.name, 
                        mf.path as local_path, 
                        mapped_files.path as network_files, 
                        size
                    FROM mapped_files join match_files mf on mapped_files.match_id = mf.ROWID
                    ORDER BY size desc ;
                    '''
            )
        ):

            yield DuplicateReportSqlite.Record(
                filename=result[0],
                local_file=os.path.join(result[1], result[0]),
                mapped_file=os.path.join(result[2], result[0]),
            )
            if (i + 1) % int(number_of_matches/10) == 0 or \
                    number_of_matches == i + 1 or \
                    time.time() - start_time > 1:

                completed = ((i + 1) / number_of_matches) * 100

                logger.debug(
                    f"Retrieving records of duplicates: "
                    f"({i+1} / {number_of_matches}): {completed:.3f}%"
                )

                start_time = time.time()
        cur.close()


    def init_tables(self):
        cur = self._con.cursor()
        cur.execute('DROP TABLE IF EXISTS match_files')
        cur.execute('DROP TABLE IF EXISTS mapped_files')
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
        # if os.path.exists(self.filename):
        #     os.remove(self.filename)
        self._con = sqlite3.connect(self.filename)

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