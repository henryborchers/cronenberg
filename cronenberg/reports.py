import abc
import contextlib
import importlib.resources
import logging
import os
import pathlib
import shutil
import sqlite3
import time
import typing
from types import TracebackType
from typing import Optional, Type
import cronenberg
from cronenberg import database
import html

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

        buffer_size = 100

        logger = logging.getLogger('cronenberg')
        logger.debug(f"Pruning files {files}")
        cur = self._con.cursor()

        pruned: typing.Set[str] = set()

        buffer: typing.Set[typing.Tuple[str, str]] = set()
        for f in sorted(files, key=lambda x: x.lower()):
            path, file_name = os.path.split(f)
            buffer.add((path, file_name))
            if len(buffer) > buffer_size:
                removal_files = [
                    os.path.join(fn[0], fn[1]) for fn in sorted(buffer, key=lambda x: x[1])
                ]
                logger.debug(f"Removing from database: [{', '.join(removal_files)}]")
                cur.executemany(
                    """
                    DELETE FROM match_files WHERE path = ? AND name = ?
                    """,
                    buffer
                )
                pruned.update(buffer)
                buffer.clear()
        removal_files = [
                os.path.join(fn[0], fn[1]) for fn in sorted(buffer, key=lambda x: x[1])
            ]
        logger.debug(f"Removing from database: [{', '.join(removal_files)}]")
        cur.executemany(
            """
            DELETE FROM match_files WHERE path = ? AND name = ?
            """,
            buffer
        )
        pruned.update(buffer)
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
                    ORDER BY local_path ASC , mapped_files.name ASC  ;
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

class HTMLFormatter:
    def __init__(self, item_column_headings: typing.List[str], instance_column_headings: typing.List[str]):
        self._item_column_names: typing.List[str] = item_column_headings
        self._instance_columns: typing.List[str] = instance_column_headings


    def generate_table_header(self):
        return ''.join(
            [
                f"<th>{html.escape(heading)}</th>" for (i, heading) in
                enumerate(self._item_column_names + self._instance_columns)
            ]
        )

    def _generate_table(self, items):
        table_rows = []
        for row_i, (item, instances) in enumerate(items):
            item_row = ''.join(
                [f'<td class="item">{html.escape(str(value))}</td>' for value in item] +
                ['<td class="item emptycell" colspan="{len(self._instance_columns)}"></td>']
            )
            table_rows.append(f'<tr class="item">{item_row}</tr>')
            instance_rows = []
            for instance in instances:
                instance_rows.append(
                    f'<tr class="instance"><td class="instance emptycell" colspan="{len(self._item_column_names)}"></td><td class="instance">{html.escape(instance)}</td></tr>')
            table_rows += instance_rows
        tables_rows_html = '\n'.join([value for value in table_rows])
        return f"""<table cellspacing="0" cellpadding="0">
                    <tr>
                        {self.generate_table_header()}
                    </tr>
                {tables_rows_html}
                </table>
        """
    def _generate_page(self, content):
        return f"""<!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <link rel="stylesheet" type="text/css" href="styles.css" /> 
            <title>Title</title>
        </head>
        <body>
            <h1>Duplication Report</h1>
            {content}
        </body>
        </html>

        """

    def _iter_get_page_content(self, items):
        page_size = 1000
        page_content = []
        for i, item in enumerate(items):
            page_content.append(item)
            if i % page_size == 0 and i != 0:
                yield self._generate_table(page_content)
                page_content.clear()
        yield self._generate_table(page_content)

    def iter_items_to_pages(self, items):
        all_page_content = list(self._iter_get_page_content(items))
        header = self.generate_header(len(all_page_content))
        for content in all_page_content:
            yield self._generate_page(
                f"""<div>
                <div>{header}</div>
                <div>{content}</div>
                </div>
                """
            )
    def generate_header(self, number_of_pages):
        header_rows = []
        row_contents = []
        for i in range(number_of_pages):
            if i != 0 and i % 20 == 0:
                header_rows.append(f"{''.join([content for content in row_contents])}")
                row_contents.clear()
            row_contents.append(f'<div><a href="./page{i + 1}.html">[Page {i + 1}]</a></div>'.strip())
        header_rows.append(f"{''.join([content for content in row_contents])}")
        row_contents.clear()

        header = "".join(
            [f'<div class="row">{row}</div>' for row in header_rows]
        ).strip()
        return f'"<div class="header">{header}</div>"'
    def generate(self, items):
        content_pages = list(self.iter_items_to_pages(items))
        header =self.generate_header(len(content_pages))
        front_page_data = self._generate_page(
            f"""<div>
            {header}
</div>"""
        )

        for i, page in enumerate(content_pages):
            yield f"page{i + 1}.html", page
        yield "index.html", front_page_data

class HTMLOutputReport:

    def __init__(self, output_path) -> None:
        super().__init__()
        self.output_path = output_path
        self._item_column_names: typing.List[str] = []
        self._instance_columns: typing.List[str] = []
        self._items = []

    def generate(self):

        try:
            os.mkdir(self.output_path)
        except FileExistsError:
            pass

        with open(os.path.join(self.output_path, "styles.css"), "w", encoding="utf-8") as writer:
            writer.write(importlib.resources.files(cronenberg).joinpath('styles.css').read_text())

        formatter = HTMLFormatter(self._item_column_names, self._instance_columns)
        pages = formatter.generate(self._items)
        for page_name, page in pages:
            with open(os.path.join(self.output_path, page_name), "w", encoding="utf-8") as writer:
                writer.write(page)

    def add_record(self, item, instances):
        if len(item) != len(self._item_column_names):
            raise ValueError("item contents should match the number of column names")
        self._items.append((item, instances))

    def set_item_columns(self, *column_names: str):
        self._item_column_names = [name for name in column_names]

    def set_instance_columns(self, *column_names: str):
        self._instance_columns = [name for name in column_names]
