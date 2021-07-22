__all__ = ['scan_path']

import abc
import os
import pathlib
import sys
import typing
import argparse
from hdscan import filescanner, recorder, reports

SYSTEM_FILES = [
    ".DS_Store",
    "Thumbs.db"
]


def scan_path(path: str) -> typing.Iterable[pathlib.Path]:
    for root, dirs, files in os.walk(path, followlinks=False):
        if ".git" in dirs:
            dirs.remove(".git")
        dirs.sort()
        files.sort()
        for f in files:
            if f in SYSTEM_FILES:
                continue
            file_path = pathlib.Path(os.path.join(root, f))
            if file_path.is_symlink():
                continue
            yield file_path


def get_arg_parser():
    parser = argparse.ArgumentParser()
    subparser = parser.add_subparsers(dest='command')
    map_parser = subparser.add_parser("map", help="make a mapping of a file path")
    map_parser.add_argument("root", help="starting point")
    map_parser.add_argument("outputfile", help="database file to store to")

    map_parser = subparser.add_parser("dups", help="Find duplicates")
    map_parser.add_argument("root", help="starting point")
    map_parser.add_argument("mapfile", help="database file to compare against")
    map_parser.add_argument("--output_file", default=None, help="output file")

    return parser


class Command(abc.ABC):
    @abc.abstractmethod
    def __init__(self, args):
        pass
    @abc.abstractmethod
    def execute(self):
        pass


class DupsPath(Command):

    def __init__(self, args):
        self.map_file = args.mapfile
        self.root = args.root
        self.output_file = args.output_file


    @staticmethod
    def get_records(map_file):
        with recorder.SQLiteWriter(
                filename=map_file,
                schema_strategy=recorder.DataSchema1()) as reader:
            existing_files = set()
            for i, (file_name, file_path, _) in enumerate(reader.get_records()):
                existing_files.add(os.path.join(file_path, file_name))
            print(f"loaded {len(existing_files)} records")
            return existing_files

    def execute(self):

        with recorder.SQLiteWriter(
                filename=self.map_file,
                schema_strategy=recorder.DataSchema1()) as reader:
            with reports.DuplicateReportSqlite(self.output_file) as report_writer:
            # with reports.DuplicateReportCSV(self.output_file) as report_writer:
                for f in scan_path(self.root):
                    print(f)
                    matches = reader.find_matches(f)
                    if len(matches) > 0:
                        print(f"Found duplicate for {f}: {matches}", file=sys.stderr)
                        report_writer.add_duplicates(f, matches)



class MapPath(Command):
    def __init__(self, args):
        self.output_file = args.outputfile
        self.root = args.root

    def execute(self):
        output_files = self.output_file
        if not os.path.exists(output_files):
            with recorder.SQLiteWriter(
                    filename=output_files,
                    schema_strategy=recorder.DataSchema1()) as writer:
                writer = typing.cast(recorder.SQLiteWriter, writer)
                print("initing the tables")
                writer.init_tables()

        with recorder.SQLiteWriter(
                filename=output_files,
                schema_strategy=recorder.DataSchema1()) as writer:
            existing_files = set()
            for i, (file_name, file_path, _) in enumerate(writer.get_records()):
                existing_files.add(os.path.join(file_path, file_name))
            print(f"loaded {len(existing_files)} records")
            writer = typing.cast(recorder.SQLiteWriter, writer)

            buffer = []
            try:
                for f in scan_path(self.root):
                    if str(f.relative_to(self.root)) in existing_files:
                        print(f"Skipping {f.relative_to(self.root)}")
                        continue
                    data = filescanner.scan_file(self.root, f)
                    if data.size == 0:
                        continue

                    print(f.relative_to(self.root))

                    buffer.append((f.name, data.path, data.size))
                    if len(buffer) > 100:
                        writer.add_files(buffer)
                        buffer.clear()
            finally:
                writer.add_files(buffer)


def main(argv: typing.Optional[typing.List[str]] = None):
    argv = argv or sys.argv
    parser = get_arg_parser()
    args = parser.parse_args(argv[1:])

    commands: typing.Dict[str, typing.Type[Command]] = {
        "map": MapPath,
        "dups": DupsPath
    }
    command = commands.get(args.command)
    if command is None:
        print(f"Unknown command: {args.command}", file=sys.stderr)
        sys.exit(1)
    command(args).execute()
    # if args.command == "map":
        # output_files = args.outputfile
        # if not os.path.exists(output_files):
        #     with recorder.SQLiteWriter(
        #             filename=output_files,
        #             schema_strategy=recorder.DataSchema1()) as writer:
        #         writer = typing.cast(recorder.SQLiteWriter, writer)
        #         print("initing the tables")
        #         writer.init_tables()
        #
        # with recorder.SQLiteWriter(
        #         filename=output_files,
        #         schema_strategy=recorder.DataSchema1()) as writer:
        #     existing_files = set()
        #     for i, (file_name, file_path, _) in enumerate(writer.get_records()):
        #         existing_files.add(os.path.join(file_path, file_name))
        #     print(f"loaded {len(existing_files)} records")
        #     writer = typing.cast(recorder.SQLiteWriter, writer)
        #
        #     buffer = []
        #     try:
        #         for f in scan_path(args.root):
        #             if str(f.relative_to(args.root)) in existing_files:
        #                 print(f"Skipping {f.relative_to(args.root)}")
        #                 continue
        #             data = filescanner.scan_file(args.root, f)
        #             print(f.relative_to(args.root))
        #
        #             buffer.append((f.name, data.path, data.size))
        #             if len(buffer) > 100:
        #                 writer.add_files(buffer)
        #                 buffer.clear()
        #     finally:
        #         writer.add_files(buffer)
