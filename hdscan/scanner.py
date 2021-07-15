__all__ = ['scan_path']

import os
import pathlib
import sys
import typing
import argparse
from hdscan import filescanner, recorder


def scan_path(path):
    for root, dirs, files in os.walk(path):
        if ".git" in dirs:
            dirs.remove(".git")
        dirs.sort()
        files.sort()
        for f in files:
            yield pathlib.Path(os.path.join(root, f))


def get_arg_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("root", help="starting point")
    parser.add_argument("outputfile", help="database file to store to")
    return parser


def main(argv: typing.Optional[typing.List[str]] = None):
    argv = argv or sys.argv
    parser = get_arg_parser()
    args = parser.parse_args(argv[1:])
    output_files = args.outputfile

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
            for f in scan_path(args.root):
                if str(f.relative_to(args.root)) in existing_files:
                    print(f"Skipping {f.relative_to(args.root)}")
                    continue
                data = filescanner.scan_file(args.root, f)
                print(f.relative_to(args.root))

                buffer.append((f.name, data.path, data.size))
                if len(buffer) > 100:
                    writer.add_files(buffer)
                    buffer.clear()
        finally:
            writer.add_files(buffer)
