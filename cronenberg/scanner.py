
import abc
import os
import pathlib
import sys
import typing
import argparse
import json
from cronenberg import filescanner, recorder, reports
import logging

__all__ = ['PathScanner']

logging.getLogger('cronenberg').addHandler(logging.NullHandler())

SYSTEM_FILES = [
    ".DS_Store",
    "._.DS_Store",
    "Thumbs.db",

]

DEFAULT_DATA_SCHEME = recorder.DataSchema2()


def get_skippable_directories(suppression_file):
    with open(suppression_file) as file_handle:
        data = json.loads(file_handle.read())
        return data['ignore_recursive']


class PathScanner:
    def __init__(self):
        self.slipped_paths = set()

    def scan_path(self, path: str) -> typing.Iterable[pathlib.Path]:
        for root, dirs, files in os.walk(path, followlinks=False):
            if any(root.startswith(s) for s in self.slipped_paths):
                continue
            if ".git" in dirs:
                dirs.remove(".git")
            for sub_directory in dirs:
                if sub_directory in self.slipped_paths or \
                        os.path.join(path, sub_directory) in self.slipped_paths:
                    dirs.remove(sub_directory)
            dirs.sort()
            files.sort()
            for f in files:
                if f in SYSTEM_FILES:
                    continue
                file_path = pathlib.Path(os.path.join(root, f))
                if file_path.is_symlink():
                    continue
                yield file_path


class CommandParserBuilder(abc.ABC):
    def __init__(self, root):
        self.root = root

    @abc.abstractmethod
    def build_command_subparser(self) -> argparse.ArgumentParser:
        """Build and return newly create ArgumentParser"""


class MapParserBuilder(CommandParserBuilder):

    @classmethod
    def _build_create_command(cls, map_command_parser):
        create_command = map_command_parser.add_parser("create")

        create_command.add_argument("root", help="starting point")

        create_command.add_argument("outputfile",
                                    help="database file to store to")

        create_command.add_argument('--suppression_file',
                                    default=None,
                                    help="Json file for suppressing searches")

        create_command.add_argument("--append", default=False,
                                    action='store_true')

    def build_command_subparser(self) -> argparse.ArgumentParser:
        map_parser = self.root.add_parser(
            "map",
            help="make a mapping of a file path"
        )

        map_command_parser = map_parser.add_subparsers(dest="map_commands")
        self._build_create_command(map_command_parser)
        return map_parser


class DupsParserBuilder(CommandParserBuilder):

    @classmethod
    def _build_locate_command(cls, command_parser):
        create_command = command_parser.add_parser("locate")

        create_command.add_argument("root", help="starting point")

        create_command.add_argument(
            "--mapfile",
            action="extend",
            nargs="+",
            help="database file to compare against")

        create_command.add_argument(
            "--output_file",
            default=None,
            help="output file"
        )

        create_command.add_argument(
            '--suppression_file',
            default=None,
            help="Json file for suppressing searches"
        )

    @classmethod
    def _build_prune_command(cls, command_parser) -> None:
        create_command = command_parser.add_parser("prune")

        create_command.add_argument(
            "dups_file",
            help="file containing duplication"
        )

    def build_command_subparser(self) -> argparse.ArgumentParser:
        dup_parser = self.root.add_parser("dups", help="Find duplicates")
        dup_command_parser = dup_parser.add_subparsers(dest="dups_command")
        self._build_locate_command(dup_command_parser)
        self._build_prune_command(dup_command_parser)

        return dup_parser


class ParserCreator:

    def _build_map_parser(self, root_subparser) -> argparse.ArgumentParser:
        builder = MapParserBuilder(root_subparser)
        return builder.build_command_subparser()

    def _build_dups_parser(self, root_subparser) -> argparse.ArgumentParser:
        builder = DupsParserBuilder(root_subparser)
        return builder.build_command_subparser()

    def get(self) -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser()
        subparser = parser.add_subparsers(dest='command')
        self._build_map_parser(subparser)
        self._build_dups_parser(subparser)

        return parser


def get_arg_parser():
    builder = ParserCreator()
    return builder.get()


class Command(abc.ABC):
    @abc.abstractmethod
    def __init__(self, args):
        pass

    @abc.abstractmethod
    def execute(self):
        pass


class DupsPath(Command):

    def __init__(self, args):
        self.command = args.dups_command

        if args.dups_command == "locate":
            self.map_files = args.mapfile
            self.root = args.root
            self.output_file = args.output_file
            self._suppression_file = args.suppression_file
        elif args.dups_command == "prune":
            self.dups_file = args.dups_file

    def _prune(self):
        logger = logging.getLogger('cronenberg')
        logger.debug("Pruning dups file")
        files_no_longer_existing = set()

        with reports.DuplicateReportSqlite(
                self.dups_file) as report:
            logger.debug("Locating dups entries to prune")
            for r in report.duplicates():
                if not os.path.exists(r.local_file):
                    logger.debug(f"Unable to locate: {r.local_file}")
                    files_no_longer_existing.add(r.local_file)

            if files_no_longer_existing:
                pruned_files = report.remove_local_files(
                    files_no_longer_existing
                )

                logger.info(
                    "Pruned %d entries from dups database",
                    len(pruned_files)
                )
            else:
                logger.info(
                    "No entries from dups database needed to be pruned"
                )

    def _locate(self):
        with recorder.SQLiteReader(
                self.map_files,
                schema_strategy=DEFAULT_DATA_SCHEME
        ) as reader:
            with reports.DuplicateReportSqlite(
                    self.output_file) as report_writer:
                report_writer.init_tables()
                scanner = PathScanner()
                if self._suppression_file is not None and \
                        os.path.exists(self._suppression_file):
                    print("Using suppression file")
                    for skipped_dir in get_skippable_directories(
                            self._suppression_file):
                        print(f"Adding: {skipped_dir}")
                        scanner.slipped_paths.add(skipped_dir)
                for f in scanner.scan_path(self.root):
                    print(f)
                    matches = [m for m in reader.find_matches(f)]
                    if matches:
                        print(f"Found duplicate for {f}: {matches}",
                              file=sys.stderr)

                        report_writer.add_duplicates(
                            f,
                            [
                                os.path.join(m[0], m[1]) for m in
                                reader.find_matches(f)
                            ]
                        )

    @staticmethod
    def get_records(map_file):
        with recorder.SQLiteWriter(
                filename=map_file,
                schema_strategy=DEFAULT_DATA_SCHEME) as reader:
            existing_files = set()
            for i, (file_name, file_path, _) in enumerate(reader.get_records()):
                existing_files.add(os.path.join(file_path, file_name))
            print(f"loaded {len(existing_files)} records")
            return existing_files

    def execute(self):
        sub_commands = {
            "locate": self._locate,
            "prune": self._prune
        }
        sub_command = sub_commands.get(self.command)
        if sub_command is None:
            raise KeyError(f"Invalid subcommand for dups: {self.command}")
        sub_command()



class MapPath(Command):
    def __init__(self, args):
        self.output_file = args.outputfile
        self.root = args.root
        self._suppression_file = args.suppression_file
        self._append = args.append
        # self._suppression_file = SUPPRESSION_FILE

    def execute(self):
        output_files = self.output_file
        if self._append is False and not os.path.exists(output_files):
            with recorder.SQLiteWriter(
                    filename=output_files,
                    schema_strategy=DEFAULT_DATA_SCHEME) as writer:
                writer = typing.cast(recorder.SQLiteWriter, writer)
                print("initing the tables")
                writer.init_tables()

        with recorder.SQLiteWriter(
                filename=output_files,
                schema_strategy=DEFAULT_DATA_SCHEME) as writer:
            existing_files = set()
            for i, (file_name, file_path, _) in enumerate(writer.get_records()):
                existing_files.add(os.path.join(file_path, file_name))
            print(f"loaded {len(existing_files)} records")
            writer = typing.cast(recorder.SQLiteWriter, writer)

            buffer = []
            try:
                scanner = PathScanner()
                if self._suppression_file is not None and \
                        os.path.exists(self._suppression_file):
                    print("Using suppression file")
                    for skipped_dir in get_skippable_directories(
                            self._suppression_file):
                        scanner.slipped_paths.add(skipped_dir)
                for f in scanner.scan_path(self.root):
                    if str(f.relative_to(self.root)) in existing_files or \
                            not os.path.exists(f):
                        print(f"Skipping {f.relative_to(self.root)}")
                        continue
                    data = filescanner.scan_file(self.root, f)
                    if data.size == 0:
                        continue

                    print(f.relative_to(self.root))

                    buffer.append((self.root, f.name, data.path, data.size))
                    if len(buffer) > 100:
                        writer.add_files(buffer, source=self.root)
                        buffer.clear()
            finally:
                writer.add_files(buffer, source=self.root)


def main(argv: typing.Optional[typing.List[str]] = None):
    argv = argv or sys.argv
    parser = get_arg_parser()
    args = parser.parse_args(argv[1:])

    set_logging()

    commands: typing.Dict[str, typing.Type[Command]] = {
        "map": MapPath,
        "dups": DupsPath
    }
    command = commands.get(args.command)
    if command is None:
        print(f"Unknown command: {args.command}", file=sys.stderr)
        sys.exit(1)
    command(args).execute()


def set_logging():
    logger = logging.getLogger("cronenberg")
    logger.setLevel(logging.DEBUG)
    log_handler = logging.StreamHandler(stream=sys.stdout)
    log_handler.setLevel(logging.DEBUG)
    logger.addHandler(log_handler)
