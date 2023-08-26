import abc
import itertools
import sqlite3
import typing
import os
import logging
import json
import warnings
from pprint import pprint

from cronenberg import recorder, reports
from cronenberg.path_scanner import PathScanner
from cronenberg.database import DEFAULT_FILE_SYSTEM_MAP_DATA_SCHEME, update_dups_database_report, SQLiteReportWriter, DupReportDataSchema

logger = logging.getLogger(__name__)


def get_skippable_directories(suppression_file):
    with open(suppression_file) as file_handle:
        data = json.loads(file_handle.read())
        return data['ignore_recursive']


class AbsLocateCommand(abc.ABC):
    @abc.abstractmethod
    def run(self) -> None:
        """Run Locate Command"""


class Locate1(AbsLocateCommand):
    def __init__(self, root: str, output_file: str, map_files: typing.List[str], suppression_file=None):
        self._suppression_file = suppression_file
        self.root = root
        self.map_files = map_files
        self.output_file = output_file

    def run(self):
        with recorder.SQLiteReader(
                self.map_files,
                schema_strategy=DEFAULT_FILE_SYSTEM_MAP_DATA_SCHEME
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
                    logger.info("Checking %s", f)
                    matches = [os.path.join(*m) for m in reader.find_matches(f)]
                    if matches:
                        matches_text = "\n".join([f"----> {line}" for line in sorted(matches)])
                        logger.info("Found duplicate for %s: \n%s\n", f.name, matches_text)
                        # logger.info(f"Found duplicate for {f.name}: \n{matches_text}\n")
                        report_writer.add_duplicates(f, matches)


class FileDup(typing.NamedTuple):
    # source: str
    # path: str
    name: str
    size: int
    # md5: str
    count: int

class FileInstance(typing.NamedTuple):
    source: str
    path: str
    name: str
    size: int
    md5: str
    # count: int
def update_hash_value(con, file: FileInstance, hash_value: str):
    con.execute(
        '''
        UPDATE files
        SET md5 = ?
        WHERE path=? AND name=?
        ''',
        (hash_value, file.path, file.name)
    )
    con.commit()
class Locate2(AbsLocateCommand):
    def __init__(self, root: str, output_file: str, map_files: typing.List[str], suppression_file=None):
        self._suppression_file = suppression_file
        self.root = root
        self.map_files = map_files
        self.output_file = output_file

    def _iter_dups(self, con):
        cur = con.cursor()
        for result in cur.execute(
                'SELECT name, size, COUNT (*) as "Count" '
                'FROM files GROUP BY name, size '
                'HAVING Count(*) > 1'
        ):
            yield FileDup(*result)

    def run(self) -> None:
        matches_results = self.locate_duplicate_files()

        print("\n\n\n")
        print(f"----------------------------------------------------------------------------------------")
        print("Final Result")
        print(f"----------------------------------------------------------------------------------------")
        for file_name, variations in matches_results.items():
            print(f"\n")
            for variation_key, variation_instances in variations.items():
                # print(variation_key)
                print(f"\"{file_name}\" ({variation_key})")
                for variation_instance in variation_instances:
                    x = os.path.join(variation_instance.path, variation_instance.name)
                    print(f"---> {x}")
        print(f"\n=========================================================================================")
    def locate_duplicate_files(self):
        matches_results = {}
        # self.output_file
        with SQLiteReportWriter(self.output_file, schema_strategy=DupReportDataSchema()) as writer:
            for key, value in self._iter_dup_files():
                update_dups_database_report(writer, key, value)
                matches_results[key] = value
        return matches_results

    def _iter_dup_files(self):
        with recorder.SQLiteReader(
                self.map_files,
                schema_strategy=DEFAULT_FILE_SYSTEM_MAP_DATA_SCHEME
        ) as reader:
            for con in reader._con:
                files_with_possible_dups = list(self._iter_dups(con))
                for i, (file_name, file_size, matches) in enumerate(files_with_possible_dups):
                    percent_done = i / len(files_with_possible_dups)
                    print(f"\nLocating duplicates for {file_name} {(percent_done * 100):.3f}%")
                    matches = list(self._find_matches_based_on_name_and_size(file_name, file_size, con))
                    exact_files_matching_result = self.compare(matches,
                                                               lambda *args, con=con: update_hash_value(con, *args))
                    for variation_key, variation_instances in exact_files_matching_result.items():
                        print(f"\"{file_name}\" ({variation_key})")
                        for variation_instance in variation_instances:
                            x = os.path.join(variation_instance.path, variation_instance.name)
                            print(f"---> {x}")

                    yield file_name, exact_files_matching_result

    def compare(
            self,
            candidates: typing.List[FileInstance],
            update_value:  typing.Optional[typing.Callable[[FileInstance, str], None]] = None
    ):
        if len(candidates) < 2:
            raise ValueError("Needs more than one candidate")

        candidates_resolved = []
        for candidate in candidates:
            if candidate.md5 is None:
                try:
                    hash_value = self.get_hash_value(os.path.join(candidate.source, candidate.path, candidate.name))
                except FileNotFoundError as e:
                    warnings.warn(f"{e} not found")
                    continue
                # candidate.md5 = hash_value
                if update_value:
                    update_value(candidate, hash_value)
                new_candidate = FileInstance(
                    source=candidate.source,
                    path=candidate.path,
                    name=candidate.name,
                    size=candidate.size,
                    md5=hash_value
                )
                candidates_resolved.append(new_candidate)
            else:
                candidates_resolved.append(candidate)
        return {
            key: list(value) for key, value
            in itertools.groupby(
                sorted(candidates_resolved, key=lambda x: x.md5), key=lambda x: x.md5)
        }
    def get_hash_value(self, file_path: str):
        return recorder.FileNameSizeMd5Comparison.get_md5(file_path)
    def _find_matches_based_on_name_and_size(self, file_name: str, file_size: int, con):
        cur = con.cursor()
        for result in cur.execute(
            'SELECT source, path, name, size, md5 FROM files WHERE name = ? AND size = ?', (file_name, file_size)
        ):
            yield FileInstance(*result)