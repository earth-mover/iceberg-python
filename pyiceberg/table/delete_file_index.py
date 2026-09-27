# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

from bisect import bisect_left, bisect_right

from pyiceberg.expressions import EqualTo
from pyiceberg.expressions.visitors import _InclusiveMetricsEvaluator
from pyiceberg.manifest import (
    INITIAL_SEQUENCE_NUMBER,
    POSITIONAL_DELETE_SCHEMA,
    DataFile,
    DataFileContent,
    FileFormat,
    ManifestEntry,
)
from pyiceberg.table.delete_file import DeleteFileSet
from pyiceberg.typedef import Record

PATH_FIELD_ID = 2147483546


class PositionDeletes:
    """Collects position delete files and indexes them by sequence number."""

    __slots__ = ("_buffer", "_seqs", "_files")

    def __init__(self) -> None:
        self._buffer: list[tuple[DataFile, int]] | None = []
        self._seqs: list[int] = []
        self._files: list[tuple[DataFile, int]] = []

    def add(self, delete_file: DataFile, seq_num: int) -> None:
        if self._buffer is None:
            raise ValueError("Cannot add files after indexing")
        self._buffer.append((delete_file, seq_num))

    def _ensure_indexed(self) -> None:
        if self._buffer is not None:
            self._files = sorted(self._buffer, key=lambda file: file[1])
            self._seqs = [seq for _, seq in self._files]
            self._buffer = None

    def filter_by_seq(self, seq: int) -> list[DataFile]:
        return [delete_file for delete_file, _ in self.filter_entries_by_seq(seq)]

    def filter_entries_by_seq(self, seq: int) -> list[tuple[DataFile, int]]:
        """Return the delete files, with their sequence numbers, that apply to data with the given sequence number."""
        self._ensure_indexed()
        if not self._files:
            return []
        start_idx = bisect_left(self._seqs, seq)
        return self._files[start_idx:]

    def referenced_delete_files(self) -> list[DataFile]:
        self._ensure_indexed()
        return [data_file for data_file, _ in self._files]


class EqualityDeletes(PositionDeletes):
    """Collects equality delete files and indexes them by sequence number."""

    __slots__ = ()

    def filter_entries_by_seq(self, seq: int) -> list[tuple[DataFile, int]]:
        """Return the delete files that apply to data with the given sequence number.

        Equality deletes only apply to data files with a strictly lower data sequence number.
        """
        self._ensure_indexed()
        if not self._files:
            return []
        start_idx = bisect_right(self._seqs, seq)
        return self._files[start_idx:]


def _has_path_bounds(delete_file: DataFile) -> bool:
    lower = delete_file.lower_bounds
    upper = delete_file.upper_bounds
    if not lower or not upper:
        return False

    return PATH_FIELD_ID in lower and PATH_FIELD_ID in upper


def _applies_to_data_file(delete_file: DataFile, data_file: DataFile) -> bool:
    if not _has_path_bounds(delete_file):
        return True

    evaluator = _InclusiveMetricsEvaluator(POSITIONAL_DELETE_SCHEMA, EqualTo("file_path", data_file.file_path))
    return evaluator.eval(delete_file)


def _referenced_data_file_path(delete_file: DataFile) -> str | None:
    """Return the path, if the path bounds evaluate to the same location."""
    lower_bounds = delete_file.lower_bounds
    upper_bounds = delete_file.upper_bounds

    if not lower_bounds or not upper_bounds:
        return None

    lower = lower_bounds.get(PATH_FIELD_ID)
    upper = upper_bounds.get(PATH_FIELD_ID)

    if lower and upper and lower == upper:
        try:
            return lower.decode("utf-8")
        except (UnicodeDecodeError, AttributeError):
            pass

    return None


def _is_deletion_vector(delete_file: DataFile) -> bool:
    return delete_file.file_format == FileFormat.PUFFIN


def _partition_key(spec_id: int, partition: Record | None) -> tuple[int, Record]:
    if partition:
        return spec_id, partition
    return spec_id, Record()  # unpartitioned handling


class DeleteFileIndex:
    """Indexes delete files for scan planning.

    Deletion vectors are indexed by referenced data file, position delete files by partition and by exact
    data file path, and equality delete files by partition or globally when written with an unpartitioned spec.
    """

    def __init__(self) -> None:
        self._by_partition: dict[tuple[int, Record], PositionDeletes] = {}
        self._by_path: dict[str, PositionDeletes] = {}
        self._dvs_by_path: dict[str, PositionDeletes] = {}
        self._eq_by_partition: dict[tuple[int, Record], EqualityDeletes] = {}
        self._eq_global: EqualityDeletes = EqualityDeletes()
        self._has_global_eq_deletes = False

    def is_empty(self) -> bool:
        return (
            not self._by_partition
            and not self._by_path
            and not self._dvs_by_path
            and not self._eq_by_partition
            and not self._has_global_eq_deletes
        )

    def add_delete_file(self, manifest_entry: ManifestEntry, partition_key: Record | None = None) -> None:
        delete_file = manifest_entry.data_file
        seq = manifest_entry.sequence_number or INITIAL_SEQUENCE_NUMBER

        if delete_file.content == DataFileContent.EQUALITY_DELETES:
            # Equality deletes stored with an unpartitioned spec apply to every partition
            if not partition_key:
                self._eq_global.add(delete_file, seq)
                self._has_global_eq_deletes = True
            else:
                key = _partition_key(delete_file.spec_id or 0, partition_key)
                self._eq_by_partition.setdefault(key, EqualityDeletes()).add(delete_file, seq)
            return

        # A deletion vector applies only to the data file it references
        if _is_deletion_vector(delete_file) and delete_file.referenced_data_file is not None:
            self._dvs_by_path.setdefault(delete_file.referenced_data_file, PositionDeletes()).add(delete_file, seq)
            return

        target_path = _referenced_data_file_path(delete_file)

        if target_path:
            deletes = self._by_path.setdefault(target_path, PositionDeletes())
            deletes.add(delete_file, seq)
        else:
            key = _partition_key(delete_file.spec_id or 0, partition_key)
            deletes = self._by_partition.setdefault(key, PositionDeletes())
            deletes.add(delete_file, seq)

    def for_data_file(self, seq_num: int, data_file: DataFile, partition_key: Record | None = None) -> DeleteFileSet:
        if self.is_empty():
            return DeleteFileSet()

        candidates: list[tuple[DataFile, int]] = []
        spec_id = data_file.spec_id or 0

        key = _partition_key(spec_id, partition_key)
        partition_deletes = self._by_partition.get(key)
        if partition_deletes:
            candidates.extend(
                (delete_file, seq)
                for delete_file, seq in partition_deletes.filter_entries_by_seq(seq_num)
                if _applies_to_data_file(delete_file, data_file)
            )

        path_deletes = self._by_path.get(data_file.file_path)
        if path_deletes:
            candidates.extend(path_deletes.filter_entries_by_seq(seq_num))

        dvs = self._dvs_by_path.get(data_file.file_path)
        dv_entries = dvs.filter_entries_by_seq(seq_num) if dvs else []
        if dv_entries:
            # At most one DV applies to a data file; it replaces older position deletes for that file.
            # The entries are sorted by sequence number, so the last one is the most recent DV.
            dv, dv_seq = dv_entries[-1]
            candidates = [(delete_file, seq) for delete_file, seq in candidates if seq > dv_seq]
            candidates.append((dv, dv_seq))

        if self._has_global_eq_deletes:
            candidates.extend(self._eq_global.filter_entries_by_seq(seq_num))

        eq_partition_deletes = self._eq_by_partition.get(key)
        if eq_partition_deletes:
            candidates.extend(eq_partition_deletes.filter_entries_by_seq(seq_num))

        return DeleteFileSet(delete_file for delete_file, _ in candidates)

    def referenced_delete_files(self) -> list[DataFile]:
        data_files: list[DataFile] = []

        for deletes in self._by_partition.values():
            data_files.extend(deletes.referenced_delete_files())

        for deletes in self._by_path.values():
            data_files.extend(deletes.referenced_delete_files())

        for deletes in self._dvs_by_path.values():
            data_files.extend(deletes.referenced_delete_files())

        for eq_deletes in self._eq_by_partition.values():
            data_files.extend(eq_deletes.referenced_delete_files())

        data_files.extend(self._eq_global.referenced_delete_files())

        return data_files
