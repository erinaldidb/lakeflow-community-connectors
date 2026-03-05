"""DICOMweb-specific DataSourceStreamReader for executor-side file/metadata fetching.

Replaces the generic LakeflowStreamReader (SimpleDataSourceStreamReader) with a
DataSourceStreamReader that distributes WADO-RS network traffic across Spark
executors via InputPartitions.

For non-file tables (studies, series, diagnostics) and instances without
fetch_dicom_files or fetch_metadata, a single SimplePartition is returned and
read() on the executor queries the source normally.

For instances with fetch_dicom_files=true or fetch_metadata=true, partitions()
(driver-side) collects bare instance records without downloading files or
metadata, then batches instances into DicomBatchPartitions.  Each
read(DicomBatchPartition) call runs on a Spark executor that has UC Volume FUSE
access, downloads the DICOM files, and writes them to the volume.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Iterator
import json

from pyspark.sql.datasource import DataSourceStreamReader, InputPartition
from pyspark.sql.types import StructType


@dataclass
class SimplePartition(InputPartition):
    start_json: str


@dataclass
class DicomBatchPartition(InputPartition):
    instances_json: str
    options_json: str


class DicomStreamReader(DataSourceStreamReader):
    """
    DataSourceStreamReader for DICOMweb.

    For non-file tables (studies, series, diagnostics) and instances without
    fetch_dicom_files, a single SimplePartition is returned and read() on the
    executor queries the source normally.

    For instances with fetch_dicom_files=true, partitions() (driver-side)
    collects all instance metadata without downloading files, then batches
    instances into DicomBatchPartitions. Each read(DicomBatchPartition) call
    runs on a Spark executor that has UC Volume FUSE access, downloads the
    DICOM files, and writes them to the volume.
    """

    def __init__(
        self,
        options: dict[str, str],
        schema: StructType,
        lakeflow_connect,
    ):
        self.options = options
        self.schema = schema
        self._lakeflow_connect = lakeflow_connect

    def initialOffset(self):
        return {}

    def latestOffset(self):
        return {"study_date": date.today().strftime("%Y%m%d"), "page_offset": 0}

    def partitions(self, start, end):
        is_delete_flow = self.options.get(IS_DELETE_FLOW) == "true"
        table_options = {k: v for k, v in self.options.items() if k != IS_DELETE_FLOW}
        table_name = self.options.get(TABLE_NAME)
        fetch_files = table_options.get("fetch_dicom_files", "false").lower() == "true"
        fetch_metadata = table_options.get("fetch_metadata", "false").lower() == "true"
        volume_path = table_options.get("dicom_volume_path", "")

        if not is_delete_flow and table_name == "instances" and (fetch_files or fetch_metadata):
            if fetch_files and not volume_path:
                raise ValueError("fetch_dicom_files=true requires dicom_volume_path to be set")
            # Driver-side: collect bare instance records only (no metadata, no file
            # downloads). Both are deferred to executor-side _read_dicom_batch() so
            # that WADO-RS network traffic is distributed across executors.
            meta_options = dict(table_options)
            meta_options["fetch_dicom_files"] = "false"
            meta_options["fetch_metadata"] = "false"
            all_records = []
            current_start = dict(start) if start else {}
            while True:
                records, next_offset = self._lakeflow_connect.read_table(
                    table_name, current_start, meta_options
                )
                batch = list(records)
                all_records.extend(batch)
                if not batch or next_offset.get("page_offset", 0) == 0:
                    break
                if next_offset == current_start:
                    break
                current_start = next_offset

            batch_size = int(self.options.get("dicom_batch_size", "50"))
            options_json = json.dumps(dict(self.options))
            partitions_list = []
            for i in range(0, max(1, len(all_records)), batch_size):
                partitions_list.append(DicomBatchPartition(
                    instances_json=json.dumps(all_records[i:i + batch_size], default=str),
                    options_json=options_json,
                ))
            return partitions_list

        else:
            return [SimplePartition(start_json=json.dumps(start if start else {}))]

    def read(self, partition):
        if isinstance(partition, DicomBatchPartition):
            return self._read_dicom_batch(partition)
        else:
            return self._read_simple(partition)

    def _read_dicom_batch(self, partition):
        """Executor-side: fetch metadata and/or download DICOM files (FUSE accessible)."""
        instances = json.loads(partition.instances_json)
        options = json.loads(partition.options_json)
        table_options = {k: v for k, v in options.items() if k != IS_DELETE_FLOW}
        volume_path = table_options.get("dicom_volume_path", "")
        wado_mode = table_options.get("wado_mode", "auto")
        fetch_metadata = table_options.get("fetch_metadata", "false").lower() == "true"
        fetch_files = table_options.get("fetch_dicom_files", "false").lower() == "true"
        connector = LakeflowConnectImpl(options)

        if fetch_metadata:
            # One WADO-RS metadata request per unique series in this batch.
            sop_to_meta: dict = {}
            seen_series: set = set()
            for rec in instances:
                key = (rec.get("study_instance_uid"), rec.get("series_instance_uid"))
                if key not in seen_series and all(key):
                    seen_series.add(key)
                    sop_to_meta.update(connector._build_metadata_map(key[0], key[1]))
            for rec in instances:
                sop_uid = rec.get("sop_instance_uid")
                rec["metadata"] = sop_to_meta.get(sop_uid) if sop_uid else None

        results = []
        for rec in instances:
            if fetch_files:
                rec = connector._attach_dicom_file(rec, volume_path, wado_mode)
            results.append(parse_value(rec, self.schema))
        return iter(results)

    def _read_simple(self, partition):
        """Executor-side: query source and return records (no file downloads)."""
        start = json.loads(partition.start_json)
        table_name = self.options.get(TABLE_NAME)
        is_delete_flow = self.options.get(IS_DELETE_FLOW) == "true"
        table_options = {k: v for k, v in self.options.items() if k != IS_DELETE_FLOW}
        connector = LakeflowConnectImpl(self.options)
        if is_delete_flow:
            records, _ = connector.read_table_deletes(table_name, start, table_options)
        else:
            records, _ = connector.read_table(table_name, start, table_options)
        return iter(map(lambda x: parse_value(x, self.schema), records))

    def commit(self, end):
        pass


# ---------------------------------------------------------------------------
# Merge-script workaround
# ---------------------------------------------------------------------------
# The merge script (merge_python_source.py) places source connector code
# BEFORE lakeflow_datasource.py in the generated file, and we cannot modify
# lakeflow_datasource.py.  lakeflow_datasource.py defines LakeflowSource
# with simpleStreamReader → LakeflowStreamReader (driver-only).
#
# PySpark's DataSource dispatch prefers streamReader() over simpleStreamReader()
# when both are available.  By shadowing the DataSource base class here with a
# subclass that defines streamReader → DicomStreamReader, any DataSource
# subclass defined later (i.e. LakeflowSource) inherits the streamReader
# method, enabling executor-side WADO-RS file downloads and metadata fetching.
# ---------------------------------------------------------------------------
global DataSource  # noqa: E261 — required so the class statement below
                   # assigns to the module-level name (imported from pyspark)
                   # rather than creating a function-local that would trigger
                   # UnboundLocalError on earlier references.
_OrigDataSource = DataSource


class DataSource(_OrigDataSource):
    """DataSource with DICOMweb streamReader support."""

    def streamReader(self, schema):
        return DicomStreamReader(self.options, schema, self.lakeflow_connect)
