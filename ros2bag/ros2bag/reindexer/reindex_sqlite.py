# Copyright 2020 DCS Corporation, All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# DISTRIBUTION A. Approved for public release; distribution unlimited.
# OPSEC #4584.
#
# Delivered to the U.S. Government with Unlimited Rights, as defined in DFARS
# Part 252.227-7013 or 7014 (Feb 2014).
#
# This notice must appear in all copies of this file and its derivatives.

import pathlib
import sqlite3
import sys
from typing import List, Literal, Optional, TypedDict

from ros2bag.api import print_error

import bag_metadata

class TopicInfo(TypedDict):
    topic_name: str
    topic_type: str
    topic_ser_fmt: str
    topic_count: str
    topic_qos: str

class TopicInfo(TypedDict):
    topic_name: str
    topic_type: str
    topic_ser_fmt: str
    topic_count: str
    topic_qos: str


class DBMetadata(TypedDict):
    topic_metadata: List[TopicInfo]
    min_time: int
    max_time: int


def get_metadata_from_connection(db_con: sqlite3.Connection) -> DBMetadata:
    # Query the metadata
    c = db_con.execute('SELECT name, type, serialization_format, COUNT(messages.id), '
                       'MIN(messages.timestamp), MAX(messages.timestamp), offered_qos_profiles '
                       'FROM messages JOIN topics on topics.id = messages.topic_id '
                       'GROUP BY topics.name;')

    rows = c.fetchall()

    # Set up initial values
    topics: List[TopicInfo] = []
    min_time: int = sys.maxsize
    max_time: int = 0

    # Aggregate metadata
    for row in rows:
        topics.append(TopicInfo(
            topic_name=row[0],
            topic_type=row[1],
            topic_ser_fmt=row[2],
            topic_count=row[3],
            topic_qos=row[6]))
        if row[4] < min_time:
            min_time = row[4]
        if row[5] > max_time:
            max_time = row[5]

    return {'topic_metadata': topics, 'min_time': min_time, 'max_time': max_time}


def get_metadata(db_file: pathlib.Path,
                 compression_fmt: Optional[str],
                 compression_mode: Optional[str]) -> DBMetadata:
    # Handle compression
    if compression_fmt != '':
        if compression_mode == 'message':
            # Message-compressed files only compress the data of a message - not its metadata
            # So this will work exactly the same as a regular file
            with sqlite3.connect(db_file) as db_con:
                return get_metadata_from_connection(db_con)
        else:
            raise ValueError(print_error(
                                'Invalid compression mode for compressed file. '
                                'Expected "message", got {}'.format(compression_mode)))
    else:
        with sqlite3.connect(db_file) as db_con:
            return get_metadata_from_connection(db_con)


def reindex(
        uri: str,
        compression_fmt: Optional[str],
        compression_mode: Optional[str],
        _test_output_dir: Optional[str]) -> None:
    """Reconstruct a metadata.yaml file for an sqlite3-based rosbag."""
    # Disallow working with compressed formats
    if compression_fmt or (compression_mode and compression_mode != 'message'):
        raise ValueError(
            print_error('Reindex does not currently support file-compressed bags'))

    uri_dir = pathlib.Path(uri)
    if not uri_dir.is_dir():
        raise ValueError(
            print_error('Reindex needs a bag directory. Was given path "{}"'.format(uri)))

    # Get the relative paths
    rel_file_paths = sorted(f for f in uri_dir.iterdir() if f.suffix == '.db3')

    # Start recording metadata
    metadata = bag_metadata.MetadataWriter()
    metadata.version = 4
    metadata.storage_identifier = 'sqlite3'
    metadata.add_multiple_rel_file_paths([p.relative_to(uri_dir) for p in rel_file_paths])
    metadata.compression_format = compression_fmt
    metadata.compression_mode = compression_mode

    # Get topic info for each database
    rolling_min_time = sys.maxsize
    rolling_max_time = 0
    for db_file in rel_file_paths:
        db_metadata = get_metadata(db_file, compression_fmt, compression_mode)
        for topic in db_metadata['topic_metadata']:
            metadata.add_topic(**topic)

        if db_metadata['min_time'] < rolling_min_time:
            rolling_min_time = db_metadata['min_time']
        if db_metadata['max_time'] > rolling_max_time:
            rolling_max_time = db_metadata['max_time']

    metadata.starting_time = rolling_min_time
    metadata.duration = rolling_max_time - rolling_min_time
    metadata.calculate_message_count()

    if _test_output_dir is not None:
        out_dir = pathlib.Path(_test_output_dir)
        if not out_dir.is_dir():
            raise ValueError(
                print_error('Reindex test output needs a directory. '
                            'Was given path "{}"'.format(uri)))
    else:
        out_dir = uri_dir

    metadata.write_yaml(out_dir)
