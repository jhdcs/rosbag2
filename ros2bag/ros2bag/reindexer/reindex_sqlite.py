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
from typing import Dict, List, Union

from ros2bag.api import print_error

from . import bag_metadata


def get_metadata(db_file: pathlib.Path) -> Dict[str, Union[List[Dict[str, Union[str, int]]], int]]:
    db_con = sqlite3.connect(db_file)
    c = db_con.cursor()

    # Query the metadata
    c.execute('SELECT name, type, serialization_format, COUNT(messages.id), '
              'MIN(messages.timestamp), MAX(messages.timestamp), offered_qos_profiles '
              'FROM messages JOIN topics on topics.id = messages.topic_id '
              'GROUP BY topics.name;')

    # Set up initial values
    topics: List[Dict[str, Union[str, int]]] = []
    min_time: int = sys.maxsize
    max_time: int = 0

    # Aggregate metadata
    for row in c:
        topics.append({'topic_name': row[0],
                       'topic_type': row[1],
                       'topic_ser_fmt': row[2],
                       'message_count': row[3],
                       'offered_qos_profiles': row[6]})
        if row[4] < min_time:
            min_time = row[4]
        if row[5] > max_time:
            max_time = row[5]

    return {'topic_metadata': topics, 'min_time': min_time, 'max_time': max_time}


def reindex(uri: str, serialization_fmt: str, compression_fmt: str, compression_mode: str) -> None:
    """Reconstruct a metadata.yaml file for an sqlite3-based rosbag."""
    uri_dir = pathlib.Path(uri)
    if not uri_dir.is_dir():
        raise ValueError(
            print_error('Reindex needs a bag directory. Was given path "{}"'.format(uri)))

    # Get the relative paths
    rel_file_paths = [f for f in uri_dir.iterdir() if f.suffix == '.db3']

    # Start recording metadata
    metadata = bag_metadata.MetadataWriter()
    metadata.version = 4
    metadata.storage_identifier = 'sqlite3'
    metadata.add_multiple_rel_file_paths(rel_file_paths)
    metadata.compression_format = compression_fmt
    metadata.compression_mode = compression_mode
