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
from typing import Dict, List, Literal, TypedDict, Union

from ros2bag.api import print_error
import yaml


class TopicBlock(TypedDict):
    block_name: str
    block_type: str
    block_ser: str
    block_qos: str


def block_as_dict(topic_block: TopicBlock) -> Dict[str, str]:
    return {
        'name': topic_block['block_name'],
        'type': topic_block['block_type'],
        'serialization_format': topic_block['block_ser'],
        'offered_qos_profiles': topic_block['block_qos']
    }


class TopicMetadata(TypedDict):
    """Contains the metadata for a ROS 2 topic."""

    tm_block: TopicBlock
    tm_message_count: int


def topic_as_dict(topic: TopicMetadata) -> Dict[str, Union[Dict[str, str], str, int]]:
    metadata_topics = block_as_dict(topic['tm_block'])
    return {'topic_metadata': metadata_topics, 'message_count': topic['tm_message_count']}


class RosbagYamlDumper(yaml.Dumper):

    def increase_indent(self, flow=False, indentless=False):
        return super(RosbagYamlDumper, self).increase_indent(flow, False)


class MetadataWriter:
    """Class for building a metadata.yaml file."""

    def __init__(self):
        self._version: int = 0
        self._storage_identifier: str = ''
        self._relative_file_paths: List[str] = []
        self._duration: int = 0
        self._starting_time: int = 0
        self._message_count: int = 0
        self._topics: List[TopicMetadata] = []
        self._compression_format: Literal['', 'zstd'] = ''
        self._compression_mode: Literal['', 'FILE', 'MESSAGE'] = ''

    @property
    def version(self) -> int:
        return self._version

    @version.setter
    def version(self, x: int):
        if x < 0:
            raise ValueError(print_error('Version must be greater than 0, got {}'.format(x)))

        self._version = x

    @property
    def storage_identifier(self) -> str:
        return self._storage_identifier

    @storage_identifier.setter
    def storage_identifier(self, s: str):
        self._storage_identifier = s

    @property
    def relative_file_paths(self) -> List[str]:
        return self._relative_file_paths

    def add_rel_file_path(self, fp: pathlib.Path):
        """Add a relative path to the internal relative filepaths storage."""
        if fp.is_absolute():
            raise ValueError(
                print_error('Cannot add absolute path to relative filepaths. '
                            'Got path: "{}"'.format(fp)))

        self._relative_file_paths.append(str(fp))

    def add_multiple_rel_file_paths(self, fps: List[pathlib.Path]):
        """Add multiple relative paths to the internal relative filepaths storage."""
        for p in fps:
            if p.is_absolute():
                raise ValueError(
                    print_error('Cannot add absolute path to relative filepaths. '
                                'Got path: "{}"'.format(p)))
            self._relative_file_paths.append(str(p))

    @property
    def duration(self) -> int:
        return self._duration

    @duration.setter
    def duration(self, x: int):
        if x < 0:
            raise ValueError(print_error('Duration cannot be negative. Got {}'.format(x)))
        self._duration = x

    @property
    def starting_time(self) -> int:
        return self._starting_time

    @starting_time.setter
    def starting_time(self, x: int):
        if x < 0:
            raise ValueError(print_error('Starting time cannot be negative. Got {}'.format(x)))
        self._starting_time = x

    @property
    def message_count(self) -> int:
        return self._message_count

    @message_count.setter
    def message_count(self, x: int):
        if x < 0:
            raise ValueError(print_error('Message count cannot be negative. Got {}'.format(x)))
        self._message_count = x

    def calculate_message_count(self):
        self.message_count = sum(d['tm_message_count'] for d in self._topics)

    @property
    def topics(self) -> List[TopicMetadata]:
        return self._topics

    def add_topic(self,
                  topic_name: str,
                  topic_type: str,
                  topic_ser_fmt: str,
                  topic_qos: str,
                  topic_count: int):
        if topic_count < 0:
            raise ValueError(
                print_error('Topic message count cannot be negative. Got {}'.format(topic_count)))
        topic_block = TopicBlock(
            block_name=topic_name,
            block_type=topic_type,
            block_ser=topic_ser_fmt,
            block_qos=topic_qos
        )
        new_topic: TopicMetadata = \
            TopicMetadata(
                tm_block=topic_block,
                tm_message_count=topic_count)

        # Check to see if this topic already exists
        if not any(d['tm_block']['block_name'] == topic_name for d in self._topics):
            self._topics.append(new_topic)
        else:
            # It exists. Just add the number of new messages
            topic_to_edit = next(d for d in self._topics
                                 if d['tm_block']['block_name'] == topic_name)
            topic_to_edit['tm_message_count'] += topic_count

    @property
    def compression_format(self) -> Literal['', 'zstd']:
        return self._compression_format

    @compression_format.setter
    def compression_format(self, f: Literal['', 'zstd']):
        if not ((f == '') or (f == 'zstd')):
            raise ValueError(print_error("Compression format must either be '' or 'zstd', "
                                         "Got '{}'".format(f)))
        self._compression_format = f

    @property
    def compression_mode(self) -> str:
        return self._compression_mode

    @compression_mode.setter
    def compression_mode(self, m: Literal['', 'none', 'file', 'message']):
        if not ((m == '') or (m == 'none') or (m == 'file') or (m == 'message')):
            raise ValueError(print_error(
                                "Compression format must either be 'none', 'file or 'message', "
                                "Got '{}'".format(m)))
        if m == 'none':
            translated = ''
        else:
            translated = m.upper()
        self._compression_mode = translated

    def _as_yaml_dict(self) -> Dict:
        # Sort the relative file paths really quick
        self.relative_file_paths.sort()

        ordered_topics = [topic_as_dict(topic) for topic in self.topics]

        yaml_dict = {
            'version': self.version,
            'storage_identifier': self.storage_identifier,
            'relative_file_paths': self.relative_file_paths,
            'duration': {'nanoseconds': self.duration},
            'starting_time': {'nanoseconds_since_epoch': self.starting_time},
            'message_count': self.message_count,
            'topics_with_message_count': ordered_topics,
            'compression_format': self.compression_format,
            'compression_mode': self.compression_mode
        }
        return yaml_dict

    def write_yaml(self, bag_dir: pathlib.Path):
        """Write a metadata.yaml file to the directory pointed to by bag_dir."""
        if not bag_dir.is_dir():
            raise ValueError(
                print_error('Expected a directory to output yaml file to. '
                            'Got path: "{}"'.format(bag_dir)))

        # Create the dictionary for export
        yaml_dict = {'rosbag2_bagfile_information': self._as_yaml_dict()}

        fp = bag_dir / 'metadata.yaml'
        with fp.open('w') as yaml_file:
            yaml_text = yaml.dump(yaml_dict,
                                  Dumper=RosbagYamlDumper,
                                  default_flow_style=False,
                                  width=1000,
                                  sort_keys=False)
            yaml_file.write(yaml_text)


if __name__ == '__main__':
    test_path = pathlib.Path.home() / pathlib.Path('bag_test')

    test_metadata = MetadataWriter()
    test_metadata.version = 4
    test_metadata.storage_identifier = 'sqlite3'
    test_metadata.add_rel_file_path(pathlib.Path('cdr_test_0.db3'))
    test_metadata.duration = 151137181
    test_metadata.starting_time = 1586406456763032325
    test_metadata.message_count = 7
    test_metadata.add_topic(
        '/test_topic',
        'test_msgs/msg/BasicTypes',
        'cdr',
        '- history: 3\n  '
        'depth: 0\n  '
        'reliability: 1\n  '
        'durability: 2\n  '
        'deadline:\n    '
        'sec: 2147483647\n    '
        'nsec: 4294967295\n  '
        'lifespan:\n    '
        'sec: 2147483647\n    '
        'nsec: 4294967295\n  '
        'liveliness: 1\n  '
        'liveliness_lease_duration:\n    '
        'sec: 2147483647\n    '
        'nsec: 4294967295\n  '
        'avoid_ros_namespace_conventions: false',
        3)
    test_metadata.add_topic(
        '/array_topic',
        'test_msgs/msg/Arrays',
        'cdr',
        '- history: 3\n  '
        'depth: 0\n  '
        'reliability: 1\n  '
        'durability: 2\n  '
        'deadline:\n    '
        'sec: 2147483647\n    '
        'nsec: 4294967295\n  '
        'lifespan:\n    '
        'sec: 2147483647\n    '
        'nsec: 4294967295\n  '
        'liveliness: 1\n  '
        'liveliness_lease_duration:\n    '
        'sec: 2147483647\n    '
        'nsec: 4294967295\n  '
        'avoid_ros_namespace_conventions: false',
        4)
    test_metadata.compression_format = ''
    test_metadata.compression_mode = ''

    test_metadata.write_yaml(test_path)
