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

from ros2bag.api import check_path_exists, print_error
from ros2bag.reindexer import reindex_base
from ros2bag.verb import VerbExtension


class ReindexVerb(VerbExtension):
    """Generate metadata from a bag."""

    def add_arguments(self, parser, cli_name):  # noqa: D102
        parser.add_argument(
            'bag_file', type=check_path_exists, help='Bag file to reindex')
        parser.add_argument(
            '-s', '--storage', default='sqlite3',
            help="storage identifier to be used, defaults to 'sqlite3'")
        parser.add_argument(
            '-c', '--compression-format', type=str, default='', choices=['zstd'],
            help='Specify the compression format/algorithm. Default is none.'
        )
        parser.add_argument(
            'm', '--compression-mode', type=str, default='none',
            choices=['none', 'file', 'message'],
            help="Specify whether bag is compressed by file or by message. Default is 'none'"
        )
        parser.add_argument(
            '-t', '--test-output-dir', type=str, default=None,
            help='Write output metadata file to a specified directory, instead of the bag'
                 'file directory. Useful for testing'
        )
        self._subparser = parser

    def main(self, *, args):  # noqa: D102

        if args.compression_fmt and args.compression_mode == 'none':
            return print_error('Invalid choice: Cannot specify compression format '
                               'without a compression mode.')

        reindex_base.reindex(
            uri=args.bag_file,
            storage_id=args.storage_id,
            compression_fmt=args.compression_fmt,
            compression_mode=args.compression_mode,
            _test_output_dir=args._test_output_dir
        )
