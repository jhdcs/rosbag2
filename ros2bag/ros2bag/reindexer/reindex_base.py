# DISTRIBUTION A. Approved for public release; distribution unlimited.
# OPSEC #4584.
#
# Copyright (c) 2020 DCS Corporation
# All Rights Reserved
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    Caution-http://www.apache.org/licenses/LICENSE-2.0
#
# The software/firmware is provided to you on an As-Is basis.
#
# Delivered to the U.S. Government with Unlimited Rights, as defined in DFARS
# Part 252.227-7013 or 7014 (Feb 2014).
#
# This notice must appear in all copies of this file and its derivatives.

from ros2bag.api import print_error
from typing import Optional
from . import reindex_sqlite

def reindex(uri: str, storage_id: str, serialization_fmt: str, compression_fmt: str) -> Optional[str]:
    if storage_id == 'sqlite3':
        reindex_sqlite.reindex(uri, serialization_fmt, compression_fmt)
    else:
        return print_error("Reindex for storage type {} not implemented".format(storage_id))