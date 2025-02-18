# Copyright 2023-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included
# in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
# in that file, in accordance with the Business Source License, use of this
# software will be governed by the Apache License, Version 2.0, included in
# the file licenses/APL2.txt.

import sgcollect_info


def test_parser():
    sgcollect_info.create_option_parser()


def test_parser_log_redaction_salt():
    parser = sgcollect_info.create_option_parser()
    options, _ = parser.parse_args(["--log-redaction-salt=a", "foo.zip"])
    assert options.salt_value == b"a"

    options, _ = parser.parse_args(["foo.zip"])
    # assert this is bytes uuid4
    assert isinstance(options.salt_value, bytes)
    assert len(options.salt_value) == 16
