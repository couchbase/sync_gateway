# Copyright 2023-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included
# in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
# in that file, in accordance with the Business Source License, use of this
# software will be governed by the Apache License, Version 2.0, included in
# the file licenses/APL2.txt.

from importlib.util import spec_from_loader, module_from_spec
from importlib.machinery import SourceFileLoader
import sys

module_name = "sgcollect_info"
spec = spec_from_loader(
    module_name, SourceFileLoader(module_name, "tools/" + module_name)
)
mod = module_from_spec(spec)
sys.modules[spec.name] = mod
spec.loader.exec_module(mod)
