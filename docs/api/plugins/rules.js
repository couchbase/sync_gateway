/**
 * Copyright 2025-Present Couchbase, Inc.
 *
 * Use of this software is governed by the Business Source License included
 * in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 * in that file, in accordance with the Business Source License, use of this
 * software will be governed by the Apache License, Version 2.0, included in
 * the file licenses/APL2.txt.
 */

const TypeCheckDefaults = require("./rules/typecheck-defaults.js");
const CheckAdditionalPropertiesNames = require("./rules/check-additional-properties-names.js");
const CheckPropertiesSorted = require("./rules/check-properties-sorted.js");
module.exports = {
  rules: {
    oas3: {
      "typecheck-defaults": TypeCheckDefaults,
      "check-additional-properties-names": CheckAdditionalPropertiesNames,
      "check-properties-sorted": CheckPropertiesSorted,
    },
  },
  id: "custom-rules",
};
