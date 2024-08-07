/**
 * Copyright 2024-Present Couchbase, Inc.
 *
 * Use of this software is governed by the Business Source License included
 * in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 * in that file, in accordance with the Business Source License, use of this
 * software will be governed by the Apache License, Version 2.0, included in
 * the file licenses/APL2.txt.
 */

const ProcessRBAC = require("./decorators/process-rbac.js");
const ReplaceDescriptionCapella = require("./decorators/replace-description-capella.js");
const ReplaceInfoCapella = require("./decorators/replace-info-capella.js");
const ReplaceServerCapella = require("./decorators/replace-server-capella.js");

module.exports = {
  decorators: {
    oas3: {
      "process-rbac": ProcessRBAC,
      "replace-description-capella": ReplaceDescriptionCapella,
      "replace-info-capella": ReplaceInfoCapella,
      "replace-server-capella": ReplaceServerCapella,
    },
  },
  id: "plugin",
};
