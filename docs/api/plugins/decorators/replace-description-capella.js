/**
 * Copyright 2024-Present Couchbase, Inc.
 *
 * Use of this software is governed by the Business Source License included
 * in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 * in that file, in accordance with the Business Source License, use of this
 * software will be governed by the Apache License, Version 2.0, included in
 * the file licenses/APL2.txt.
 */

/**
 * Does a string replacement on all operations (GET,PUT,POST,etc) to replace Sync Gateway with App Services.
 * @module ReplaceDescriptionCapella
 */

module.exports = ReplaceDescriptionCapella;

/** @type {import('@redocly/cli').OasDecorator} */
function ReplaceDescriptionCapella() {
  return {
    Operation: {
      leave(Operation) {
        if (Operation.description) {
          Operation.description = Operation.description.replace(
            "Sync Gateway",
            "App Services",
          );
        }
      },
    },
  };
}
