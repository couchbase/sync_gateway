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
 * Modifies the title of openapi object to value passed in.
 * @module ReplaceInfoCapella
 */

module.exports = ReplaceInfoCapella;

/** @type {import('@redocly/cli').OasDecorator} */
function ReplaceInfoCapella({ title }) {
  return {
    Info: {
      leave(Info) {
        if (title) {
          Info.title = title;
        }
        Info.description =
          "App Services manages access and synchronization between Couchbase Lite and Couchbase Capella";
      },
    },
  };
}
