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
 * Removes the RBAC roles from capella API docs. This expects the RBAC information to be at the end of the documentation string. This is not a robust way of doing this.
 * @module ExciseRBACCapella
 */

module.exports = ProcessRBAC;

/** @type {import('@redocly/cli').OasDecorator} */
function ProcessRBAC() {
  return {
    Operation: {
      leave(Operation) {
        rbacRoles = Operation["x-rbac"];
        console.log("rbacRoles: ", rbacRoles);
        if (rbacRoles) {
          extra = "\n\nRequired Sync Gateway RBAC roles:\n";
          for (roleIdx in rbacRoles) {
            extra += `  * ${rbacRoles[roleIdx]}\n`;
          }
          Operation.description += extra;
        }
      },
    },
  };
}
