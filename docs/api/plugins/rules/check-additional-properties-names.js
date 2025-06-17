/**
 * Copyright 2025-Present Couchbase, Inc.
 *
 * Use of this software is governed by the Business Source License included
 * in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 * in that file, in accordance with the Business Source License, use of this
 * software will be governed by the Apache License, Version 2.0, included in
 * the file licenses/APL2.txt.
 */

module.exports = CheckAdditionalPropertiesNames;

function CheckAdditionalPropertiesNames() {
  return {
    Schema: {
      enter(schema, ctx) {
        props = schema.additionalProperties;
        if (props === undefined) {
          return;
        }
        // additionalProperties: true or additionalProperties: {} is valid openapi
        if (Object.keys(props).length === 0) {
          return;
        }
        // x-additionalPropertiesName is a redocly extension used to provide example map values
        if (
          props["x-additionalPropertiesName"] === undefined &&
          schema.example === undefined
        ) {
          ctx.report({
            message: `additionalProperties should also include x-additionalPropertiesName or an example to show a meaningful name for generating examples.`,
          });
        }
      },
    },
  };
}
