/**
 * Copyright 2025-Present Couchbase, Inc.
 *
 * Use of this software is governed by the Business Source License included
 * in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 * in that file, in accordance with the Business Source License, use of this
 * software will be governed by the Apache License, Version 2.0, included in
 * the file licenses/APL2.txt.
 */

module.exports = TypeCheckDefaults;

function checkMinMaxInteger(schema, ctx) {
  if (schema.default > schema.maximum) {
    ctx.report({
      message:
        "Default value for integer type exceeds maximum value of " +
        schema.maximum,
    });
  }
  if (schema.default < schema.minimum) {
    ctx.report({
      message:
        "Default value for integer type is below minimum value of " +
        schema.minimum,
    });
  }
}

function TypeCheckDefaults() {
  return {
    Schema: {
      enter(schema, ctx) {
        switch (schema.type) {
          case "boolean":
            if (
              schema.default !== undefined &&
              typeof schema.default !== "boolean"
            ) {
              ctx.report({
                message:
                  "Default value for boolean type must be a boolean was " +
                  typeof schema.default,
              });
            }
            break;
          case "number":
            if (schema.default !== undefined) {
              if (typeof schema.default !== "number") {
                ctx.report({
                  message:
                    "Default value for number type must be a number was " +
                    typeof schema.default,
                });
              }
              checkMinMaxInteger(schema, ctx);
            }
            break;
          case "string":
            if (
              schema.default !== undefined &&
              typeof schema.default !== "string"
            ) {
              ctx.report({
                message:
                  "Default value for string type must be a string was " +
                  typeof schema.default,
              });
            }
            break;
          case "integer":
            if (schema.default !== undefined) {
              if (!Number.isInteger(schema.default)) {
                ctx.report({
                  message:
                    "Default value for integer type must be an integer was " +
                    typeof schema.default,
                });
              }
              if (schema.default > schema.maximum) {
                ctx.report({
                  message:
                    "Default value for integer type exceeds maximum value of " +
                    schema.maximum,
                });
              }
              if (schema.default < schema.minimum) {
                ctx.report({
                  message:
                    "Default value for integer type is below minimum value of " +
                    schema.minimum,
                });
              }
            }
            break;
          case "array":
            if (
              schema.default !== undefined &&
              !Array.isArray(schema.default)
            ) {
              ctx.report({
                message:
                  "Default value for array type must be an array was " +
                  typeof schema.default,
              });
            }
            break;
          case "object":
            if (
              schema.default !== undefined &&
              typeof schema.default !== "object"
            ) {
              ctx.report({
                message:
                  "Default value for object type must be an object was " +
                  typeof schema.default,
              });
            }
            break;
          case undefined:
            // assume no type is equivalent to "object" in JSON Schema
            break;
          default:
            ctx.report({
              message: "Unsupported type in schema " + schema.type,
            });
        }
      },
    },
  };
}
