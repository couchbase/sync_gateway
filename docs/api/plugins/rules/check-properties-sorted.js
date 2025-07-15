/**
 * Copyright 2025-Present Couchbase, Inc.
 *
 * Use of this software is governed by the Business Source License included
 * in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 * in that file, in accordance with the Business Source License, use of this
 * software will be governed by the Apache License, Version 2.0, included in
 * the file licenses/APL2.txt.
 */

module.exports = CheckPropertiesSorted;

function CheckPropertiesSorted() {
  return {
    Schema: {
      enter(schema, ctx) {
        if (!schema["x-requirePropertiesSorted"]) {
          return;
        }
        if (!schema || typeof schema !== "object") {
          ctx.report({
            message: "Schema is not an object or is undefined.",
          });
          return;
        }

        const props = schema.properties;
        if (!props || typeof props !== "object") {
          ctx.report({
            message: "Schema properties are not defined or not an object.",
          });
          return;
        }
        if (schema.allOf) {
          ctx.report({
            message:
              "Schema contains 'allOf', which is not supported yet by this rule." +
              schema.allOf,
          });
          return;
        }
        if (props.allOf) {
          ctx.report({
            message:
              "Schema properties contains 'allOf', which is not supported yet by this rule.",
          });
          return;
        }
        if (schema.anyOf) {
          ctx.report({
            message:
              "Schema contains 'anyOf', which is not supported yet by this rule.",
          });
          return;
        }
        if (props.anyOf) {
          ctx.report({
            message:
              "Schema properties contains 'anyOf', which is not supported yet by this rule.",
          });
          return;
        }
        if (schema.oneOf) {
          ctx.report({
            message:
              "Schema contains 'oneOf', which is not supported yet by this rule.",
          });
          return;
        }
        if (props.oneOf) {
          ctx.report({
            message:
              "Schema properties contains 'oneOf', which is not supported yet by this rule.",
          });
          return;
        }

        const nonDeprecatedKeys = Object.keys(props).filter(
          (key) => !props[key].deprecated,
        );
        const deprecatedKeys = Object.keys(props).filter(
          (key) => props[key].deprecated,
        );
        const keys = Object.keys(props);
        const sortedKeys = [
          ...nonDeprecatedKeys.sort(),
          ...deprecatedKeys.sort(),
        ];

        for (let i = 0; i < keys.length; i++) {
          if (keys[i] !== sortedKeys[i]) {
            ctx.report({
              message: `Schema properties are not sorted alphabetically. Non-deprecated properties are sorted first, followed by deprecated properties. \nExisting:\n${keys.join(", ")}\nShould be:\n${sortedKeys.join(", ")}\nDeprecatedKeys:\n${deprecatedKeys.join(", ")}`,
            });
            break; // Only report once per schema
          }
        }
      },
    },
  };
}
