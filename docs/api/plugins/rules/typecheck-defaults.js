module.exports = TypeCheckDefaults;

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
            if (
              schema.default !== undefined &&
              typeof schema.default !== "number"
            ) {
              ctx.report({
                message:
                  "Default value for number type must be a number was " +
                  typeof schema.default,
              });
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
            if (
              schema.default !== undefined &&
              !Number.isInteger(schema.default)
            ) {
              ctx.report({
                message:
                  "Default value for integer type must be an integer was " +
                  typeof schema.default,
              });
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
            if (schema.oneOf === undefined && schema.allOf === undefined) {
              ctx.report({
                message: "Schema missing a type definition",
              });
            }
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
