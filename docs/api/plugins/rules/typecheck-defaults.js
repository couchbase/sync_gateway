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
        }
      },
    },
  };
}
