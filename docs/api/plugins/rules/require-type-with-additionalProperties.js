module.exports = RequireTypeWithAdditionalProperties;

function RequireTypeWithAdditionalProperties() {
  return {
    Schema: {
      enter(schema, ctx) {
        if (
          schema.additionalProperties !== undefined &&
          schema.type === undefined
        ) {
          ctx.report({
            message:
              "Schema with additionalProperties must have a type defined.",
          });
        }
      },
    },
  };
}
