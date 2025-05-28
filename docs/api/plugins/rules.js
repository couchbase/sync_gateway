const RequireTypeWithAdditionalProperties = require("./rules/require-type-with-additionalProperties.js");
const TypeCheckDefaults = require("./rules/typecheck-defaults.js");

module.exports = {
  rules: {
    oas3: {
      "require-type-with-additionalProperties":
        RequireTypeWithAdditionalProperties,
      "typecheck-defaults": TypeCheckDefaults,
    },
  },
  id: "custom-rules",
};
