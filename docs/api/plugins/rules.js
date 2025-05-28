const RequireTypeWithAdditionalProperties = require("./rules/require-type-with-additionalProperties.js");

module.exports = {
  rules: {
    oas3: {
      "require-type-with-additionalProperties":
        RequireTypeWithAdditionalProperties,
    },
  },
  id: "custom-rules",
};
