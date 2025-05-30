const TypeCheckDefaults = require("./rules/typecheck-defaults.js");

module.exports = {
  rules: {
    oas3: {
      "typecheck-defaults": TypeCheckDefaults,
    },
  },
  id: "custom-rules",
};
