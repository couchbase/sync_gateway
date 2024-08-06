const ReplaceServersCapella = import("./decorators/replace-servers-capella.js");
const id = "plugin";

const decorators = {
  oas3: {
    "replace-servers-capella": ReplaceServersCapella,
  },
};

module.exports = {
  decorators,
  id,
};
