module.exports = ReplaceServersCapella;

/** @type {import('@redocly/cli').OasDecorator} */

function ReplaceServersCapella({ serverUrl }) {
  return {
    Server: {
      leave(Server) {
        Server.url = serverUrl;
        delete Server.protocol;
      },
    },
  };
}
