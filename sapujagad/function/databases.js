/**
 * Responds to HTTP request.
 *
 * @param { app, context, callback }
 * app { getRequester, getPublisher }
 * context { body, cookies, method, params, query, headers }
 * callback(error, response)
 */

const hive = require("hive-driver");
exports.handler = async ({ app, context, callback }) => {
  const { TCLIService, TCLIService_types } = hive.thrift;
  const client = new hive.HiveClient(TCLIService, TCLIService_types);
  const utils = new hive.HiveUtils(TCLIService_types);

  const { query } = context;
  const { secret } = query;

  // dynamic secret
  var secretKey = "babahaha";

  if (secret !== secretKey) {
    return callback(null, {
      statusCode: 403,
      message: "Forbidden",
    });
  }

  // dinamically saved in the mg table
  client
    .connect(
      {
        host: "10.10.65.1",
        port: 10000,
      },
      new hive.connections.TcpConnection(),
      new hive.auth.PlainTcpAuthentication({
        username: "hive",
        password: "hive",
      })
    )
    .then(async (client) => {
      const session = await client.openSession({
        client_protocol:
          TCLIService_types.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V10,
      });

      let databasesDetails = [];

      try {
        const showDbs = await session.executeStatement(`SHOW DATABASES`);
        await utils.waitUntilReady(showDbs, false, () => {});
        await utils.fetchAll(showDbs);

        await showDbs.close();

        // console.log(utils.getResult(showDbs).getValue());

        var databases = utils.getResult(showDbs).getValue();

        var databaseNames = databases.map(function (database) {
          return database.database_name;
        });

        // console.log(databaseNames);

        return callback(null, {
          statusCode: 200,
          message: "OK",
          databases: databaseNames,
        });
      } catch (error) {
        return callback(null, {
          statusCode: 400,
          message: error,
        });
      }
    });
};
