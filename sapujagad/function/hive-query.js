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

  const { body } = context;
  const { dbName, dbQuery } = body;

  if (!dbName || !dbQuery) {
    return callback(null, {
      statusCode: 400,
      message: "dbName and dbQuery are required",
    });
  }

  // var dbName = "gg";
  // var dbQuery = "SELECT * FROM gg2022_2023 LIMIT 10";

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

      try {
        const useDb = await session.executeStatement(`USE ${dbName}`);
        await utils.waitUntilReady(useDb, false, () => {});
        await utils.fetchAll(useDb);

        const operation = await session.executeStatement(dbQuery);
        await utils.waitUntilReady(operation, false, () => {});
        await utils.fetchAll(operation);

        // console.log(utils.getResult(operation).getValue());

        await useDb.close();
        await operation.close();
        await session.close();

        return callback(null, {
          statusCode: 200,
          message: "finished",
          result: utils.getResult(operation).getValue(),
        });
      } catch (error) {
        return callback(null, {
          statusCode: 400,
          message: error,
        });
      }
    });
};
