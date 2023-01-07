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
  const { secret, dbName } = query;

  if (!dbName) {
    return callback(null, {
      statusCode: 400,
      message: "dbName is required",
    });
  }

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
        host: "10.10.65.3",
        port: 10500,
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

      let databaseDetails = [];

      try {
        const useDb = await session.executeStatement("USE " + dbName);
        await utils.waitUntilReady(useDb, false, () => {});
        await utils.fetchAll(useDb);

        //   console.log(utils.getResult(useDb).getValue());
        await useDb.close();

        const showTables = await session.executeStatement("SHOW TABLES");
        await utils.waitUntilReady(showTables, false, () => {});
        await utils.fetchAll(showTables);

        //   console.log(utils.getResult(showTables).getValue());
        await showTables.close();

        var tables = utils.getResult(showTables).getValue();

        if (!tables.length) {
          return callback(null, {
            statusCode: 200,
            message: "No tables found",
            results: databaseDetails,
          });
        }

        tables.map(async (table, indexTab, rowTab) => {
          const descTable = await session.executeStatement(
            "SHOW COLUMNS IN " + table.tab_name
          );
          await utils.waitUntilReady(descTable, false, () => {});
          await utils.fetchAll(descTable);

          // console.log(utils.getResult(descTable).getValue());
          await descTable.close();

          var columns = utils.getResult(descTable).getValue();
          let fields = columns.map(({ field }) => field);

          databaseDetails.push({
            dbName: dbName,
            table: table.tab_name,
            fields: fields,
          });

          // change data to dbName:{tables:{name:"tbName", fields:[]}}
          var databaseDetailsFormatted = databaseDetails.reduce(
            (acc, { dbName, table, fields }) => {
              if (!acc[dbName]) {
                acc[dbName] = {};
              }
              if (!acc[dbName][table]) {
                acc[dbName][table] = {};
              }
              acc[dbName][table] = fields;
              return acc;
            },
            {}
          );

          // console.log(databaseDetailsFormatted);

          // last row
          if (indexTab === rowTab.length - 1) {
            await session.close();
            return callback(null, {
              statusCode: 200,
              databaseDetails: databaseDetailsFormatted[dbName],
            });
          }
        });
      } catch (error) {
        return callback(null, {
          statusCode: 400,
          message: error,
        });
      }
    });
};
