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

        databases.map(async (db, index, row) => {
          const useDb = await session.executeStatement(
            "USE " + db.database_name
          );
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

            databasesDetails.push({
              dbName: db.database_name,
              table: table.tab_name,
              fields: fields,
            });

            // change data to dbName:{tables:{name:"tbName", fields:[]}}
            var databasesDetailsFormatted = databasesDetails.reduce(function (r, a) {
              r[a.dbName] = r[a.dbName] || [];
              r[a.dbName].push(a);
              return r;
            }, Object.create(null));

            // console.log(databasesDetailsFormatted);

            // last row
            if (index === row.length - 1 && indexTab === rowTab.length - 1) {
              await session.close();
              return callback(null, {
                statusCode: 200,
                numFound: databasesDetails.length,
                databasesDetails: databasesDetailsFormatted,
              });
            }
          });
        });
      } catch (error) {
        return callback(null, {
          statusCode: 400,
          message: error,
        });
      }
    });
};
