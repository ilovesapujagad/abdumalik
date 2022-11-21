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
  var { dbName, dbQuery, page } = body;

  var dbQueryOld = dbQuery;
  let pagination;

  if (!page | (page < 1)) {
    pagination = 1;
  } else {
    // pagination = page to int
    pagination = parseInt(page);
  }

  if (!dbName || !dbQuery) {
    return callback(null, {
      statusCode: 400,
      message: "dbName and dbQuery are required",
    });
  }

  var hiveQueries = [
    "USE",
    "SELECT",
    "INSERT",
    "CREATE",
    "DROP",
    "ALTER",
    "DESCRIBE",
    "SHOW",
    "LOAD",
    "WITH",
  ];

  // Check if the query is a valid Hive query
  var query = null;
  for (var i = 0; i < hiveQueries.length; i++) {
    if (dbQuery.toUpperCase().startsWith(hiveQueries[i])) {
      query = hiveQueries[i];
      break;
    }
  }

  let limit;
  // check if limit > 50
  if (
    dbQuery.toUpperCase().includes(" LIMIT ") ||
    dbQuery.toUpperCase().includes(" limit ")
  ) {
    limit = dbQuery.toUpperCase().split("LIMIT")[1].trim();
  }

  if (
    (query == "SELECT" && !dbQuery.toUpperCase().includes(" LIMIT ")) ||
    (query == "SELECT" && !dbQuery.toUpperCase().includes(" OFFSET ")) ||
    limit > 50
  ) {
    console.log("limit", `limit ${limit}`);
    var replaceLimit = `limit ${limit}`;
    var replaceLimitUpper = ` LIMIT ${limit} `;
    // apply pagination to the query 50 rows per page
    dbQuery =
      dbQuery.replace(replaceLimit, "").replace(replaceLimitUpper, "") +
      " LIMIT 50" +
      " OFFSET " +
      (pagination - 1) * 50;
  }

  // console.log("Query: " + dbQuery);

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

        if (
          utils.getResult(operation).getValue() &&
          utils.getResult(operation).getValue().constructor === Array
        ) {
          return callback(null, {
            statusCode: 200,
            numFound: utils.getResult(operation).getValue().length,
            query: dbQueryOld,
            limit: 50,
            currPage: pagination,
            results: utils.getResult(operation).getValue(),
          });
        }

        return callback(null, {
          statusCode: 200,
          query: dbQueryOld,
          message: query + " success",
        });
      } catch (error) {
        var errMsg = error.message;
        var msg =
          (errMsg.split("line")[1]
            ? errMsg.split("line")[1].substring(errMsg.indexOf(" "))
            : errMsg.split("Line")[1]
            ? errMsg.split("Line")[1].substring(errMsg.indexOf(" ") + 1)
            : errMsg) || error;
        msg = msg[0].toUpperCase() + msg.substring(1);
        return callback(null, {
          statusCode: 400,
          query: dbQuery,
          message: msg,
        });
      }
    });
};
