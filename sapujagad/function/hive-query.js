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

  // split dbQuery into array by new line / \n
  var dbQueries = dbQuery.split("\n");

  // replace \t with space
  dbQueries = dbQueries.map((query) => {
    return query.replace(/\t/g, " ");
  });
  // remove empty string
  dbQueries = dbQueries.filter((query) => query !== "");

  // check if query not contain ; at the end of the query join with space + next query and remove the ; from the end of the query
  for (var i = 0; i < dbQueries.length; i++) {
    if (dbQueries[i].slice(-1) == ";") {
      dbQueries[i] = dbQueries[i].slice(0, -1);
    }
    if (dbQueries[i + 1] != undefined) {
      dbQueries[i] = dbQueries[i] + " " + dbQueries[i + 1];
      dbQueries.splice(i + 1, 1);
    }
  }

  // check ; at the end of each query and remove it
  dbQueries.forEach((query, index) => {
    if (query.slice(-1) === ";") {
      dbQueries[index] = query.slice(0, -1);
    }
  });

  var lastQuery = dbQueries[dbQueries.length - 1]
    ? dbQueries[dbQueries.length - 1]
    : dbQueries[dbQueries.length - 2];

  dbQuery = lastQuery;

  // if (dbQueries.length > 1) {
  //   return callback(null, {
  //     statusCode: 400,
  //     message: "Hive : Failed to execute query, Only one query is allowed.",
  //   });
  // }

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
  var offset = 0;
  // check if limit > 50
  if (dbQuery.toUpperCase().includes(" LIMIT ")) {
    limit = dbQuery.toUpperCase().split("LIMIT")[1].trim();
  }

  if (dbQuery.toUpperCase().includes(" OFFSET ")) {
    offset = dbQuery.toUpperCase().split("OFFSET")[1].trim();
  }

  // console.log(page);
  console.log("Query: " + dbQuery);

  if (page && dbQuery.toUpperCase().includes(" OFFSET ")) {
    return callback(null, {
      statusCode: 400,
      query: dbQueryOld,
      offset: offset,
      page: page,
      message: "Pagination is not allowed for this query",
    });
  }

  if (
    ((query == "SELECT" && !dbQuery.toUpperCase().includes(" OFFSET ")) ||
      (limit > 50 && query == "SELECT")) &&
    dbQuery.toUpperCase().includes(" OFFSET ")
  ) {
    var replaceOffset = `offset ${offset}`;
    var replaceOffsetUpper = ` OFFSET ${offset} `;
    // apply pagination to the query 50 rows per page
    dbQuery =
      dbQuery.replace(replaceOffset, "").replace(replaceOffsetUpper, "") +
      " LIMIT 50";

    offset = (pagination - 1) * 50;
  }

  if (
    ((query == "SELECT" && !dbQuery.toUpperCase().includes(" LIMIT ")) ||
      (limit > 50 && query == "SELECT")) &&
    !dbQuery.toUpperCase().includes(" OFFSET ")
  ) {
    var replaceLimit = `limit ${limit}`;
    var replaceLimitUpper = ` LIMIT ${limit} `;
    // apply pagination to the query 50 rows per page
    dbQuery =
      dbQuery.replace(replaceLimit, "").replace(replaceLimitUpper, "") +
      " LIMIT 50" +
      " OFFSET " +
      (pagination - 1) * 50;
    offset = (pagination - 1) * 50;
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

        if (
          utils.getResult(operation).getValue() &&
          utils.getResult(operation).getValue().constructor === Array
        ) {
          if (query != "SELECT") {
            return callback(null, {
              statusCode: 200,
              numFound: utils.getResult(operation).getValue().length,
              query: dbQueryOld,
              results: utils.getResult(operation).getValue(),
            });
          }

          return callback(null, {
            statusCode: 200,
            numFound: utils.getResult(operation).getValue().length,
            query: dbQueryOld,
            limit: 50,
            offset: offset,
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
