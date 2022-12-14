/**
 * Responds to HTTP request.
 *
 * @param { app, context, callback }
 * app { getRequester, getPublisher }
 * context { body, cookies, method, params, query, headers }
 * callback(error, response)
 */

const hive = require("hive-driver");
const { NodeSSH } = require("node-ssh");
exports.handler = async ({ app, context, callback }) => {
  const { TCLIService, TCLIService_types } = hive.thrift;
  const client = new hive.HiveClient(TCLIService, TCLIService_types);
  const utils = new hive.HiveUtils(TCLIService_types);

  const { body } = context;
  var { dbName, dbQuery, page } = body;

  // dbQuery add \n after not ; inside string
  dbQuery = dbQuery.replace(/;/g, ";\n");

  // multi \n to single \n
  dbQuery = dbQuery.replace(/\n+/g, "\n");

  // if last char is not ; add ;
  if (dbQuery.slice(-1) !== ";") {
    dbQuery = dbQuery + ";";
  }

  // split dbQuery into array by new line / \n
  var dbQueries = dbQuery.split("\n");

  // replace \t with space
  dbQueries = dbQueries.map((query) => {
    return query.replace(/\t/g, " ");
  });
  // remove empty string
  dbQueries = dbQueries.filter((query) => query !== "");

  let newDbQueries = [];

  // check if query not contain ; at the end of the query join with space + next query and join with space + next query until founds ; and remove the ; from the end of the query

  for (let i = 0; i < dbQueries.length; i++) {
    // recursive function to check if query not contain ; at the end of the query join with space + next query and join with space + next query until founds ; and remove the ; from the end of the query
    function checkQuery(query) {
      if (query[query.length - 1] !== ";") {
        query = query + " " + dbQueries[i + 1];
        i++;
        return checkQuery(query);
      } else {
        return query.slice(0, -1);
      }
    }
    newDbQueries.push(checkQuery(dbQueries[i]));
  }

  // newDbQueries delete empty string
  newDbQueries = newDbQueries.filter((query) => query !== "");
  dbQueries = newDbQueries;

  console.log("newDbQueries", newDbQueries);

  // // check ; at the end of each query and remove it
  // dbQueries.forEach((query, index) => {
  //   if (query.slice(-1) === ";") {
  //     dbQueries[index] = query.slice(0, -1);
  //   }
  // });

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
    console.log("Includes offset");
    var replaceOffset = `offset ${offset}`;
    var replaceOffsetUpper = ` OFFSET ${offset} `;

    offset = (pagination - 1) * 50;
    // apply pagination to the query 50 rows per page
    dbQuery =
      dbQuery.replace(replaceOffset, "").replace(replaceOffsetUpper, "") +
      " LIMIT 50 OFFSET " +
      offset;
  } else if (
    ((query == "SELECT" && !dbQuery.toUpperCase().includes(" LIMIT ")) ||
      (limit > 50 && query == "SELECT")) &&
    !dbQuery.toUpperCase().includes(" OFFSET ")
  ) {
    console.log("Includes limit");
    var replaceLimit = `limit ${limit}`;
    var replaceLimitUpper = ` LIMIT ${limit} `;
    // apply pagination to the query 50 rows per page
    dbQuery =
      dbQuery.replace(replaceLimit, "").replace(replaceLimitUpper, "") +
      " LIMIT 50" +
      " OFFSET " +
      (pagination - 1) * 50;
    offset = (pagination - 1) * 50;
  } else {
    dbQuery = dbQuery + " OFFSET " + (pagination - 1) * 50;
  }

  // var dbName = "gg";
  // var dbQuery = "SELECT * FROM gg2022_2023 LIMIT 10";

  // console.log(page);
  console.log("Query: " + dbQuery);
  console.log("query: " + query);

  if (query == "INSERT") {
    const ssh = new NodeSSH();
    var vm_ip = "10.10.65.1";
    var vm_user = "sapujagad";
    var vm_password = "kayangan";

    //   var command="beeline -u jdbc:hive2://10.207.26.20:10000  -n hive -p hive -e 'select * from "+dbName+"."+dbQuery+" limit 10'";
    var command =
      "beeline -u jdbc:hive2://10.10.65.1:10000 -n hive -p hive -e 'use " +
      dbName +
      ";'";

    dbQuery = command + " '" + dbQuery + "';";

    // console.log(page);
    console.log("Query: " + dbQuery);

    try {
      ssh
        .connect({
          host: vm_ip,
          username: vm_user,
          // privateKey: Buffer.from("..."),
          password: vm_password,
        })
        .then(function () {
          // Command
          ssh
            .execCommand(`${dbQuery}`, {
              onStderr(chunk) {
                console.log("STDERR:", chunk.toString("utf8"));
                if (chunk.toString("utf8").includes("Error")) {
                  return callback(null, {
                    statusCode: 400,
                    message: chunk.toString("utf8"),
                  });
                }
                //   check if chunk is last line
                if (chunk.toString("utf8").includes("Closing")) {
                  return callback(null, {
                    statusCode: 200,
                    query: dbQueryOld,
                    results: "Insert Success",
                  });
                }
              },
            })
            .then(function (result) {
              if (result.stderr) {
                console.log("stderr: " + result.stderr);
              }
              console.log("STDOUT: " + result.stdout);
              return callback(null, {
                statusCode: 200,
                query: dbQueryOld,
                message: "Insert success",
              });
            });
        });
    } catch (error) {
      console.log(error);
      return callback(null, {
        statusCode: 400,
        body: error,
      });
    }
  } else {
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

        console.log("dbName: " + dbName);

        try {
          const useDb = await session.executeStatement(`USE ${dbName}`);
          await utils.waitUntilReady(useDb, false, () => {});
          await utils.fetchAll(useDb);

          console.log("useDb", utils.getResult(useDb).getValue());

          try {
            const operation = await session.executeStatement(dbQuery);
            console.log(
              "🚀 ~ file: hive-query.js ~ line 211 ~ .then ~ operation",
              operation
            );
            await utils.waitUntilReady(operation, false, () => {
              console.log("operation", operation);
            });
            await utils.fetchAll(operation);

            // var logOp = utils.getResult(operation).getValue();

            console.log("operation", utils.getResult(operation).getValue());

            // wait about 10 seconds if !operation
            if (!operation) {
              setTimeout(() => {
                return callback(null, {
                  statusCode: 400,
                  message: "Hive : Failed to execute query",
                });
              }, 10000);
            }

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
            console.log("error", error);
            return callback(null, {
              statusCode: 400,
              query: dbQueryOld,
              message: error.message,
            });
          }
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
  }
};
