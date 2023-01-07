/**
 * Responds to HTTP request.
 *
 * @param { app, context, callback }
 * app { getRequester, getPublisher }
 * context { body, cookies, method, params, query, headers }
 * callback(error, response)
 */

const { NodeSSH } = require("node-ssh");
var CryptoJS = require("crypto-js");

exports.handler = async ({ app, context, callback }) => {
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

  var secretKey = "12345678901234567890123456789012";
  const ssh = new NodeSSH();

  // Decrypt
  function decrypt(text) {
    var bytes = CryptoJS.AES.decrypt(text, secretKey);
    var originalText = bytes.toString(CryptoJS.enc.Utf8);
    return originalText;
  }

  var vm_ip = "10.10.65.1";
  var vm_user = "sapujagad";
  var vm_password = "kayangan";

  //   var command="beeline -u jdbc:hive2://10.10.65.1:10000  -n hive -p hive -e 'select * from "+dbName+"."+dbQuery+" limit 10'";
  var command =
    "beeline -u jdbc:hive2://10.10.65.3:10500 -n hive -p hive -e 'use " +
    dbName +
    ";'";

  dbQuery = command + " '" + dbQuery + ";'";

  // console.log(page);
  console.log("Query: " + dbQuery);

  let stdOut = "";

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
          .execCommand(`sudo -Hu hive ${dbQuery}`, {
            onStdout(chunk) {
              console.log("STDOUT:", chunk.toString("utf8"));
              stdOut += chunk.toString("utf8");
            },

            onStderr(chunk) {
              console.log("STDERR:", chunk.toString("utf8"));
            },
            // onStderr(chunk) {
            //   console.log("STDERR:", chunk.toString("utf8"));
            //   if (chunk.toString("utf8").includes("Error")) {
            //     return callback(null, {
            //       statusCode: 400,
            //       message: chunk.toString("utf8"),
            //     });
            //   }
            //   //   check if chunk is last line
            //   if (chunk.toString("utf8").includes("Closing")) {
            //     if (query != "SELECT") {
            //       return callback(null, {
            //         statusCode: 200,
            //         query: dbQueryOld,
            //         results: chunk.toString("utf8"),
            //       });
            //     }

            //     return callback(null, {
            //       statusCode: 200,
            //       query: dbQueryOld,
            //       limit: 50,
            //       offset: offset,
            //       currPage: pagination,
            //       results: chunk.toString("utf8"),
            //     });
            //   }

            //   if (
            //     !chunk
            //       .toString("utf8")
            //       .includes("INFO  : Completed compiling command(queryId=")
            //   ) {
            //     return callback(null, {
            //       statusCode: 200,
            //       query: dbQueryOld,
            //       results: "Query is running",
            //     });
            //   }
            // },
          })
          .then(function (result) {
            console.log("STDOUT:", result.stdout);
            return callback(null, {
              statusCode: 200,
              query: dbQueryOld,
              offset: offset,
              page: page,
              results: stdOut,
            });
            // if (result.stderr) {
            //   console.log("stderr: " + result.stderr);
            // }
            // console.log("STDOUT: " + result.stdout);
            // return callback(null, {
            //   statusCode: 200,
            //   query: dbQueryOld,
            //   results: result.stdout,
            // });
          });
      });
  } catch (error) {
    console.log(error);
    callback(null, {
      statusCode: 400,
      body: error,
    });
  }
};
