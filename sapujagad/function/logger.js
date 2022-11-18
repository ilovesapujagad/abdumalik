/**
 * Responds to HTTP request.
 *
 * @param { app, context, callback }
 * app { getRequester, getPublisher }
 * context { body, cookies, method, params, query, headers }
 * callback(error, response)
 */

const axios = require("axios");
exports.handler = async ({ app, context, callback }) => {
  const { query } = context;

  const { process_id: processId } = query;

  if (!processId) {
    return callback(null, {
      statusCode: 400,
      message: "process_id is required",
    });
  }

  // millisecond timestamp
  const timestamp = Date.now();

  var userId = "anonymous";

  var functionId = "logger";

  var pcsId = `U${userId}F${functionId}T${timestamp}`;

  //   console.log("pcsId", pcsId);

  var queryEndpoint = `http://10.207.26.22:8886/solr/sapujagad/select?q=process_id:${processId}`;

  try {
    const response = await axios.get(queryEndpoint, {
      headers: {
        Accept: "*/*",
        Host: "10.207.26.22",
        "User-Agent": "axios/0.21.1",
        Connection: "keep-alive",
      },
    });
    const { data } = response;
    var {
      response: { docs },
    } = data;

    if (docs.length === 0) {
      return callback(null, {
        statusCode: 404,
        process_id: processId,
        message: "process_id not found",
      });
    }

    docs = docs.map((doc) => {
      return {
        // version: doc._version_,
        // id: doc.id,
        // process_id: doc.process_id[0],
        timestamp: doc.timestamp[0],
        time: new Date(doc.timestamp[0]).toLocaleString(),
        log_str: doc.log[0],
      };
    });

    // docs sort by timestamp
    docs.sort((a, b) => {
      return a.timestamp - b.timestamp;
    });

    return callback(null, {
      statusCode: 200,
      message: "success",
      logs: docs,
    });
  } catch (error) {
    // console.log(error);
    return callback(null, {
      statusCode: 400,
      message: error,
    });
  }
};
