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
  const { body } = context;

  const { functionId, logMsg } = body;

  if (!functionId || !logMsg) {
    return callback(null, {
      statusCode: 400,
      message: "functionId and logMsg are required",
    });
  }

  // millisecond timestamp
  const timestamp = Date.now();

  var userId = "anonymous";

  var pcsId = `U${userId}F${functionId}`;

  var queryEndpoint = `http://10.207.26.22:8886/solr/sapujagad/update/json/docs?commit=true`;

  var bodyData = [
    {
      process_id: pcsId,
      log: logMsg,
      timestamp: timestamp,
    },
  ];

  try {
    const solrCreateDoc = await axios.post(queryEndpoint, bodyData, {
      headers: {
        Accept: "*/*",
        Host: "10.207.26.22",
        "User-Agent": "axios/0.21.1",
        Connection: "keep-alive",
        "Content-Type": "application/json",
      },
    });

    const { data } = solrCreateDoc;
    console.log("data", data);

    return callback(null, {
      statusCode: 200,
      message: "success",
      process_id: pcsId,
    });
  } catch (error) {
    // console.log(error);
    return callback(null, {
      statusCode: 400,
      message: error,
    });
  }
};
