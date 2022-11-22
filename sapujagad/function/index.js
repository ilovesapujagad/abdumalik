const logger = require("./logger");
const oracleToHdfs = require("./oracle-to-hdfs");
const oracleToHive = require("./oracle-to-hive");
const logCreate = require("./log-create");
const hiveQuery = require("./hive-query");
const databasesDetails = require("./databases-details");
const databaseDetails = require("./database-details");
const databases = require("./databases");

class FunctionEvent {
  constructor(req) {
    this.body = req.body;
    this.headers = req.headers;
    this.method = req.method;
    this.query = req.query;
    this.path = req.path;
    this.user = req.user;
  }
}

let isArray = (a) => {
  return !!a && a.constructor === Array;
};

let isObject = (a) => {
  return !!a && a.constructor === Object;
};

exports.handler = (app) => {
  const middleware = async (req, res, mgFunction) => {
    let cb = (err, functionResult) => {
      if (err) {
        return res.status(500).send(err.toString ? err.toString() : err);
      }

      let statusCode = 200;
      if (typeof functionResult == "object" && functionResult.statusCode) {
        statusCode = functionResult.statusCode;
        delete functionResult.statusCode;
      }

      if (isArray(functionResult) || isObject(functionResult)) {
        return res.status(statusCode).send(functionResult);
      } else {
        return res.status(statusCode).send(functionResult);
      }
    };

    let fnEvent = new FunctionEvent(req);

    Promise.resolve(mgFunction.handler({ app, context: fnEvent, callback: cb }))
      .then((res) => {})
      .catch((e) => {});
  };

  app.get("/logs", (req, res) => middleware(req, res, logger));
  app.get("/oracle-to-hdfs", (req, res) => middleware(req, res, oracleToHdfs));
  app.get("/oracle-to-hive", (req, res) => middleware(req, res, oracleToHive));
  app.post("/log-create", (req, res) => middleware(req, res, logCreate));
  app.post("/hive-query", (req, res) => middleware(req, res, hiveQuery));
  app.get("/hive-databases", (req, res) => middleware(req, res, databases));
  app.get("/hive-databases-details", (req, res) =>
    middleware(req, res, databasesDetails)
  );
  app.get("/hive-database-details", (req, res) =>
    middleware(req, res, databaseDetails)
  );
};
