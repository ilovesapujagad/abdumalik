const logger = require("./logger");

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
};
