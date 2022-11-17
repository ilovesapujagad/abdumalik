"use strict";

const express = require("express");
const app = express();
const bodyParser = require("body-parser");
const cors = require("cors");
const mgFunction = require("./function");
const mgSDK = require("./sdk");
const axios = require("axios");

require("dotenv").config();

if (process.env.RAW_BODY === "true") {
  app.use(bodyParser.raw({ type: "*/*" }));
} else {
  const jsonLimit = process.env.MAX_JSON_SIZE || "100kb"; //body-parser default
  app.use(bodyParser.json({ limit: jsonLimit }));
  app.use(bodyParser.raw());
  app.use(bodyParser.text({ type: "text/*" }));
}

app.disable("x-powered-by");

app.use(cors());
app.use(async (req, res, next) => {
  let bearerToken = req.headers["authorization"];
  var token = bearerToken ? bearerToken.split(" ")[1] : "";
  const { getRequester } = mgSDK({
    url: process.env.URL,
    accessToken: token,
  });
  app.getRequester = getRequester;

  app.env = process.env;
  // authentication
  if (process.env.AUTHENTICATION && process.env.AUTHENTICATION != "public") {
    try {
      if (token) {
        console.log("token =>", token);
        // get user from token
        const getUser = await axios.get(`${process.env.URL}/api/user`, {
          headers: {
            Authorization: `Bearer ${token}`,
          },
        });
        // console.log("getUser =>", getUser.data);
        // if (getUser.data) {
        //   req.user = getUser.data;
        // } else {
        //   res.status(401).send({
        //     message: "Unauthorized",
        //   });
        // }

        // const verify = await getRequester("user").send({
        //   type: "verifyToken",
        //   token,
        // });
        // console.log("verify =>", verify);
        // if (verify.sub) {
        //   req.user = verify.user;
        // } else {
        //   throw new Error();
        // }
      } else {
        throw new Error();
      }
    } catch (error) {
      return res.status(401).send({ message: "UnAuthorized" });
    }
  }
  // connection
  if (process.env.CONNECTION == "private") {
    try {
      let isSystem = req.headers["is-system"];
      if (isSystem) {
        delete req.headers["is-system"];
      } else {
        throw new Error();
      }
    } catch (error) {
      return res.status(403).send({ message: "Access is forbidden" });
    }
  }
  next();
});

app.get("/favicon.ico", (_, res) => res.status(204).end());

mgFunction.handler(app);

const port = process.env.http_port || 3000;

app.listen(port, () => {
  console.log(`Server listening on port: ${port}`);
});
