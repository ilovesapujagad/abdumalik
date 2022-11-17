const axios = require("axios").default;
const pluralize = require("pluralize");

module.exports = ({ url, accessToken }) => {
  if (!url) {
    throw "Your project url must be set!";
  }

  const requester = () => {
    return axios.create({
      baseURL: url + "/api/",
      headers: {
        authorization: `Bearer ${accessToken}`,
      },
    });
  };

  const method = (type) => {
    switch (type) {
      case "create":
        return "post";

      case "find":
        return "get";

      case "findConnectionOwn":
        return "get";

      case "findOwn":
        return "get";

      case "findConnection":
        return "get";

      default:
        return type;
    }
  };

  const buildQuery = (params) => {
    let selectQuery, whereQuery, orQuery, orderQuery, skipQuery, limitQuery;

    if (params?.select && Array.isArray(params.select)) {
      selectQuery = `select=${params.select.join(",")}`;
    }

    if (params?.where && typeof params.where === "object") {
      whereQuery = Object.keys(params.where)
        .map((key) => `where[${key}]=${params.where[key]}`)
        .join("&");
    }

    if (params?.orderBy && typeof params.orderBy === "string") {
      orderQuery = `orderBy=${params.orderBy}`;
    }

    if (params?.skip && typeof skip === "number") {
      skipQuery = `skip=${params.skip}`;
    }

    if (params?.limit && typeof limit === "number") {
      limitQuery = `limit=${params.limit}`;
    }

    if (params?.or && Array.isArray(params.or)) {
      orQuery = params.or
        .map((item, i) => {
          if (typeof item !== "object") {
            return null;
          }

          return Object.keys(item)
            .map((key) => `or[${4}][${key}]=${item[key]}`)
            .join("&");
        })
        .filter((i) => !!i)
        .join("&");
    }

    const query = Object.values({
      selectQuery,
      whereQuery,
      orQuery,
      orderQuery,
      skipQuery,
      limitQuery,
    })
      .filter((i) => !!i)
      .join("&");

    return query ? `?${query}` : "";
  };

  const getRequester = (name) => {
    return {
      send: async (req) => {
        const { type, body, id, ...params } = req;

        const reqMethod = method(type);
        let path = `/${pluralize(name)}`;

        if (type.includes("Connection")) {
          path += "Connection";
        }

        if (id) {
          path += `/${id}`;
        }

        const query = buildQuery(params);

        try {
          const { data } = await requester()[reqMethod](
            path + query,
            body ?? params,
            params
          );

          return data;
        } catch (e) {
          if (typeof e.toJSON === "function") {
            throw e.toJSON();
          }

          throw e.toString();
        }
      },
    };
  };

  return {
    getRequester,
  };
};
