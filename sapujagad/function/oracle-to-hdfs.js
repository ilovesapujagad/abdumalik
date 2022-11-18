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
  const { query } = context;
  console.log("query", query.secret);

  if (!query.secret || query.secret != "OncnCjYynKaYmuQ1cVjlHneqhmQRcjb") {
    console.log("secret not match");
    return callback(null, {
      status: 403,
      message: "Forbidden",
    });
  }

  var secretKey = "12345678901234567890123456789012";
  const ssh = new NodeSSH();

  // date format YYYY-MM-DD
  function getDate(date) {
    const year = date.getFullYear();
    const month = date.getMonth() + 1;
    const day = date.getDate();
    console.log(`${year}-${month}-${day}`);
    return `${year}-${month}-${day}`;
  }

  // start date = yesterday first second
  var startDate = getDate(
    new Date(new Date().setDate(new Date().getDate() - 1))
  );

  // end date = yesterday last second
  var dueDate = getDate(new Date(new Date().setDate(new Date().getDate() - 1)));

  var m = 1;

  // Decrypt
  function decrypt(text) {
    var bytes = CryptoJS.AES.decrypt(text, secretKey);
    var originalText = bytes.toString(CryptoJS.enc.Utf8);
    return originalText;
  }

  // console.log(app.env);

  var db_ip = decrypt("U2FsdGVkX196GEYWBS2d6sKT7fK3zFtQiUn72nxKYHI=");
  var db_user = decrypt("U2FsdGVkX18bG3piiQ25ImgPAPeKqRnM+7a6ARlQwuc=");
  var db_password = decrypt("U2FsdGVkX19rnLkBKG66xkp70U/tjtuYK7ghiZIL9XE=");
  var db_port = decrypt("U2FsdGVkX18Ub78E/WDdlIpeb79Vf6PGGNL8eVRKjBw=");
  var db_name = decrypt("U2FsdGVkX1/qEICWdOk4tDBrpV0uhlsDT3fkgGk5Vdw=");

  var vm_ip = decrypt("U2FsdGVkX18OuoXStwyjHc/1LMtZEHUufle9empVyyI=");
  var vm_user = decrypt("U2FsdGVkX1+cr47RXl5u6ru+KnwfKP4epjXLlIK66/k=");
  var vm_password = decrypt("U2FsdGVkX1/H15u4rMR9MCjrsk7ogAys1HDm/zC0Jcg=");

  var targetDir = "/user/apps";

  // console.log("DB", db_ip, db_user, db_password, db_port, db_name);
  // console.log("VM", vm_ip, vm_user, vm_password);

  var scoopCmd = `sqoop import \
   --connect jdbc:oracle:thin:@//${db_ip}:${db_port}/${db_name} \
   --username ${db_user} \
   --password ${db_password} \
   --query "SELECT GJH.JE_HEADER_ID,
          GJH.BATCH_NAME,
          GJH.NAME JOURNAL_NAME,
          GJH.JE_SOURCE,
          GJH.JE_CATEGORY,
          GJH.DESCRIPTION HDR_DESCRIPTION,
          GJH.BATCH_POSTED_DATE,
          GJH.CREATION_DATE,
          GJH.CURRENCY_CODE,
          GJH.RUNNING_TOTAL_DR,
          GJH.RUNNING_TOTAL_CR,
          GJH.RUNNING_TOTAL_ACCOUNTED_DR,
          GJH.RUNNING_TOTAL_ACCOUNTED_CR,
          GJH.LEDGER_ID,
          GJH.PERIOD_NAME,
          GJH.STATUS HDR_STATUS,
          GJL.JE_LINE_NUM,
          GJL.SEGMENT2 KODE_PP,
          GJL.SEGMENT3 ACCOUNT,
          GJL.SEGMENT4 SUB_ACCOUNT,
          CASE
          WHEN GJL.SEGMENT4 = '00' THEN
              (SELECT FFVL.DESCRIPTION
                 FROM FND_FLEX_VALUES_VL FFVL
                 WHERE FFVL.FLEX_VALUE_SET_ID = 1014176
                 AND FFVL.FLEX_VALUE = GJL.SEGMENT3)
           ELSE
               (SELECT FFVL.DESCRIPTION
                 FROM FND_FLEX_VALUES_VL FFVL
                 WHERE FFVL.FLEX_VALUE_SET_ID = 1014176
                 AND FFVL.FLEX_VALUE = GJL.SEGMENT3)||'-'||
               (SELECT FFVL.DESCRIPTION
                 FROM FND_FLEX_VALUES_VL FFVL
                 WHERE FFVL.FLEX_VALUE_SET_ID = 1014177
                 AND FFVL.PARENT_FLEX_VALUE_LOW = GJL.SEGMENT3
                 AND FFVL.FLEX_VALUE = GJL.SEGMENT4)
          END ACCOUNT_DESC,
          (GJL.SEGMENT1
          || '.'
          || GJL.SEGMENT2
          || '.'
          || GJL.SEGMENT3
          || '.'
          || GJL.SEGMENT4
          || '.'
          || GJL.SEGMENT5
          || '.'
          || GJL.SEGMENT6 )GL_ACCOUNT,
          GJL.DESCRIPTION LINE_DESCRIPTION,
          GJL.ACCOUNTED_DR,
          GJL.ACCOUNTED_CR,
          GJL.STATUS,
          FU.USER_NAME ENTERED_BY
     FROM GL_JE_HEADERS_V GJH,
          GL_JE_LINES_V GJL,
          GL_JE_BATCHES GJB,
          FND_USER FU,
          GL_LEDGERS GL
   WHERE GJH.LEDGER_ID = GL.LEDGER_ID
   AND GJH.JE_HEADER_ID = GJL.JE_HEADER_ID
   AND GJH.JE_BATCH_ID = GJB.JE_BATCH_ID (+)
   AND GJH.CREATED_BY = FU.USER_ID
   AND GJH.STATUS = 'P'
   AND trunc(GJH.CREATION_DATE) between to_date('${startDate}', 'YYYY-MM-DD') and to_date('${dueDate}', 'YYYY-MM-DD') AND \\$CONDITIONS" \
   --split-by JE_HEADER_ID \
   --target-dir ${targetDir} \
   --append \
   -m ${m}`;
  // console.log("scoopCmd", scoopCmd);

  var chunkI = 0;

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
          .execCommand(`pwd | ${scoopCmd}`, {
            // onStdout(chunk) {
            //   console.log("STDOUT:", chunk.toString("utf8"));
            //   if (chunkI == 0) {
            //     callback(null, {
            //       statusCode: 200,
            //       message: "Importing...",
            //     });
            //   }
            //   chunkI++;
            // },
            onStderr(chunk) {
              console.log("STDERR:", chunk.toString("utf8"));
              if (chunkI == 0) {
                callback(null, {
                  statusCode: 200,
                  message: "Importing...",
                });
              }
              chunkI++;
            },
          })
          .then(function (result) {
            if (result.stderr) {
              console.log("stderr: " + result.stderr);
              if (chunkI == 0) {
                callback(null, {
                  statusCode: 400,
                  body: result.stdout,
                });
              }
            }
            console.log("STDOUT: " + result.stdout);
            // callback(null, {
            //   statusCode: 200,
            //   body: result.stdout,
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
