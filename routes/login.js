const express = require("express");
const router = express.Router();
const dbUtil = require("../db");

const tableName="T_User"
const idKey="id"
// 登录接口 后续需要添加缓存
router.post("/login", async (req, res) => {
  //t_targetTable T_User
  let sqlStr = `select * from ${tableName} where userCode=@userCode`;
  //post的参数在req.body中
  //先根据名称查询是否存在用户
  let userCode = req.body.userCode;
  let password = req.body.password;
  let result = await dbUtil.query(sqlStr, {
    userCode
  });
  if (result.status === "ok" && result.message === "操作成功") {
    result["model"] = result.recordset[0]
    if (result.model.password === password) {
      result.message = "登录成功";
    }
    else {
      result.message = "密码错误";
      result.status = "error";
    }
  } else {
    result.message = "该用户不存在";
    result.status = "error";
  }
  res.send(result);
});

module.exports = router;