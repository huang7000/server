const express = require("express");
const router = express.Router();
const dbUtil = require("../../db");

const tableName = "T_Menu"
const idKey = "id"
// 新增
router.post("/insertModel", async (req, res) => {
    let model = {};
    for (let obj in req.body) {
        model = JSON.parse(obj);
    }
    //...扩展运算符，两者都可以将相同属性的元素替换掉（前者替换后者），不相同属性的合并
    //Object.assign会改变目标userObj 慎用
    let result = await dbUtil.insertModel({
        ...userObj,
        ...model
    }, tableName);
    res.send(result);
});

// 编辑
router.post("/updateModel", async (req, res) => {
    let model = {};
    for (let obj in req.body) {
        model = JSON.parse(obj);
    }
    let result = await dbUtil.updateModel(model);
    res.send(result);
});

// 批量删除
router.post("/deleteList", async (req, res) => {
    let model = {};
    for (let obj in req.body) {
        model = JSON.parse(obj);
    }
    let result = await dbUtil.deleteList(model);
    res.send(result);
});

// 删除
router.get("/deleteModel", async (req, res) => {
    let result = await dbUtil.deleteModel(tableName, idKey, req.body[idKey]);
    res.send(result);
});


// 查询Model
router.get("/queryModel", async (req, res) => {
    let sqlStr = `SELECT * FROM ${tableName} WHERE ${idKey}=@id`
    let paramObj = {
        id: req.body[idKey]
    }
    let result = await dbUtil.queryModel(sqlStr, paramObj);
    res.send(result);
});

// 查询Page
router.get("/queryPage", async (req, res) => {
    let model = {};
    for (let obj in req.body) {
        model = JSON.parse(obj);
    }
    let sqlStr = "SELECT * FROM T_User "
    let pageNum = req.query.pageNum;
    let pageSize = req.query.pageSize;
    let paramObj = {
        userCode: req.query.userCode,
        userName: req.query.userName
    };
    let sortField = "id";
    let result = await dbUtil.queryPage(sqlStr, pageNum, pageSize, paramObj, sortField);
    res.send(result);
});

// 查询Page
router.get("/buildMenus", async (req, res) => {
    let model = {};
    for (let obj in req.body) {
        model = JSON.parse(obj);
    }
    let sqlStr = `SELECT * FROM  ${tableName}  `
    
    let sortField = "id";
    let result = await dbUtil.queryList(sqlStr);
    res.send(result);
});

module.exports = router;