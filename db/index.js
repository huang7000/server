/* eslint-disable no-unused-vars */
/* eslint-disable no-unsafe-finally */
/* eslint-disable no-dupe-args */
/* eslint-disable no-undef */
/* eslint-disable prettier/prettier */
const mssql = require("mssql");
//const log4js = require("log4js");
//const logger = log4js.getLogger("dbUtil");
const connConfig = {
    "user": "sa",
    "password": "huang7000@qq.com",
    "server": "49.233.164.105",
    "database": "test",
    "connectionTimeout": 120000,
    "requestTimeout": 3000000,
    "retryTimes": 3,
    "options": {
        "encrypt": false
    },
    "pool": {
        "max": 1024,
        "min": 1,
        "idleTimeoutMillis": 30000
    }
};
mssql.on('error', err => {
    // ... error handler
    //logger.error(err);
});
let connectionPool;

let getConnection = async function () { //连接数据库
    if (!(connectionPool && connectionPool.connected)) {
        connectionPool = await mssql.connect(connConfig);
    }
    return connectionPool;
}

/**
 * @method 执行
 * @param {String} sqlStr 执行sql语句
 * @param {Object} paramObj 执行input参数
 * @return {Object} 执行结果
 */
let query = async function (sqlStr, paramObj) { //写sql语句自由查询
    await mssql.close(); // close
    let pool = await getConnection();
    let request = pool.request();
    if (paramObj) {
        for (let index in paramObj) {
            request.input(index, paramObj[index]);
        }
    }
    let result = {};
    try {
        result = await request.query(sqlStr);
        result["status"] = "ok";
        result["message"] = "操作成功";
        result["error"] = "";
    } catch (error) {
        result["status"] = "error";
        result["message"] = "操作失败";
        result["error"] = error
    } finally {
        await mssql.close(); // close
        return result;
    }

};

/**
 * @method 存储过程
 * @param {String} procedure_name 存储过程名称
 * @param {Object} inputparams 执行input参数
 * @param {Object} outputparams 执行input参数
 * @return {Object} 存储过程结果
 */
let execute = async function (procedure_name, inputparams, outputparams) {
    //写sql语句自由查询
    await mssql.close(); // close
    let pool = await getConnection();
    let request = pool.request();
    if (inputparams) {
        for (let index in inputparams) {
            request.input(index, inputparams[index]);
        }
    }
    if (outputparams) {
        for (let index in outputparams) {
            request.output(index, outputparams[index]);
        }
    }
    let result = {};
    try {
        result = await request.execute(procedure_name);
        result["status"] = "ok";
        result["message"] = "操作成功";
        result["error"] = "";
    } catch (error) {
        result["status"] = "error";
        result["message"] = "操作失败";
        result["error"] = error
    } finally {
        await mssql.close(); // close
        return result;
    }
};

/**
 * @method 事务
 * @param {Array} sqlObjList 事务执行sql集合
 * @return {Object} 存储过程结果
 */

let transaction = async function (sqlObjList) { //写sql语句自由查询
    await mssql.close(); // close
    const connpool = await new mssql.ConnectionPool(connConfig);
    const pool = await connpool.connect();
    const transaction = await new mssql.Transaction(pool)
    await transaction.begin()
    let rolledBack = false
    transaction.on('rollback', aborted => {
        rolledBack = true
    })
    const request = await new mssql.Request(transaction);
    let res = {};
    try {
        let sqlStr = "";
        //of 跟 in 有差别
        for (let sqlObj of sqlObjList) {
            sqlStr = sqlStr + sqlObj["sqlStr"];
            if (!sqlStr.endsWith(";")) {
                sqlStr = sqlStr + ";";
            }
            if (sqlObj["paramObj"]) {
                for (let index in paramObj) {
                    request.input(index, sqlObj["paramObj"][index]);
                }
            }
        }
        res = await request.query(sqlStr);
        res["status"] = "ok";
        res["message"] = "操作成功";
        res["error"] = "";
        await transaction.commit()
    } catch (error) {
        res["status"] = "error";
        res["message"] = "操作失败";
        res["error"] = error;
        await transaction.rollback()
    } finally {
        await mssql.close();
        return res;
    }
};
//modelList不分页

/**
 * @method 列表查询
 * @param {String} sqlStr 执行sql语句
 * @param {Object} paramObj 执行input参数
 * @return {Object} 执行结果
 */
let queryList = async function (sqlStr, paramObj) { //写sql语句自由查询
    let res = await query(sqlStr, paramObj);
    if (res["status"] === "ok") {
        res["list"] = res["recordset"];
        res["message"] = "查询成功";
    } else {
        res["list"] === [];
        res["message"] = "查询失败";
    }
    let result = {
        status: res.status,
        message: res.message,
        error: res.error,
        list: res.list
    };
    return result;
};
/**
 * @method 列表分页查询
 * @param {String} sqlStr 执行sql语句
 * @param {Object} paramObj 执行input参数
 * @param {sortField} sortField 排序字段
 * @param {Object} pageNum 当前页码
 * @param {String} pageSize 页码大小
 * @return {Object} 执行结果
 */
let queryPage = async function (sqlStr,pageNum, pageSize,paramObj, sortField) { //写sql语句自由查询
    //select * from t_targetTable  order by id offset 2 rows fetch next 2 rows only
    let sqlCount = `select Count(*) as total from (${sqlStr}) T;`;
    sqlStr = sqlStr + ` order by ${sortField}`;
    sqlStr = sqlStr + ` offset ${(pageNum-1)*pageSize} rows fetch next ${pageSize} rows only;`;
    let res = await query(sqlStr + sqlCount, paramObj);
    if (res["status"] === "ok") {
        res["list"] = res["recordsets"][0];
        res["total"] = res["recordsets"][1][0]["total"];
        res["message"] = "查询成功";
    } else {
        res["list"] === [];
        res["message"] = "查询失败";
    }
    let result = {
        status: res.status,
        message: res.message,
        error: res.error,
        list: res.list,
        total: res.total
    };
    return result;
};

/**
 * @method 实体查询
 * @param {String} tableName 目标表
 * @param {Object} idKey id名
 * @param {Object} idValue id值
 * @return {Object} 执行结果
 */
let queryModel = async function (tableName, idKey, idValue) {
    let res = {
        status: "ok",
        message: "查询成功",
        error: "",
        model: {},
        modelList: []
    };
    try {
        let sqlStr = `select * from ${tableName} where ${idKey}=@id`;
        let paramObj = {
            id: idValue
        }
        let result = await query(sqlStr, paramObj);
        if (Array.isArray(result.recordset)) {
            if (result.recordset.length === 1) {
                res["model"] = result.recordset[0];
            } else {
                res["message"] = "存在多个实体";
                res["modelList"] = result.recordset;
            }

        } else {
            res["message"] = "不存在实体";
        }
    } catch (error) {
        res["status"] = "error";
        res["error"] = error;
        res["message"] = "查询失败";
    } finally {
        await mssql.close(); // close
        return res;
    }
};


/**
 * @method 实体新增
 * @param {Object} model 实体
 * @param {String} tableName 表名
 * @param {String} idKey 默认
 * @param {Boolean} idFilter 默认
 * @param {Object} defObj 默认
 * @return {Object} 执行结果
 */
let insertModel = async function (model, tableName, idKey, idFilter, defObj) { //添加数据
    let res = {
        status: "error",
        message: "新增失败",
        error: ""
    };
    if (!model) {
        res.error = "新增数据为空";
        return res;
    }
    let connection = await getConnection();
    let request = connection.request();
    let sqlStr = "";
    sqlStr += "INSERT INTO " + tableName + "(";

    //for in 对象时 循环的是属性字段
    for (let index in model) {
        // 过滤Id时并且index等于idkey时
        if (!(idFilter && index === idKey)) {
            sqlStr += index + ",";
        }
    }
    sqlStr = sqlStr.trimEnd(" ").trimEnd(",") + " ) ";
    sqlStr = sqlStr + " values(";
    for (let index in model) {
        // 过滤Id时并且index等于idkey时
        if (!(idFilter && index === idKey)) {
            if (typeof defObj === "object" && defObj !== {} && defObj[index] !== undefined) {
                sqlStr = sqlStr + `${defObj[index]},`;
            } else {
                sqlStr = sqlStr + "@" + index + ",";
                request.input(index, model[index]);
            }
        }

    }
    sqlStr = sqlStr.trimEnd(" ").trimEnd(",") + " ) ";
    try {
        let result = await request.query(sqlStr);
        //rowsAffected:
        if (result.rowsAffected.length === 1 && result.rowsAffected[0] === 1) {
            res["status"] = "ok";
            res["message"] = "新增成功";
            res["error"] = "";
            res["count"] = result.rowsAffected[0];
        } else {
            res["status"] = "error";
            res["message"] = "新增失败";
        }
    } catch (error) {
        res["status"] = "error";
        res["message"] = "新增失败，请联系管理员";
        res["error"] = error;
    } finally {
        await mssql.close(); // close
        return res;
    }
};

/**
 * @method 实体编辑
 * @param {Object} model 实体
 * @param {String} tableName 表名
 * @param {String} idKey 主键id
 * @param {String} idValue 主键值
 * @param {Object} defObj 默认
 * @return {Object} 执行结果
 */
let updateModel = async function (model, tableName, idKey, idValue, defObj) {
    let res = {
        status: "error",
        message: "编辑失败",
        error: ""
    };
    if (!model) {
        res.error = "编辑数据为空";
        return res;
    }
    if (!whereStr) {
        res.error = "编辑数据条件为空";
        return res;
    }
    await mssql.close(); // close
    //修改要改成事务处理，因为有可能休息到多条数据需要回滚
    let connection = await getConnection();
    let request = connection.request();
    let sqlStr = `UPDATE ${tableName} SET `;
    if (model) {
        for (let index in model) {
            if (!(idFilter && index === idKey)) {
                if (typeof defObj === "object" && defObj !== {} && defObj[index] !== undefined) {
                    sqlStr = sqlStr + index + `=${defObj[index]},`;
                } else {
                    sqlStr = sqlStr + index + "=@" + index + ",";
                    request.input(index, model[index]);
                }
            }
        }
    }
    sqlStr = sqlStr.trimEnd(" ").trimEnd(",") + " ";
    sqlStr = sqlStr + ` where ${idKey}=@id`;
    request.input(idKey, idValue);

    try {
        let result = await request.query(sqlStr);
        //rowsAffected:
        if (result.rowsAffected.length === 1) {
            res["status"] = "ok";
            res["message"] = "编辑成功";
            res["error"] = "";
            res["count"] = result.rowsAffected[0];
        } else {
            res["status"] = "error";
            res["message"] = "编辑失败";
            res["error"] = "";
        }
    } catch (error) {
        res["status"] = "error";
        res["message"] = "编辑失败，请联系管理员";
        res["error"] = error;
    } finally {
        await mssql.close(); // close
        return res;
    }
};


/**
 * @method 删除
 * @param {String} tableName 目标表
 * @param {Object} idKey id名
 * @param {Object} idValue id值
 * @return {Object} 执行结果
 */
let deleteModel = async function (tableName, idKey, idValue) {
    let res = {
        status: "error",
        message: "编辑失败",
        error: "",
        count: 0
    };

    try {
        let sqlStr = `delete from ${tableName} where ${idKey}=@id`;
        let paramObj = {
            id: idValue
        }
        let result = await query(sqlStr, paramObj);
        //rowsAffected:
        if (result.rowsAffected.length === 1) {
            res["status"] = "ok";
            res["message"] = "删除成功";
            res["count"] = result.rowsAffected[0];
            res["error"] = "";
        } else {
            res["status"] = "error";
            res["message"] = "删除失败";
        }
    } catch (error) {
        res["status"] = "error";
        res["message"] = "删除失败，请联系管理员";
        res["error"] = error;
    } finally {
        await mssql.close(); // close
        return res;
    }
};


/**
 * @method 批量删除
 * @param {String} sqlStr 执行sql语句
 * @param {Object} paramObj 执行input参数
 * @return {Object} 执行结果
 */
let deleteList = async function (sqlStr, paramObj) {
    let res = {
        status: "error",
        message: "编辑失败",
        error: "",
        count: 0
    };

    try {
        let result = await query(sqlStr, paramObj);
        //rowsAffected:
        if (result.rowsAffected.length === 1) {
            res["status"] = "ok";
            res["message"] = "删除成功";
            res["count"] = result.rowsAffected[0];
            res["error"] = "";
        } else {
            res["status"] = "error";
            res["message"] = "删除失败";
        }
    } catch (error) {
        res["status"] = "error";
        res["message"] = "删除失败，请联系管理员";
        res["error"] = error;
    } finally {
        await mssql.close(); // close
        return res;
    }
};


/**
 * @method 合并数据执行增删改
 * @param {String} targetTable 目标表
 * @param {Object} obj 执行字段
 * @param {Object} whereObj 条件对象
 * @param {Object} guid guid
 * @return {Object} 执行结果
 */
let saveModel = async function (targetTable, model, idKey, idValue, idFilter, defObj) {
    let sqlStr = "";
    sqlStr = sqlStr + `
    MERGE INTO ${targetTable}  AS T
    USING  `
    sqlStr = sqlStr + `
    (SELECT `;
    for (let index in model) {
        if (typeof defObj === "object" && defObj !== {} && defObj[index] !== undefined) {
            sqlStr = sqlStr + `'${defObj[index]}' AS  ${index},,`;
        } else {
            sqlStr = sqlStr + ` '${model[index]}' AS  ${index},`;
        }
    }
    sqlStr = sqlStr + ` ) AS S   
    ON   AND T.${idKey}=S.${idKey}`;
    //U匹对修改
    sqlStr = sqlStr + `
    WHEN MATCHED THEN `;
    sqlStr = sqlStr + `
    UPDATE SET `;

    for (let index in model) {
        //修改时不修改创建人和创建时间信息
        sqlStr = sqlStr + `T.${index}=S.${index} ,`;
    }

    sqlStr = sqlStr.trimEnd(" ").trimEnd(",") + " ) ";

    //I匹对新增
    sqlStr = sqlStr + `
    WHEN NOT MATCHED  BY TARGET THEN `;
    sqlStr = sqlStr + " INSERT(";
    for (let index in model) {
        // 过滤Id时并且index等于idkey时
        if (!(idFilter && index === idKey)) {
            sqlStr = sqlStr + `${model[index]},`;
        }

    }
    sqlStr = sqlStr.trimEnd(" ").trimEnd(",") + " ) ";
    sqlStr = sqlStr + " VALUES(";
    for (let index in model) {
        // 过滤Id时并且index等于idkey时
        if (!(idFilter && index === idKey)) {
            sqlStr = sqlStr + `${index},`;
        }
    }
    sqlStr = sqlStr.trimEnd(" ").trimEnd(",") + " ) ";
    sqlStr = sqlStr + `;`;
    let connection = await getConnection();
    let request = connection.request();
    let result = await request.query(sqlStr);
    await mssql.close(); // close
    return result;
};



/**
 * @method 实体查询
 * @param {String} tableName 目标表
 * @param {String} idKey id名
 * @param {Object} uniquenessModel 唯一标准
 * @return {Object} 执行结果
 */
let uniquenessModel = async function (tableName, idKey, uniquenessModel) {
    let res = {
        status: "ok",
        message: "查询成功",
        error: "",
        model: {},
        modelList: []
    };
    try {
        let sqlStr = `select * from ${tableName} where 1=1 `;
        for (let index in uniquenessModel) {
            if(index===idKey){
                sqlStr =" AND "+ sqlStr + index + "<>@" + index + ",";
            }else{
                sqlStr =" AND "+ sqlStr + index + "=@" + index + ",";
            }
            request.input(index, uniquenessModel[index]);
        }
        sqlStr = sqlStr.trimEnd(" ").trimEnd(",") + " ";
        let result = await query(sqlStr, paramObj);
        if (Array.isArray(result.recordset)) {
            if (result.recordset.length === 1) {
                res["model"] = result.recordset[0];
            } else {
                res["message"] = "存在多个实体";
                res["modelList"] = result.recordset;
            }

        } else {
            res["message"] = "不存在实体";
        }
    } catch (error) {
        res["status"] = "error";
        res["error"] = error;
        res["message"] = "查询失败";
    } finally {
        await mssql.close(); // close
        return res;
    }
};

/**
 * @method queryPage列表分页查询
 * @param {String} sqlStr 执行sql语句
 * @param {Object} paramObj 执行input参数
 * @param {sortField} sortField 排序字段
 * @param {Object} pageNum 当前页码
 * @param {String} pageSize 页码大小
 * @return {Object} 执行结果
 */
let mergeObjs = async function (targetTable, objs, whereStr, uFields, iFields, isDelete) {
    let sqlStr = "";
    sqlStr = sqlStr + `
    ( `
    objs.forEach(mergeObj => {
        sqlStr = sqlStr + " " + `
        (SELECT
         `;
        for (let index in mergeObj) {
            sqlStr = sqlStr + ` '${mergeObj[index]}' AS  ${index},`;
        }
        sqlStr = sqlStr.trimEnd(" ").trimEnd(",") + ` )
        UNION ALL  `;
        rowIndex++;
    });
    sqlStr = sqlStr.trimEnd(" ").trimEnd("UNION ALL") + ` `;

    let result = mergeSql(targetTable, sqlStr, whereStr, uFields, iFields, isDelete);
    return result;
};

/**
 * @method queryPage列表分页查询
 * @param {String} targetTable 执行sql语句
 * @param {Object} sourceTable 执行input参数
 * @param {sortField} whereStr 排序字段
 * @param {Object} uFields 当前页码
 * @param {String} iFields 页码大小
 * @param {String} isDelete 页码大小
 * @return {Object} 执行结果
 */
let mergeSql = async function (targetTable, sourceTable, whereStr, uFields, iFields, isDelete) {
    let sqlStr = "";
    sqlStr = sqlStr + " " + `
    MERGE INTO ${targetTable}  AS T
    USING  ${sourceTable}  AS S 
    ON 2>1 ${(typeof whereStr==='string'&&whereStr!=="")?whereStr:""}`
    //U匹对修改
    if (Array.isArray(uFields) && uFields !== []) {
        sqlStr = sqlStr + " " + `
    WHEN MATCHED THEN`
        sqlStr = sqlStr + " " + `
    UPDATE SET `
        for (let field of uFields) {
            sqlStr = sqlStr + `T.${field}=S.${field} ,`;
        }
        sqlStr = sqlStr.trimEnd(" ").trimEnd(",") + " ) ";
    }
    //I匹对新增
    if (Array.isArray(iFields) && iFields !== []) {
        sqlStr = sqlStr + " " + `
    WHEN NOT MATCHED  BY TARGET THEN `;
        sqlStr = sqlStr + " " + `
    INSERT(${ iFields.join(",")  })`;
        sqlStr = sqlStr + `
    VALUES(`;
        for (let field of iFields) {
            sqlStr = sqlStr + `S.${field} ,`;
        }
        sqlStr = sqlStr.trimEnd(" ").trimEnd(",") + " ) ";
    }
    if (isDelete) {
        sqlStr = sqlStr + " " + `
    WHEN NOT MATCHED BY SOURCE THEN
    Delete`;
    }
    sqlStr = sqlStr + " " + `
    OUTPUT $ACTION AS optName;`;
    return sqlStr; // close
};

//批量合并数据
let merge = async function (sqlStr, paramObj) {
    let res = {
        status: "error",
        message: "批量合并数据失败",
        error: "",
        count: 0
    };
    try {
        let result = await query(sqlStr, paramObj);
        //rowsAffected:
        if (result.rowsAffected.length === 1) {
            res["status"] = "ok";
            res["message"] = "保存成功";
            res["count"] = result.rowsAffected[0];
            res["error"] = "";
        } else {
            res["status"] = "error";
            res["message"] = "保存失败";
        }
    } catch (error) {
        res["status"] = "error";
        res["message"] = "保存失败，请联系管理员";
        res["error"] = error;
    } finally {
        await mssql.close(); // close
        return res;
    }
};

/**
 * @method queryPage列表分页查询
 * @param {String} sqlStr 执行sql语句
 * @param {Object} paramObj 执行input参数
 * @param {sortField} sortField 排序字段
 * @param {Object} pageNum 当前页码
 * @param {String} pageSize 页码大小
 * @return {Object} 执行结果
 */
let mergeTemp = async function (prevSqlList, nextSqlList, targetTable, objs, columns, whereStr, uFields, iFields, isDelete) {
    //已存在表时不会重新设计表结构
    //方法二：
    //select * into #TEMP from 你的表;
    //select * into ##TEMP from 你的表;
    //清空临时表
    //truncate table #TEMP --清空临时表的所有数据和约束
    //删除临时表
    //DROP table #TEMP
    let sourceTable = "##Temp" + targetTable;
    //只复制表结构不复制数据
    prevSqlList.push({
        sqlStr: `select * into ${sourceTable} from ${targetTable} where 1>2;`
    });
    //清空临时表的所有数据和约束
    prevSqlList.push({
        sqlStr: `  truncate table  ${sourceTable};`
    });
    let table = new mssql.Table(sourceTable);
    table.create = true;
    for (let column of columns) {
        table.columns.add(...Object.values(column));
    }
    for (let obj of objs) {
        //传递数组记得加上 解析 ...
        table.rows.add(...Object.values(obj));
    }
    //合并语句
    let mergeSqlStr = await mergeSql(targetTable, sourceTable, whereStr, uFields, iFields, isDelete);
    nextSqlList.push({
        sqlStr: mergeSqlStr
    });

    //删除临时表
    nextSqlList.push({
        sqlStr: `DROP table ${sourceTable};`
    });
    await mssql.close(); // close
    const connpool = await new mssql.ConnectionPool(connConfig);
    const pool = await connpool.connect();
    const transaction = await new mssql.Transaction(pool)
    await transaction.begin()
    let rolledBack = false
    transaction.on('rollback', aborted => {
        rolledBack = true
    })
    const request = await new mssql.Request(transaction);
    let res = {};
    try {
        let prevSqlStr = "";
        //of 跟 in 有差别
        for (let sqlObj of prevSqlList) {
            prevSqlStr = prevSqlStr + sqlObj["sqlStr"];
            if (!prevSqlStr.endsWith(";")) {
                prevSqlStr = prevSqlStr + ";";
            }
            if (sqlObj["paramObj"]) {
                for (let index in paramObj) {
                    request.input(index, sqlObj["paramObj"][index]);
                }
            }
        }
        await request.query(prevSqlStr);

        await request.bulk(table);

        let nextSqlStr = "";
        //of 跟 in 有差别
        for (let sqlObj of nextSqlList) {
            nextSqlStr = nextSqlStr + sqlObj["sqlStr"];
            if (!nextSqlStr.endsWith(";")) {
                nextSqlStr = nextSqlStr + ";";
            }
            if (sqlObj["paramObj"]) {
                for (let index in paramObj) {
                    request.input(index, sqlObj["paramObj"][index]);
                }
            }
        }
        res = await request.query(nextSqlStr);
        res["status"] = "ok";
        res["message"] = "操作成功";
        res["error"] = "";
        await transaction.commit()
    } catch (error) {
        res["status"] = "error";
        res["message"] = "操作失败";
        res["error"] = error;
        await transaction.rollback()
    } finally {
        await mssql.close();
        return res;
    }
};

exports.query = query;
// 基础
exports.queryList = queryList;
exports.queryPage = queryPage;
exports.queryModel = queryModel;
exports.deleteModel = deleteModel;
exports.updateModel = updateModel;
exports.insertModel = insertModel;
exports.saveModel = saveModel;
exports.uniquenessModel = uniquenessModel;
exports.deleteList = deleteList;

exports.execute = execute;
exports.transaction = transaction;
exports.mergeObjs = mergeObjs;
exports.mergeSql = mergeSql;
exports.merge = merge;
exports.mergeTemp = mergeTemp;