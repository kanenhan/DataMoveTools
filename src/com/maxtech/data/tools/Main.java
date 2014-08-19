package com.maxtech.data.tools;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;

import com.maxtech.data.db2.CallDb;
import com.maxtech.data.oracle.ConnectJdbc;

public class Main {

	/**
	 * 表存在时 是否覆盖
	 */
	public static boolean cover = true;
	
	public static void main(String[] args) throws FileNotFoundException, SQLException, ClassNotFoundException, IOException, InstantiationException, IllegalAccessException {
		System.out.println("Oracle to DB2 数据迁移开始...");
		
		String tableName = "TS_MK_INFO12500";
		
		// 获取 oracle 表结构
		ResultSetMetaData meta = ConnectJdbc.redTable(tableName);
		if(meta == null || meta.getColumnCount() == 0){
    		System.out.println("未获取到 表 "+tableName+"的表结构!");
    		return;
    	}
		// 根据oracle表结构生成 DB2 表结构 
		StringBuffer createTableSql = new StringBuffer();
		createTableSql.append("CREATE TABLE ").append(tableName).append("(");
		
		for (int i = 1; i <= meta.getColumnCount(); i++) {
			if(i < meta.getColumnCount()){
				createTableSql.append(meta.getColumnName(i)).append(" ").append(FiledTypeConvert.converOraToDB2(meta.getColumnTypeName(i),meta.getPrecision(i))).append(",");
				
			}else{
				createTableSql.append(meta.getColumnName(i)).append(" ").append(FiledTypeConvert.converOraToDB2(meta.getColumnTypeName(i),meta.getPrecision(i)));
			}
			createTableSql.append("\r\n");
		}
		createTableSql.append(")in tablespacehmy");
		
		// TODO 没有该表 创建，否则 repalce该表 
		// DB2 不能像 sql server 或者 oracle 通过简单的额 sql语句实现，需要用到 存储过程，判断存在时 删除后创建，
		// 参考http://www.2cto.com/database/201303/196191.html
		// 暂时 手动 处理
		System.out.println("向DB2执行建表语句:"+tableName);
		System.out.println(createTableSql.toString());
		System.out.println();
		
		// 执行建表语句
		boolean tableExist = CallDb.isTableExists(tableName);
		
		if(cover && tableExist){
			System.out.println("表已经存在，将先执行drop表操作");
			String dropSql = "drop table "+tableName;
			CallDb.excuteSql(dropSql);
		}else if(tableExist && !cover){
			System.out.println("表已经存在，且不允许覆盖，即将退出");
			return;
		}else{
			System.out.println("表不存在,即将创建表");
		}
		System.out.println("建表中...");
		CallDb.excuteSql(createTableSql.toString());
		// 获取插入数据sql
		StringBuffer sql = new StringBuffer();
    	sql.append("select ");
    	for (int i = 1; i <= meta.getColumnCount(); i++) {
			if(i <  meta.getColumnCount()){
				sql.append(meta.getColumnName(i)).append(", ");
			}else{
				sql.append(meta.getColumnName(i)).append(" from " +tableName);
			}
		}
    	
    	System.out.println("数据查询语句");
    	System.out.println(sql.toString());
    	System.out.println();
    	System.out.println("开始查询数据");
    	
		List<String> insertSqls = ConnectJdbc.getSqlStatements(sql.toString(), meta, tableName);
		
		// 向DB2 中插入数据 OK
		if(insertSqls == null || insertSqls.size() == 0){
			System.out.println("表"+tableName+"没有查到数据");
			return;
		}
		
		System.out.println("添加数据（"+insertSqls.size()+"）语句： "+tableName);
		for (int i = 0; i < insertSqls.size(); i++) {
			System.out.println("执行insert 语句:"+(i+1)+"/"+insertSqls.size());
			CallDb.excuteSql(insertSqls.get(i));
		}
		// 关闭连接
		ConnectJdbc.closeConn();
		CallDb.closeConn();
	}
}
