package com.maxtech.data.tools;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import com.maxtech.data.db2.CallDb;
import com.maxtech.data.oracle.ConnectJdbc;

public class Main {

	/**
	 * 表存在时 是否覆盖
	 */
	public static boolean cover = true;
	
	public static void main(String[] args) throws FileNotFoundException, SQLException, ClassNotFoundException, IOException, InstantiationException, IllegalAccessException {
		System.out.println("Oracle to DB2 数据迁移开始...");
		String[] tables = {"TS_JYMK_INFO","TB_SYSTEM_MK_SP_SX12500","TS_MK_SP_FX12500",
				"TS_MK_INFO12500","TS_SP_PP_INFO","TS_MK_PIC12500","ts_mk_sp_sx12500","TS_ASSET_INFO","TS_MK_SP_ML"};
		 
		for (int i = 0; i < tables.length; i++) {
			String tableName = tables[i];
			System.out.println("开始对表 "+tableName+"进行数据迁移");
			System.out.println();
			invokeMove(tableName);
			System.out.println("-----"+tableName+"--move----done!--------------------------");
		}
		// 关闭连接
		ConnectJdbc.closeConn();
		CallDb.closeConn();
	}
	
	/**
	 * 执行 数据 移动  ora->db2
	 * @param tableName
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 * @throws SQLException 
	 * @throws FileNotFoundException 
	 * @throws IllegalAccessException 
	 * @throws InstantiationException 
	 */
	public static void invokeMove(String tableName) throws FileNotFoundException, SQLException, ClassNotFoundException, IOException, InstantiationException, IllegalAccessException{
		if(tableName == null || "".equals(tableName)){
			return;
		}
		
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
				createTableSql.append(meta.getColumnName(i))
					.append(" ").append(FiledTypeConvert
							.converOraToDB2(meta.getColumnTypeName(i),meta.getPrecision(i),meta.getScale(i))).append(",");
				
			}else{
				createTableSql.append(meta.getColumnName(i))
					.append(" ").append(FiledTypeConvert
							.converOraToDB2(meta.getColumnTypeName(i),meta.getPrecision(i),meta.getScale(i)));
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
//    	sql.append(" where TS_MK_ID = '00000000ED389111565141EDE043AE1410AC09FD'");
    	System.out.println("数据查询语句");
    	System.out.println(sql.toString());
    	System.out.println();
    	System.out.println("开始查询数据");
    	
    	List<Map<String,Object>> insertSqls = ConnectJdbc.getSqlStatements(sql.toString(), meta, tableName);
		
		// 向DB2 中插入数据 OK
		if(insertSqls == null || insertSqls.size() == 0){
			System.out.println("表"+tableName+"没有查到数据");
			return;
		}
		
		System.out.println("添加数据（"+insertSqls.size()+"）语句： "+tableName);
		System.out.println("执行insert 语句:");
		for (int i = 0; i < insertSqls.size(); i++) {
			System.out.println((i+1)+"/"+insertSqls.size());
			
			String insertSql = (String) insertSqls.get(i).get("sql");
			List<Object> params = (List<Object>) insertSqls.get(i).get("params");
			
//			System.out.println(insertSql);
//			StringBuffer sb = new StringBuffer();
//			for (int j = 0; j < params.size(); j++) {
//				Object p = params.get(j);
//				if(p != null){
//					sb.append(p.toString()).append("\r\n");
//				}else{
//					sb.append("null").append("\r\n");
//				}
//			}
//			System.out.println(sb.toString());
			
			if(params != null && params.size() >0){
				CallDb.excuteSql(insertSql,params);
			}else{
				CallDb.excuteSql(insertSql);
			}
		}
		CallDb.conn.commit();		
	}
}
