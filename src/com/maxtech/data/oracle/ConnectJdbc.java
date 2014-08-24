package com.maxtech.data.oracle;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import junit.framework.Assert;

import org.junit.Test;

 
public class ConnectJdbc {
    public static final String drive = "oracle.jdbc.driver.OracleDriver";
    /**
     * jdbc:oracle:thin:@localhost:1521:ORCL localhost 是ip地址。
     */
    public static String url = "jdbc:oracle:thin:@172.16.50.67:1521:ORCL";
    /**
     * 用户 密码
     */
    public static String DBUSER="e_channel";
    public static String password="e_channel_test";
    
    public static int maxsize = Integer.MAX_VALUE;
//    public static int maxsize = 10;
    public static Connection conn = null;//表示数据库连接
    public static Connection getConnection() throws ClassNotFoundException, SQLException, FileNotFoundException, IOException{
    	if(conn == null){
            Class.forName(drive);//使用class类来加载程序
    	
            Properties properties = new Properties();
    		properties.load(new FileInputStream("config.properties"));
    		
    		url = properties.getProperty("url", url);
    		DBUSER = properties.getProperty("username", DBUSER);
    		password = properties.getProperty("password", password);
    		
    		conn =DriverManager.getConnection(url,DBUSER,password); //连接数据库
    	}
    	return conn;
    }
    
    public static void main(String[] args) throws Exception{
    	conn = getConnection();
    	String tableName = "TS_MK_INFO12500";
    	ResultSetMetaData tableMeta = redTable(tableName);
    	
    	if(tableMeta == null || tableMeta.getColumnCount() == 0){
    		System.out.println("未获取到 表 "+tableName+"的表结构");
    		return;
    	}
    	StringBuffer sql = new StringBuffer();
    	sql.append("select ");
    	for (int i = 1; i <= tableMeta.getColumnCount(); i++) {
			if(i <  tableMeta.getColumnCount()){
				sql.append(tableMeta.getColumnName(i)).append(", ");
			}else{
				sql.append(tableMeta.getColumnName(i)).append(" from " +tableName);
			}
		}
    	
    	System.out.println(sql.toString());
    	List<Map<String,Object>> list = getSqlStatements(sql.toString(),tableMeta,tableName);
    	
    	for (int i = 0; i < list.size(); i++) {
			System.out.println(list.get(i));
		}
    	closeConn();
    }
    
    public static void closeConn() throws SQLException{
        if(conn != null){
        	conn.close();//关闭数据库
        }
    }
    
    public static Integer getDataCount(String tableName) throws FileNotFoundException, ClassNotFoundException, SQLException, IOException{
    	Integer count = 0;
    	conn = getConnection();
    	Statement stmt= null;//表示数据库的更新
    	ResultSet result = null;//查询数据库
    	String sql = "select count(*) from "+tableName;
    	stmt = conn.createStatement();
    	//执行SQL语句来查询数据库
        result =stmt.executeQuery(sql);
        while(result.next()){//判断有没有下一行
        	count = Integer.parseInt(result.getString(1));
        }
        return count;
    }
    
    public static List<Map<String,Object>> getSqlStatements(String sql, ResultSetMetaData tableMeta, String tableName) throws SQLException, FileNotFoundException, ClassNotFoundException, IOException{
//    	List<String> sqlList =new ArrayList<String>();
    	List<Map<String,Object>> sqlList = new ArrayList<Map<String,Object>>();
    	conn = getConnection();
    	Statement stmt= null;//表示数据库的更新
    	ResultSet result = null;//查询数据库    
        //Statement接口要通过connection接口来进行实例化操作
        stmt = conn.createStatement();
        //执行SQL语句来查询数据库
        result =stmt.executeQuery(sql);
        int rowCount = getDataCount(tableName); //得到当前行号，也就是记录数   
        System.out.println("数据共  "+rowCount+"  条，开始拼装insert sql语句");
        
        int i = 0;
        System.out.println("拼接insert 语句:");
        while(result.next() && i < maxsize){//判断有没有下一行
        	Map<String,Object> map = new HashMap<String,Object>(); 
        	i++;
        	System.out.println(i+"/"+rowCount);
        	StringBuffer record = new StringBuffer();
        	StringBuffer after = new StringBuffer();
        	List<Object> params = new ArrayList<Object>();
        	
        	record.append("INSERT INTO db2inst1.").append(tableName).append(" (");
        	after.append(") VALUES (");
        	for (int j = 1; j <= tableMeta.getColumnCount(); j++) {
        		if(j<tableMeta.getColumnCount()){
        			record.append(tableMeta.getColumnName(j)).append(",");
        		}else{
        			record.append(tableMeta.getColumnName(j));
        		}
        		switch (tableMeta.getColumnType(j)) {
				case 1:
					//CHAR
					String c= result.getString(j);
					if(j<tableMeta.getColumnCount()){
						after.append(c == null?null:"'"+c+"'").append(",");
	        		}else{
	        			after.append(c == null?null:"'"+c+"'").append(")");
	        		}
					break;
				case 2:
					// NUMBER
					// (38,0)-Integer;(n,m)-NUMBER(n,m);
					// (0,-127)-NUMBER 未指定长度精度
					// ora-db2 类型对应 http://www.cnblogs.com/newstar/archive/2010/04/13/1711486.html
					// 
					int p = tableMeta.getPrecision(j);
					int s = tableMeta.getScale(j);
					
					if(p != 0 && s!= 0){
						BigDecimal num= result.getBigDecimal(j);
						if(j<tableMeta.getColumnCount()){
							after.append(num).append(",");
						}else{
							after.append(num).append(")");
						}
					}else{
						Integer num= result.getInt(j);
						if(j<tableMeta.getColumnCount()){
							after.append(num).append(",");
						}else{
							after.append(num).append(")");
						}
					}
					
					break;
				case 12:
					//VARCHAR2
					String vc2 = result.getString(j);
					if(j<tableMeta.getColumnCount()){
						after.append(vc2 == null?"null":"'"+vc2+"'").append(",");
	        		}else{
	        			after.append(vc2 == null?"null":"'"+vc2+"'").append(")");
	        		}
					break;
				case 91:
					//DATE
//					Date d = result.getDate(j);
					Timestamp d = result.getTimestamp(j);
					if(j<tableMeta.getColumnCount()){
						after.append(d == null?"null":"'"+d.toLocaleString()+"'").append(",");
	        		}else{
	        			after.append(d == null?"null":"'"+d.toLocaleString()+"'").append(")");
	        		}
					break;
				case 93:
					//TIMESTAMP
					Timestamp ta = result.getTimestamp(j);
					if(j<tableMeta.getColumnCount()){
						after.append(ta == null?"null":"'"+ta.toLocaleString()+"'").append(",");
					}else{
						after.append(ta == null?"null":"'"+ta.toLocaleString()+"'").append(")");
					}
					break;
				case 2005:
					//CLOB
					Clob cl = result.getClob(j);
					String content = null;
//					if(cl != null && cl.length() > 0){
						// 方法一 最快 但是有乱码问题
//						InputStream input = cl.getAsciiStream();
//						int len = (int)cl.length();
//						byte by[] = new byte[len];
//						while(-1 != (i = input.read(by, 0, by.length))){
//							input.read(by, 0, i);
//						}
//						content = new String(by, "utf-8");
						
						// 方法二 简洁 无乱码
//						content = cl.getSubString((long)1,(int)cl.length());
						
						// 方法三 无乱码  但是 效率太低
//						Reader is = cl.getCharacterStream();// 得到流   
//			            BufferedReader br = new BufferedReader(is);   
//			            String s = br.readLine();   
//			            StringBuffer sb = new StringBuffer();   
//			            while (s != null) {   
//			                sb.append(s);   
//			                sb.append("/n");   
//			                s = br.readLine();   
//			            }   
//			            content = sb.toString().trim();   
//					}
					if(j<tableMeta.getColumnCount()){
						after.append("?").append(",");
	        		}else{
	        			after.append("?").append(")");
	        		}
//					System.out.println(content);
//					params.add(content);
					params.add(cl);
					break;
				default:
					System.out.println("新的字段类型（编码）:"+tableMeta.getColumnType(j));
					System.out.println("类型名称:"+tableMeta.getColumnTypeName(j));
					break;
				}
			}
//        	after.append(";");//java 多语句执行 不需要“;”
        	record.append(after);
        	map.put("sql", record.toString());
        	map.put("params", params);
        	sqlList.add(map);
        }
        stmt.close();
        result.close();
        return sqlList;
    }
    
    public static ResultSetMetaData redTable(String tableName) throws SQLException, FileNotFoundException, ClassNotFoundException, IOException{
    	conn = getConnection();
    	String sql = "select * from "+ tableName +"  where rownum<=1 ";
    	Statement stmt= null;//表示数据库的更新
        ResultSet result = null;//查询数据库    
        //Statement接口要通过connection接口来进行实例化操作
		stmt = conn.createStatement();
	
        //执行SQL语句来查询数据库
        result =stmt.executeQuery(sql);
        ResultSetMetaData meta = result.getMetaData();
        System.out.println("表 " +tableName+" 共有 "+meta.getColumnCount()+" 列");
        System.out.println();
        return meta;
    }
    
    public static ResultSet executeQuery(String sql){
    	try {
			conn = getConnection();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
    	Statement stmt= null;//表示数据库的更新
        ResultSet result = null;//查询数据库    
        //Statement接口要通过connection接口来进行实例化操作
		try {
			stmt = conn.createStatement();
		} catch (SQLException e) {
			e.printStackTrace();
		}
        //执行SQL语句来查询数据库
        try {
			result =stmt.executeQuery(sql);
		} catch (SQLException e) {
			e.printStackTrace();
		}
        return result;
    }
    
    @Test
    public void testOracleIsReachable(){
    	System.out.println("测试数据库连接是否正常");
    	Connection conn = null;
    	try {
    		conn = getConnection();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
    	Assert.assertEquals("connect db failed,please check db link status", conn!= null,true);
    }
}