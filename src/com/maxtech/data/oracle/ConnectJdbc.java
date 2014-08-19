package com.maxtech.data.oracle;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
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
    
//    public static int maxsize = Integer.MAX_VALUE;
    public static int maxsize = 10;
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
    	List<String> list = getSqlStatements(sql.toString(),tableMeta,tableName);
    	
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
    
    public static List<String> getSqlStatements(String sql, ResultSetMetaData tableMeta, String tableName) throws SQLException, FileNotFoundException, ClassNotFoundException, IOException{
    	List<String> sqlList =new ArrayList<String>();
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
        while(result.next() && i < maxsize){//判断有没有下一行
        	i++;
        	System.out.println("拼接insert 语句:"+i+"/"+rowCount);
        	StringBuffer record = new StringBuffer();
        	StringBuffer after = new StringBuffer();
        	
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
					Integer num= result.getInt(j);
					if(j<tableMeta.getColumnCount()){
						after.append(num).append(",");
	        		}else{
	        			after.append(num).append(")");
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
					Date d = result.getDate(j);
					if(j<tableMeta.getColumnCount()){
						after.append(d == null?"null":"'"+d+"'").append(",");
	        		}else{
	        			after.append(d == null?"null":"'"+d+"'").append(")");
	        		}
					break;
				case 2005:
					//CLOB
					Clob cl = result.getClob(j);
					if(j<tableMeta.getColumnCount()){
						after.append("null,");
//						after.append(cl == null?null:cl.getAsciiStream()).append(",");
	        		}else{
	        			after.append("null)");
//	        			after.append(cl == null?null:cl.getAsciiStream()).append(")");
	        		}
					break;
				default:
					System.out.println("新的字段类型（编码）:"+tableMeta.getColumnType(j));
					System.out.println("类型名称:"+tableMeta.getColumnTypeName(j));
					break;
				}
			}
//        	after.append(";");//java 多语句执行 不需要“;”
        	record.append(after);
        	sqlList.add(record.toString());
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