package com.maxtech.data.db2;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 * DB2 访问类
 * @author root
 *
 */
public class CallDb {
	public static String url = "jdbc:db2://172.16.50.200:50000/ctwap";
    /**
     * 用户 密码
     */
    public static String DBUSER="db2inst1";
    public static String password="passw0rd";
    
    static Connection conn = null;//表示数据库连接
    public static Connection getConnection() throws ClassNotFoundException, SQLException, FileNotFoundException, IOException, InstantiationException, IllegalAccessException{
    	if(conn == null){
    		Class.forName("com.ibm.db2.jcc.DB2Driver").newInstance();
            Properties properties = new Properties();
    		properties.load(new FileInputStream("config.properties"));
    		
    		url = properties.getProperty("db2_url", url);
    		DBUSER = properties.getProperty("db2_username", DBUSER);
    		password = properties.getProperty("db2_password", password);
    		
    		conn =DriverManager.getConnection(url,DBUSER,password); //连接数据库
    	}
    	return conn;
    }
    
	public static void main(String args[]) throws FileNotFoundException, ClassNotFoundException, SQLException, IOException, InstantiationException, IllegalAccessException{
//		insertData("");
//		String sql = "CREATE TABLE TS_MK_INFO12500("
//				+ "TS_MK_ID CHAR,"
//				+ "TS_MK_BH VARCHAR(200),"
//				+ "TS_MK_TITLE VARCHAR(200),"
//				+ "TS_MK_CUSER CHAR,"
//				+ "TS_MK_C_DATE DATE,"
//				+ "TS_MK_DATE DATE,"
//				+ "TS_MK_ENDDATE DATE,"
//				+ "TS_MK_ZT INTEGER,"
//				+ "TS_MK_SX INTEGER,"
//				+ "TS_MK_DEL INTEGER,"
//				+ "TS_MK_LY INTEGER,"
//				+ "TS_MK_TYPE_ID CHAR,"
//				+ "TS_TDY15 VARCHAR(500))";
//		String dropSql = "drop table TS_MK_INFO12500";
//		excuteSql(dropSql);
//		excuteSql(sql);
		
		queryData("");
	}
	
	
	public static void excuteSql(String sql) throws FileNotFoundException, ClassNotFoundException, SQLException, IOException, InstantiationException, IllegalAccessException{
		conn = getConnection();
		Statement stmt = null;
		try{
			stmt=conn.createStatement();
			stmt.execute(sql);
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	
	public static void 	queryData(String sql) throws FileNotFoundException, ClassNotFoundException, SQLException, IOException, InstantiationException, IllegalAccessException{
		conn = getConnection();
		Statement stmt=null;
		ResultSet rs=null;
		try{
			stmt=conn.createStatement();
			rs=stmt.executeQuery("select * from TS_MK_INFO12500");
			while(rs.next()){
				System.out.println(rs.getString("TS_MK_TITLE"));
			}
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	public static int insertData(String sql) throws FileNotFoundException, ClassNotFoundException, SQLException, IOException, InstantiationException, IllegalAccessException{
		conn = getConnection();
		Statement stmt=null;
		int ret = 0;
		try{
			stmt=conn.createStatement();
			int id = (int)(Math.random()*1000000);
			sql = "INSERT INTO T_HANMANYI_TEST (id,code,name) VALUES ('"+id+"','03','hanmanyi00"+id+"')"; 
			System.out.println("sql:"+sql);
			ret = stmt.executeUpdate(sql);
		}catch(Exception e){
			e.printStackTrace();
		}
		return ret;
	}
	public static void closeConn() throws SQLException{
		if(conn != null){
			conn.close();//关闭数据库
		}
    }
	
	/**
	 * 判断表是否存在
	 * @param tableName
	 * @return
	 * @throws FileNotFoundException
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 * @throws IOException
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 */
	public static boolean isTableExists(String tableName) throws FileNotFoundException, ClassNotFoundException, SQLException, IOException, InstantiationException, IllegalAccessException{
		boolean exist = false;
		String sql = "select 1 from sysibm.systables where TID <> 0 and name = '"+tableName+"'";
		conn = getConnection();
		Statement stmt=null;
		ResultSet rs=null;
		try{
			stmt=conn.createStatement();
			rs=stmt.executeQuery(sql);
			if(rs != null && rs.next()){
				exist = true;
			}
		}catch(Exception e){
		}
		return exist;
	}
}
