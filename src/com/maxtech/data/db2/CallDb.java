package com.maxtech.data.db2;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.maxtech.data.manual.GoodsMove;

/**
 * DB2 访问类
 * @author root
 *
 */
public class CallDb {
	static Logger logger = Logger.getLogger(CallDb.class.getName());  
	static{
		PropertyConfigurator.configure ("src//log4j.properties");  
	}
	public static String url = "jdbc:db2://172.16.50.200:50000/ctwap";
    /**
     * 用户 密码
     */
    public static String DBUSER="db2inst1";
    public static String password="passw0rd";
    
    public static Connection conn = null;//表示数据库连接
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
			logger.error(sql);
			logger.error(e.getMessage());
			e.printStackTrace();
		}finally{
			stmt.close();
		}
	}
	
	public static void excuteSql(String sql,List<Object> params) throws FileNotFoundException, ClassNotFoundException, SQLException, IOException, InstantiationException, IllegalAccessException{
		conn = getConnection();
		PreparedStatement ps = null;
		try{
			ps = conn.prepareStatement(sql) ; 
			// 赋值
			if(params != null && params.size() > 0){
				for (int i = 0; i < params.size(); i++) {
					ps.setObject(i+1, params.get(i));
				}
			}
            ps.executeUpdate() ;  
		}catch(Exception e){
			logger.error(sql);
			StringBuffer sb = new StringBuffer();
			for (int j = 0; j < params.size(); j++) {
				Object p = params.get(j);
				if(p != null){
					sb.append(p.toString()).append("\r\n");
				}else{
					sb.append("null").append("\r\n");
				}
			}
			logger.error(sb.toString());
			logger.error(e.getMessage());
			e.printStackTrace();
		}finally{
			ps.close();
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
				Clob cl = rs.getClob("TS_COUPON_JS");
				if(cl != null && cl.length() >0){
					String content = cl.getSubString((long)1,(int)cl.length());
					System.out.println(content);
				}
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
	public static ResultSetMetaData redTable(String tableName){
		try {
			conn = CallDb.getConnection();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		}
    	String sql = "select * from "+ tableName +"  FETCH FIRST 1 ROWS ONLY ";
    	Statement stmt= null;//表示数据库的更新
        ResultSet result = null;//查询数据库    
        //Statement接口要通过connection接口来进行实例化操作
		try {
			stmt = conn.createStatement();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	
        //执行SQL语句来查询数据库
		ResultSetMetaData meta = null;
        try {
			result =stmt.executeQuery(sql);
			meta = result.getMetaData();
			System.out.println("表 " +tableName+" 共有 "+meta.getColumnCount()+" 列");
		} catch (SQLException e) {
			e.printStackTrace();
		}
        return meta;
    }

}
