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
     * jdbc:oracle:thin:@localhost:1521:ORCL localhost ��ip��ַ��
     */
    public static String url = "jdbc:oracle:thin:@172.16.50.67:1521:ORCL";
    /**
     * �û� ����
     */
    public static String DBUSER="e_channel";
    public static String password="e_channel_test";
    
//    public static int maxsize = Integer.MAX_VALUE;
    public static int maxsize = 10;
    public static Connection conn = null;//��ʾ���ݿ�����
    public static Connection getConnection() throws ClassNotFoundException, SQLException, FileNotFoundException, IOException{
    	if(conn == null){
            Class.forName(drive);//ʹ��class�������س���
    	
            Properties properties = new Properties();
    		properties.load(new FileInputStream("config.properties"));
    		
    		url = properties.getProperty("url", url);
    		DBUSER = properties.getProperty("username", DBUSER);
    		password = properties.getProperty("password", password);
    		
    		conn =DriverManager.getConnection(url,DBUSER,password); //�������ݿ�
    	}
    	return conn;
    }
    
    public static void main(String[] args) throws Exception{
    	conn = getConnection();
    	String tableName = "TS_MK_INFO12500";
    	ResultSetMetaData tableMeta = redTable(tableName);
    	
    	if(tableMeta == null || tableMeta.getColumnCount() == 0){
    		System.out.println("δ��ȡ�� �� "+tableName+"�ı��ṹ");
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
        	conn.close();//�ر����ݿ�
        }
    }
    
    public static Integer getDataCount(String tableName) throws FileNotFoundException, ClassNotFoundException, SQLException, IOException{
    	Integer count = 0;
    	conn = getConnection();
    	Statement stmt= null;//��ʾ���ݿ�ĸ���
    	ResultSet result = null;//��ѯ���ݿ�
    	String sql = "select count(*) from "+tableName;
    	stmt = conn.createStatement();
    	//ִ��SQL�������ѯ���ݿ�
        result =stmt.executeQuery(sql);
        while(result.next()){//�ж���û����һ��
        	count = Integer.parseInt(result.getString(1));
        }
        return count;
    }
    
    public static List<String> getSqlStatements(String sql, ResultSetMetaData tableMeta, String tableName) throws SQLException, FileNotFoundException, ClassNotFoundException, IOException{
    	List<String> sqlList =new ArrayList<String>();
    	conn = getConnection();
    	Statement stmt= null;//��ʾ���ݿ�ĸ���
    	ResultSet result = null;//��ѯ���ݿ�    
        //Statement�ӿ�Ҫͨ��connection�ӿ�������ʵ��������
        stmt = conn.createStatement();
        //ִ��SQL�������ѯ���ݿ�
        result =stmt.executeQuery(sql);
        int rowCount = getDataCount(tableName); //�õ���ǰ�кţ�Ҳ���Ǽ�¼��   
        System.out.println("���ݹ�  "+rowCount+"  ������ʼƴװinsert sql���");
        
        int i = 0;
        while(result.next() && i < maxsize){//�ж���û����һ��
        	i++;
        	System.out.println("ƴ��insert ���:"+i+"/"+rowCount);
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
					System.out.println("�µ��ֶ����ͣ����룩:"+tableMeta.getColumnType(j));
					System.out.println("��������:"+tableMeta.getColumnTypeName(j));
					break;
				}
			}
//        	after.append(";");//java �����ִ�� ����Ҫ��;��
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
    	Statement stmt= null;//��ʾ���ݿ�ĸ���
        ResultSet result = null;//��ѯ���ݿ�    
        //Statement�ӿ�Ҫͨ��connection�ӿ�������ʵ��������
		stmt = conn.createStatement();
	
        //ִ��SQL�������ѯ���ݿ�
        result =stmt.executeQuery(sql);
        ResultSetMetaData meta = result.getMetaData();
        System.out.println("�� " +tableName+" ���� "+meta.getColumnCount()+" ��");
        System.out.println();
        return meta;
    }
    
    @Test
    public void testOracleIsReachable(){
    	System.out.println("�������ݿ������Ƿ�����");
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