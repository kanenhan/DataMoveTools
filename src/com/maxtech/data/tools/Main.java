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
	 * �����ʱ �Ƿ񸲸�
	 */
	public static boolean cover = true;
	
	public static void main(String[] args) throws FileNotFoundException, SQLException, ClassNotFoundException, IOException, InstantiationException, IllegalAccessException {
		System.out.println("Oracle to DB2 ����Ǩ�ƿ�ʼ...");
		String[] tables = {"TS_JYMK_INFO","TB_SYSTEM_MK_SP_SX12500","TS_MK_SP_FX12500",
				"TS_MK_INFO12500","TS_SP_PP_INFO","TS_MK_PIC12500","ts_mk_sp_sx12500","TS_ASSET_INFO","TS_MK_SP_ML"};
		 
		for (int i = 0; i < tables.length; i++) {
			String tableName = tables[i];
			System.out.println("��ʼ�Ա� "+tableName+"��������Ǩ��");
			System.out.println();
			invokeMove(tableName);
			System.out.println("-----"+tableName+"--move----done!--------------------------");
		}
		// �ر�����
		ConnectJdbc.closeConn();
		CallDb.closeConn();
	}
	
	/**
	 * ִ�� ���� �ƶ�  ora->db2
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
		
		// ��ȡ oracle ��ṹ
		ResultSetMetaData meta = ConnectJdbc.redTable(tableName);
		if(meta == null || meta.getColumnCount() == 0){
    		System.out.println("δ��ȡ�� �� "+tableName+"�ı�ṹ!");
    		return;
    	}
		// ����oracle��ṹ���� DB2 ��ṹ 
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
		
		// TODO û�иñ� ���������� repalce�ñ� 
		// DB2 ������ sql server ���� oracle ͨ���򵥵Ķ� sql���ʵ�֣���Ҫ�õ� �洢���̣��жϴ���ʱ ɾ���󴴽���
		// �ο�http://www.2cto.com/database/201303/196191.html
		// ��ʱ �ֶ� ����
		System.out.println("��DB2ִ�н������:"+tableName);
		System.out.println(createTableSql.toString());
		System.out.println();
		
		// ִ�н������
		boolean tableExist = CallDb.isTableExists(tableName);
		
		if(cover && tableExist){
			System.out.println("���Ѿ����ڣ�����ִ��drop�����");
			String dropSql = "drop table "+tableName;
			CallDb.excuteSql(dropSql);
		}else if(tableExist && !cover){
			System.out.println("���Ѿ����ڣ��Ҳ������ǣ������˳�");
			return;
		}else{
			System.out.println("������,����������");
		}
		System.out.println("������...");
		CallDb.excuteSql(createTableSql.toString());
		// ��ȡ��������sql
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
    	System.out.println("���ݲ�ѯ���");
    	System.out.println(sql.toString());
    	System.out.println();
    	System.out.println("��ʼ��ѯ����");
    	
    	List<Map<String,Object>> insertSqls = ConnectJdbc.getSqlStatements(sql.toString(), meta, tableName);
		
		// ��DB2 �в������� OK
		if(insertSqls == null || insertSqls.size() == 0){
			System.out.println("��"+tableName+"û�в鵽����");
			return;
		}
		
		System.out.println("������ݣ�"+insertSqls.size()+"����䣺 "+tableName);
		System.out.println("ִ��insert ���:");
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
