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
	 * �����ʱ �Ƿ񸲸�
	 */
	public static boolean cover = true;
	
	public static void main(String[] args) throws FileNotFoundException, SQLException, ClassNotFoundException, IOException, InstantiationException, IllegalAccessException {
		System.out.println("Oracle to DB2 ����Ǩ�ƿ�ʼ...");
		
		String tableName = "TS_MK_INFO12500";
		
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
				createTableSql.append(meta.getColumnName(i)).append(" ").append(FiledTypeConvert.converOraToDB2(meta.getColumnTypeName(i),meta.getPrecision(i))).append(",");
				
			}else{
				createTableSql.append(meta.getColumnName(i)).append(" ").append(FiledTypeConvert.converOraToDB2(meta.getColumnTypeName(i),meta.getPrecision(i)));
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
    	
    	System.out.println("���ݲ�ѯ���");
    	System.out.println(sql.toString());
    	System.out.println();
    	System.out.println("��ʼ��ѯ����");
    	
		List<String> insertSqls = ConnectJdbc.getSqlStatements(sql.toString(), meta, tableName);
		
		// ��DB2 �в������� OK
		if(insertSqls == null || insertSqls.size() == 0){
			System.out.println("��"+tableName+"û�в鵽����");
			return;
		}
		
		System.out.println("������ݣ�"+insertSqls.size()+"����䣺 "+tableName);
		for (int i = 0; i < insertSqls.size(); i++) {
			System.out.println("ִ��insert ���:"+(i+1)+"/"+insertSqls.size());
			CallDb.excuteSql(insertSqls.get(i));
		}
		// �ر�����
		ConnectJdbc.closeConn();
		CallDb.closeConn();
	}
}
