package com.maxtech.data.manual;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Clob;
import java.sql.Connection;
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

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.maxtech.data.db2.CallDb;
import com.maxtech.data.oracle.ConnectJdbc;

/**
 * ����sql �� oracle������ �ƶ��� db2
 * @author root
 *
 */
public class GoodsMove {
	static Logger logger = Logger.getLogger(GoodsMove.class.getName());  
	static{
		PropertyConfigurator.configure ("src//log4j.properties");  
	}
	public static void main(String[] args) throws FileNotFoundException, SQLException, ClassNotFoundException, IOException, InstantiationException, IllegalAccessException {
		logger.info("Oracle to DB2 ��Ʒ����Ǩ�ƿ�ʼ...");
		// ��ѯ����
		String sql = null;
 		sql = readFile("goodsA.sql");
 		
		// У�� ����� ͬ Ŀ��� �Ƿ�һ��
 		
		// ���ɲ������
 		String tableName = "XCATENTRY";
 		
// 		ResultSetMetaData meta = CallDb.redTable(tableName);
 		Properties properties = new Properties();
		properties.load(new FileInputStream("fieldMappings.properties"));
 		
 		List<Map<String,Object>> insertSqls = getSqlStatements(sql.toString(), tableName,properties);
		
		// ��DB2 �в������� OK
		if(insertSqls == null || insertSqls.size() == 0){
			logger.warn("��"+tableName+"û�в鵽����");
			return;
		}
		
		// ִ�в���
		logger.info("������ݣ�"+insertSqls.size()+"����䣺 "+tableName);
		logger.info("ִ��insert ���:");
		for (int i = 0; i < insertSqls.size(); i++) {
			logger.info((i+1)+"/"+insertSqls.size());
			
			String insertSql = (String) insertSqls.get(i).get("sql");
			List<Object> params = (List<Object>) insertSqls.get(i).get("params");
			
			logger.info(insertSql);
			StringBuffer sb = new StringBuffer();
			for (int j = 0; j < params.size(); j++) {
				Object p = params.get(j);
				if(p != null){
					sb.append(p.toString()).append("\r\n");
				}else{
					sb.append("null").append("\r\n");
				}
			}
//			logger.info(sb.toString());
			
			if(params != null && params.size() >0){
				CallDb.excuteSql(insertSql,params);
			}else{
				CallDb.excuteSql(insertSql);
			}
		}
		CallDb.conn.commit();
		CallDb.closeConn();
	}
	
	
	public static String readFile(String fileName){ 
	    File file = new File(fileName); 
	         BufferedReader reader = null; 
	         StringBuffer sb=new StringBuffer(); 
	        
	         try { 
	              reader = new BufferedReader(new FileReader(file)); 
	              String tempString = null; 
	              //int line = 1; 
	              // һ�ζ���һ�У�ֱ������nullΪ�ļ����� 
	              while ((tempString = reader.readLine()) != null) { 
	                  // ��ʾ�к� 
	              sb.append(tempString); 
	              sb.append("\r\n"); 
	              } 
	              reader.close(); 
	          } catch (IOException e) { 
	              e.printStackTrace(); 
	          } finally { 
	              if (reader != null) { 
	                  try { 
	                      reader.close(); 
	                  } catch (IOException e1) { 
	                  } 
	              } 
	          } 
	         
	         return sb.toString(); 
	    }
	
	public static List<Map<String,Object>> getSqlStatements(String sql, String tableName,Properties properties) throws SQLException, FileNotFoundException, ClassNotFoundException, IOException{
    	List<Map<String,Object>> sqlList = new ArrayList<Map<String,Object>>();
    	Connection conn = ConnectJdbc.getConnection();
    	Statement stmt= null;
    	ResultSet result = null;
        stmt = conn.createStatement();
        //ִ��SQL�������ѯ���ݿ�
//        logger.info("��ѯsql��"+sql);
        result =stmt.executeQuery(sql);
        logger.info("ƴ��insert ���:");
        
        ResultSetMetaData meta = result.getMetaData();// ����� �ṹ
//        int count = meta.getColumnCount();
//        for (int i = 1; i <= count; i++) {
//        	logger.info(meta.getColumnName(i)+"=");
//		}
        
        int index = 1; 
        int id = (int)(Math.random()*10000000);
        
        while(result.next()){
        	logger.info(index + "/ ? ��������");
        	index++;
        	Map<String,Object> map = new HashMap<String,Object>(); 
        	StringBuffer record = new StringBuffer();
        	StringBuffer after = new StringBuffer();
        	List<Object> params = new ArrayList<Object>();
        	
        	record.append("INSERT INTO ").append(tableName).append(" (");
//        	record.append("INSERT INTO db2inst1.").append(tableName).append(" (");
        	
        	// �ǿ��ֶ�
        	record.append("CATENTRY_ID,CATEGORY_AORB,FIELD,TB_SP_SX_ID,TB_ID,");
        	
        	after.append(") VALUES (");
        	
        	after.append((id+index)+",'a','field001','TB_SP_SX_ID001','TB_ID001',");
        	for (int j = 1; j <= meta.getColumnCount(); j++) {
        		String columnName = properties.getProperty(meta.getColumnName(j));
        		
        		if(columnName != null && !"".equals(columnName)){
        			if(j<meta.getColumnCount()){
        				record.append(columnName).append(",");
        			}else{
        				record.append(columnName);
        			}
        		}else{
        			continue;
        		}
        		
        		switch (meta.getColumnType(j)) {
        		case -5:
        			//BIGINT
        			Integer l= result.getInt(j);
        			if(j<meta.getColumnCount()){
        				after.append(l == null?null:"'"+l+"'").append(",");
        			}else{
        				after.append(l == null?null:"'"+l+"'").append(")");
        			}
        			break;
				case 1:
					//CHAR
					String c= result.getString(j);
					if(j<meta.getColumnCount()){
						after.append(c == null?null:"'"+c+"'").append(",");
	        		}else{
	        			after.append(c == null?null:"'"+c+"'").append(")");
	        		}
					break;
				case 2:
					// NUMBER
					// (38,0)-Integer;(n,m)-NUMBER(n,m);
					// (0,-127)-NUMBER δָ�����Ⱦ���
					// ora-db2 ���Ͷ�Ӧ http://www.cnblogs.com/newstar/archive/2010/04/13/1711486.html
					int p = meta.getPrecision(j);
					int s = meta.getScale(j);
					
					if(p != 0 && s!= 0){
						BigDecimal num= result.getBigDecimal(j);
						if(j<meta.getColumnCount()){
							after.append(num).append(",");
						}else{
							after.append(num).append(")");
						}
					}else{
						Integer num= result.getInt(j);
						if(j<meta.getColumnCount()){
							after.append(num).append(",");
						}else{
							after.append(num).append(")");
						}
					}
					
					break;
				case 3:
					// DECIMAL
					BigDecimal num= result.getBigDecimal(j);
					if(j<meta.getColumnCount()){
						after.append(num).append(",");
					}else{
						after.append(num).append(")");
					}
					break;
				case 4:
					// INTEGER
					Integer inte= result.getInt(j);
					if(j<meta.getColumnCount()){
						after.append(inte).append(",");
					}else{
						after.append(inte).append(")");
					}
					break;
				case 12:
					//VARCHAR2
					String vc2 = result.getString(j);
					if(j<meta.getColumnCount()){
						if(vc2 != null){
							after.append("?").append(",");
							params.add(vc2);
						}else{
							if("PICURL".equals(columnName.toUpperCase())){
								after.append("'none'").append(",");
								logger.error("picurl is null,CATENTRY_ID:"+(id+index));
							}else{
								after.append("null").append(",");
							}
						}
	        		}else{
	        			if(vc2 != null){
							after.append("?").append(")");
							params.add(vc2);
						}else{
							if("PICURL".equals(columnName.toUpperCase())){
								after.append("'none'").append(")");
								logger.error("picurl is null,CATENTRY_ID:"+(id+index));
							}else{
								after.append("null").append(")");
							}
						}
	        		}
					
					break;
				case 91:
					//DATE
					Timestamp d = result.getTimestamp(j);
					if(j<meta.getColumnCount()){
						after.append(d == null?"null":"'"+d.toLocaleString()+"'").append(",");
	        		}else{
	        			after.append(d == null?"null":"'"+d.toLocaleString()+"'").append(")");
	        		}
					break;
				case 93:
					//TIMESTAMP
					Timestamp ta = result.getTimestamp(j);
					if(j<meta.getColumnCount()){
						after.append(ta == null?"null":"'"+ta.toLocaleString()+"'").append(",");
					}else{
						after.append(ta == null?"null":"'"+ta.toLocaleString()+"'").append(")");
					}
					break;
				case 2005:
					//CLOB
					Clob cl = result.getClob(j);
					if(j<meta.getColumnCount()){
						after.append("?").append(",");
	        		}else{
	        			after.append("?").append(")");
	        		}
					params.add(cl);
					break;
				default:
					logger.info("�µ��ֶ����ͣ����룩:"+meta.getColumnType(j));
					logger.info("��������:"+meta.getColumnTypeName(j));
					break;
				}
			}
        	record.append(after);
        	map.put("sql", record.toString());
        	map.put("params", params);
        	sqlList.add(map);
        }
        stmt.close();
        result.close();
        return sqlList;
    }
}
