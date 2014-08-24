package com.maxtech.data.tools;

public class FiledTypeConvert {

	public static String converOraToDB2(String type, int precision, int scale){
		String db2Type = null;
		if (type == null) return null;
		if("VARCHAR2".equals(type.toUpperCase())){
			db2Type = "VARCHAR("+precision+")"; 
		}else if("NUMBER".equals(type.toUpperCase())){
			
			if(precision != 0 && scale != 0){
				db2Type = "DECIMAL("+precision+","+scale+")"; 
			}else{
				db2Type = "INTEGER"; 
			}
			
		}else if("CHAR".equals(type.toUpperCase())){
			db2Type =  "CHAR("+precision+")"; 
		}else if("DATE".equals(type.toUpperCase())){
			db2Type =  "TIMESTAMP"; 
		}else if("DECIMAL".equals(type.toUpperCase())){
			db2Type = "DECIMAL("+precision+","+scale+")"; 
			
		}else{
			db2Type = type;
		}
		return db2Type;
	}
}
