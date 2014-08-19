package com.maxtech.data.tools;

public class FiledTypeConvert {

	public static String converOraToDB2(String type, int precision){
		String db2Type = null;
		if (type == null) return null;
		if("VARCHAR2".equals(type.toUpperCase())){
			db2Type = "VARCHAR("+precision+")"; 
		}else if("NUMBER".equals(type.toUpperCase())){
			db2Type = "INTEGER"; 
		}else if("CHAR".equals(type.toUpperCase())){
			db2Type =  "CHAR("+precision+")"; 
		}else{
			db2Type = type;
		}
		return db2Type;
	}
}
