package com.maxtech.data.tools;

import java.util.List;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class Test {
	static Logger logger = Logger.getLogger(Test.class.getName());  
	static{
		PropertyConfigurator.configure ("src//log4j.properties");  
	}
	
	public static void main(String[] args) {
//		logger.info("hello");
		String sql = "INSERT INTO db2inst1.XCATENTRY (CATENTRY_ID,CATEGORY_AORB,FIELD,TB_SP_SX_ID,TB_ID,TS_MK_ID,TS_MEDIUM_TITLE,TS_SSYMJ,TS_MK_BH,TS_BQ,TS_MK_TITLE,TS_MK_LB,TS_MK_LB_ID,TS_SP_INFO_ID,TS_SP_QY,TB_FULL_NAME,TS_SCJ,TS_XSJ,TS_SP_PL_SF,TS_SP_DISCOUNT,TS_SP_SP_ZKL,TS_LONG_TITLE,TS_SECOND_TITLE,TS_RECOMMEND,TS_OFFER_TYPE,TS_OFFER_GROUP_TYPE,SHOPID,TS_ASSET_NAME,TS_PP_ID,TS_PP_NAME,PICURL,UPDATE_TIME) VALUES (2056338,'a','field001','TB_SP_SX_ID001','TB_ID001','00000000FF0197D2842463E5E043433210AC4557',null,799,'510338834','�ֻ� ��ֵ�� �����ֻ� ���� �����ֻ� ��׿�ֻ� ��׿','����B��������-�޹�����ר��',0,0,'00000000FB27F6FE874111EBE043AC1410ACC457',1,null,1099,799,1,0,null,'���� G3509i ��ɫ - GALAXY Trend3','4.3'/ ˫��1.2GHz/ ˫��������GSM������������ʱ������/ Android4.1 /����ͷ:300������/ RAM:768MBROM:4GB /���Ǹ��Լ۱��»���������!',null,'51',null,'10003','�Ϻ�',null,null,'http://zsc.189.cn/images/2014/06/27/2/00000000293BEA5AB00E4228B10857B194B61AC0.jpg','2014-07-25T16:07:03Z')";
//		String sql = "INSERT INTO db2inst1.XCATENTRY (CATENTRY_ID,CATEGORY_AORB,FIELD,TB_SP_SX_ID,TB_ID,TS_MK_ID,TS_MEDIUM_TITLE,TS_SSYMJ,TS_MK_BH,TS_BQ,TS_MK_TITLE,TS_MK_LB,TS_MK_LB_ID,TS_SP_INFO_ID,TS_SP_QY,TB_FULL_NAME,TS_SCJ,TS_XSJ,TS_SP_PL_SF,TS_SP_DISCOUNT,TS_SP_SP_ZKL,TS_LONG_TITLE,TS_SECOND_TITLE,TS_RECOMMEND,TS_OFFER_TYPE,TS_OFFER_GROUP_TYPE,SHOPID,TS_ASSET_NAME,TS_PP_ID,TS_PP_NAME,PICURL,UPDATE_TIME) VALUES (2056342,'a','field001','TB_SP_SX_ID001','TB_ID001','00000000FFDC33C946C83B0DE043433210AC5D72','�����������',1999,'531638991',null,'�����������',0,0,'00000000FFDC281959C23B0BE043433210AC803A',1,null,2000,1999,1,0,null,'�����������','�����������',null,'53',null,'10016','ɽ��ʡ',null,null,'http://zsc.189.cn/images/2014/08/05/2/00000000154FB9F56C6D476FAD28BB134CB32B37.jpg','2014-08-05T13:08:46Z')";
		watchSql(sql);
	}
	
	
	public static void watchSql(String sql){
		String columns = sql.substring(sql.indexOf("(")+1, sql.indexOf(")"));
		System.out.println(columns);
		String last = sql.substring(sql.indexOf(")")+1,sql.length());
//		System.out.println(last);
		String values = last.substring(last.indexOf("(")+1, last.indexOf(")"));
		System.out.println(values);
		
		String[] cl = columns.split(",");
		String[] vl = values.split(",");
		
		if(cl.length != vl.length){
			System.out.println("�ֶ���ֵ ��Ŀ��ƥ��");
			System.out.println("columns length:"+cl.length);
			System.out.println("values length"+vl.length);
			return ;
		}
		System.out.println("---------------------------------------");
		for (int i = 0; i < vl.length; i++) {
			System.out.println(cl[i]+":"+vl[i]);
		}
	}
}
