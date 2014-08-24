select distinct a.TS_MK_ID as TS_MK_ID,
                a.TS_MEDIUM_TITLE,
                a.TS_SSYMJ as TS_SSYMJ,
                TS_MK_BH,
                TS_BQ,
                TS_MK_TITLE as suggestion,
                TS_MK_LB,
                TS_MK_LB_ID,
                a.TS_SP_INFO_ID,
                TS_SP_QY,
                TB_FULL_NAME,
                TS_SCJ,
                TS_XSJ,
                TS_SP_PL_SF,
                TS_SP_DISCOUNT,
                TS_SP_SP_ZKL,
                TS_LONG_TITLE,
                TS_SECOND_TITLE,
                TS_RECOMMEND,
                TS_OFFER_TYPE,
                TS_OFFER_GROUP_TYPE,
                i.ts_asset_txm as shopid,
                i.ts_asset_name as ts_asset_name,
                TS_PP_ID,
                TS_PP_NAME as pinPai,
                0 as B,
                p.TS_PIC_URL,
                s.shuxing,
                concat(concat(concat(to_char(a.ts_mk_cdate, 'yyyy-MM-dd'),
                                     'T'),
                              to_char(a.ts_mk_cdate, 'HH24:mm:ss')),
                       'Z') update_time
  from  e_channel.TS_MK_INFO12500 a
  left outer join e_channel.TS_SP_PP_INFO ppinfo
    on ppinfo.TS_PP_INFO_ID = a.TS_PP_ID
  left outer join (select ts_mk_id,max(TS_PIC_URL) as TS_PIC_URL from e_channel.TS_MK_PIC12500 where ts_pic_gg='2' and ts_zt_sf=1 group by ts_mk_id) p 
    on p.ts_mk_id=a.ts_mk_id 
  left outer join (select ts_mk_id,cast(wm_concat(ts_name || ':' || ts_value ) AS varchar2(4000)) as shuxing  from e_channel.ts_mk_sp_sx12500 group by ts_mk_id) s
    on s.ts_mk_id=a.ts_mk_id
 inner join e_channel.TS_ASSET_INFO i
    on a.ts_gs_nm = i.ts_asset_id
 where TS_SP_QY = 1
   and TS_OFFER_TYPE <> '7'
   and TS_OFFER_TYPE <> '88'
   and TS_OFFER_TYPE <> '71'
   and TS_OFFER_TYPE <> '77'
   and (a.ts_tdy6 is null or a.ts_tdy6 <> '1')
   and i.ts_asset_txm='1003600051'
   
   
   