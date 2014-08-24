select a.title as suggestion,
       concat(concat(concat(to_char(a.update_time, 'yyyy-MM-dd'), 'T'),
                     to_char(a.update_time, 'HH24:mm:ss')),
              'Z') update_time,
       a.title as TB_FULL_NAME,
       a.title as TS_LONG_TITLE,
       a.title as TS_MK_TITLE,
       a.shopid,
       a.productid as TS_MK_ID,
       a.SCREENINGKEYWORD as shuxing,
       OPRICE as TS_SCJ,
       CPRICE as TS_XSJ,
       PICURL as TS_PIC_URL,
       URL as TS_MK_BH,
       1 as B
  from TS_JYMK_INFO a