

DROP TABLE TEMP_45.segmentacion_doc;
CREATE  TABLE TEMP_45.segmentacion_doc AS (

SELECT
  a.b2b_id, -- 1
  a.SIT_SITE_ID, -- 2
  a.KYC_COMP_IDNT_NUMBER, -- 3
  a.KYC_ENTITY_TYPE, -- 4
  a.tipo_mei,
  b.canal_max, --5
  b.subcanal,
  b.tpv_segment_detail_max,--6
  b.segmento_final,--7
  a.customer_final,--8
  br_mx.cus_tax_payer_type,--9
  br_mx.cus_tax_regime, --10
  CASE WHEN f.TGMV_BUY IS NULL or f.TGMV_BUY=0 THEN 'No Compra' 
  ELSE 'Compra' END  as TIPO_COMPRADOR_TGMV, -- 11
  CASE WHEN b.VENTAS_USD IS null THEN 'a.No Vende'
    WHEN b.VENTAS_USD= 0 THEN 'a.No Vende'
    WHEN b.VENTAS_USD <= 6000 THEN 'b.Menos 6.000'
    WHEN b.VENTAS_USD <= 40000 THEN 'c.6.000 a 40.000'
    WHEN b.VENTAS_USD<= 200000 THEN 'd.40.000 a 200.000'
    ELSE 'e.Mas de 200.000' 
  END AS RANGO_VTA_PURO, -- 12
  CASE WHEN TIPO_COMPRADOR_TGMV = 'No Compra'  and RANGO_VTA_PURO ='a.No Vende' THEN 'NC y NV'
  WHEN TIPO_COMPRADOR_TGMV = 'Compra'  and RANGO_VTA_PURO ='a.No Vende' THEN 'C y NV' 
  WHEN TIPO_COMPRADOR_TGMV = 'No Compra'  and RANGO_VTA_PURO <>'a.No Vende' THEN 'NC y V'
  ELSE 'C y V' 
  END AS tipo_consumidor, -- 13
  CASE WHEN a.KYC_ENTITY_TYPE='company' AND b.canal_max <>'Not Considered' THEN 'ok'
    WHEN a.KYC_ENTITY_TYPE <>'company' AND b.canal_max<>'Not Considered' AND RANGO_VTA_PURO not IN ('a.No Vende','b.Menos 6.000') THEN 'ok'
    ELSE 'no ok'
  END as Baseline, -- 14
  g.ACCOUNT_MONEY, -- 15
  CASE WHEN g.ACCOUNT_MONEY ='No tiene AM' THEN 0
    WHEN g.ACCOUNT_MONEY ='Menos 20 USD' or  g.ACCOUNT_MONEY = '100 a 300 USD' THEN 1
    WHEN  g.ACCOUNT_MONEY = '300 a 1000 USD' or  g.ACCOUNT_MONEY = '1000 a 3000 USD' THEN 2
    ELSE 3 
  END as am_rank_am, -- 16
  g.Ratio_AM_VTAS, -- 17
  CASE WHEN g.Ratio_AM_VTAS ='a.No Vende' THEN 0
    WHEN g.Ratio_AM_VTAS ='b.Menos d lo que vende'or  g.Ratio_AM_VTAS = 'c.Menos q el doble de lo que vende' THEN 1
    WHEN  g.Ratio_AM_VTAS = 'd.Hasta x 5 lo que vende' or  g.Ratio_AM_VTAS = 'e.Hasta x 20 lo que vende' THEN 2
    ELSE 3 
  END as am_rank_ventas, -- 18
  CASE WHEN b.VENTAS_USD > 0 THEN am_rank_ventas
  ELSE am_rank_am
  END am_rank, -- 19

  CASE WHEN h.LYL_LEVEL_NUMBER = 1 or h.LYL_LEVEL_NUMBER =2 THEN 1
    WHEN h.LYL_LEVEL_NUMBER = 3 or h.LYL_LEVEL_NUMBER =4 THEN 2
    WHEN h.LYL_LEVEL_NUMBER = 5 or h.LYL_LEVEL_NUMBER =6 THEN 3
    ELSE NULL
  END AS LOYALTY, -- 20
  CASE WHEN l.b2b_id IS null THEN 0 ELSE 1 END AS SEGUROS, -- 21
  CASE WHEN m.b2b_id IS null THEN 0 ELSE 1 END as CREDITOS, -- 22
  CASE WHEN n.b2b_id IS null THEN 0 ELSE 1 END as SHIPPING, -- 23
  CASE WHEN b.Q_SEG + SEGUROS + CREDITOS + SHIPPING = 1 or b.Q_SEG + SEGUROS + CREDITOS + SHIPPING =2 THEN 1
    WHEN  b.Q_SEG + SEGUROS + CREDITOS + SHIPPING = 3 or b.Q_SEG + SEGUROS + CREDITOS + SHIPPING =4  THEN 2
    WHEN b.Q_SEG + SEGUROS + CREDITOS + SHIPPING >= 5 THEN 3
    ELSE NULL
  END AS ECOSISTEMA, -- 24
  CASE WHEN f.TGMV_BUY IS NULL AND o.NB='Nunca Compro' THEN 'Not Buyer'
    WHEN f.TGMV_BUY IS NULL AND o.NB='Compro' THEN 'Recover'
    WHEN f.TGMV_BUY IS not NULL AND o.q_cuenta in ('Menos 1Q','1Q')  AND o.cant_q_compras >=1 THEN 'Non Frequent'
    WHEN f.TGMV_BUY IS not NULL AND o.q_cuenta in ('Menos 1Q','1Q')  AND o.cant_q_compras <1 THEN 'Non Frequent'
    WHEN f.TGMV_BUY IS not NULL AND o.q_cuenta='2Q' AND o.cant_q_compras >=2 THEN 'Frequent_NB'
    WHEN f.TGMV_BUY IS not NULL AND o.q_cuenta='2Q' AND o.cant_q_compras <2 THEN 'Non Frequent'
    WHEN f.TGMV_BUY IS not NULL AND o.q_cuenta='3Q' AND o.cant_q_compras >=3 THEN 'Frequent_NB'
    WHEN f.TGMV_BUY IS not NULL AND o.q_cuenta='3Q' AND o.cant_q_compras <3 THEN 'Non Frequent'
    WHEN f.TGMV_BUY IS not NULL AND o.q_cuenta='OK' AND o.cant_q_compras >=4 THEN 'Frequent'
    WHEN f.TGMV_BUY IS not NULL AND o.q_cuenta='OK' AND o.cant_q_compras <4 THEN 'Non Frequent'
    ELSE 'TBD'
  END AS Frequencia, -- 25
  CASE WHEN Frequencia in ('TBD','Recover','Not Buyer') then 'Non Buyer'
  when Frequencia='Non Frequent' then 'Buyer'
  when Frequencia='Frequent' or Frequencia  = 'Frequent_NB' then 'Frequent Buyer'
  ELSE 'A definir'
  end Agg_Frecuencia, -- 26
  (3*coalesce(LOYALTY,0))+(4*coalesce(ECOSISTEMA,0))+(3*coalesce(am_rank,0)) engagement, -- 27
  CASE WHEN engagement <=10 THEN 1
    WHEN engagement <=20 THEN 2
    ELSE 3 
  END engagement_rank, -- 28
  
  CASE WHEN Agg_Frecuencia='Non Buyer'  then 'Not Buyer'
  when Agg_Frecuencia='Buyer' then 'Buyer Not Engaged'
  when engagement_rank=1 and Agg_Frecuencia= 'Frequent Buyer' then 'Buyer Not Engaged'
  when engagement_rank=2 and Agg_Frecuencia= 'Frequent Buyer' then 'Frequent Engaged Buyer'
  when engagement_rank=3 and Agg_Frecuencia= 'Frequent Buyer' then 'Frequent Engaged Buyer'
  else 'No puede ser'
  end as buyer_segment, --29
  
  b.VENTAS_USD VENTAS_USD,
  f.TGMV_BUY  bgmv_cpras,
  f.torders_buy,
  f.tsi_buy,
  f.tx_buy,


  a.kyc_comp_corporate_name
FROM TEMP_45.kyc_customer a
LEFT JOIN temp_45.sell06_doc AS b ON a.b2b_id=b.b2b_id and  a.sit_site_id = b.sit_site_id
LEFT JOIN  TEMP_45.LK_br_mx_doc as br_mx ON  a.b2b_id=br_mx.b2b_id  and  a.sit_site_id = br_mx.sit_site_id
LEFT JOIN temp_45.buy01_doc AS f ON a.b2b_id=f.b2b_id   and  a.sit_site_id = f.sit_site_id
LEFT JOIN TEMP_45.LK_account_money_doc g ON a.b2b_id=g.b2b_id
LEFT JOIN TEMP_45.LK_LOYALTY h ON a.b2b_id=h.b2b_id 
LEFT JOIN temp_45.lk_seguros l ON a.b2b_id=l.b2b_id 
LEFT JOIN temp_45.lk_credits m ON a.b2b_id=m.b2b_id
LEFT JOIN temp_45.lk_seller_shipping n ON a.b2b_id=n.b2b_id

LEFT JOIN temp_45.buy03_doc o ON a.b2b_id=o.b2b_id  and  a.sit_site_id = o.sit_site_id


WHERE ((a.KYC_ENTITY_TYPE = 'company' AND (TIPO_COMPRADOR_TGMV<>'No Compra' OR RANGO_VTA_PURO<> 'a.No Vende') )
OR (a.KYC_ENTITY_TYPE <> 'company' AND  b.VENTAS_USD >= 6000)) AND a.KYC_ENTITY_TYPE = 'company'
--GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22, 23, 24,25,26, 27,28, 29, 30, 31, 32, 33






)  with data primary index (sit_site_id,b2b_id);