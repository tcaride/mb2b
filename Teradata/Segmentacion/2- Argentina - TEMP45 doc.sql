/* SEGMENTACION ARGENTINA TERADATA */


----------------------------- Categorizo account money ------------------------------------------

DROP TABLE TEMP_45.LK_account_money_doc_mla;
CREATE TABLE TEMP_45.LK_account_money_doc_mla as (
SELECT
  CASE WHEN v.b2b_id IS NULL THEN am.b2b_id ELSE v.b2b_id END AS b2b_id ,
  CASE WHEN v.sit_site_id IS NULL THEN am.sit_site_id ELSE v.sit_site_id  END AS sit_site_id,

  CASE WHEN v.VENTAS_USD is null or v.VENTAS_USD=0 or ((v.VENTAS_USD*95)/365) =0 then 'a.No Vende'
  WHEN am.balance/((v.VENTAS_USD*95)/365) <= 1 THEN 'b.Menos d lo que vende'
  WHEN am.balance/((v.VENTAS_USD*95)/365) <= 2 THEN 'c.Menos q el doble de lo que vende'
  WHEN am.balance/((v.VENTAS_USD*95)/365)  <= 5 THEN 'd.Hasta x 5 lo que vende'
  WHEN am.balance/((v.VENTAS_USD*95)/365) <= 20 THEN 'e.Hasta x 20 lo que vende'
  ELSE 'f.Mas de x 20 lo que vende' END as Ratio_AM_VTAS,

 CASE WHEN am.balance is null or am.balance=0 THEN 'No tiene AM'
  WHEN  am.balance< (20*95) THEN  'Menos 20 USD'
  WHEN am.balance<= (100*95)  THEN '20 a 100 USD'
  WHEN am.balance<= (300*95) THEN '100 a 300 USD'
  WHEN am.balance<= (1000*95)  THEN '300 a 1000 USD'
  WHEN am.balance<= (3000*95) THEN  '1000 a 3000 USD'
  WHEN am.balance<= (1000*95) THEN '3000 a 10000 USD'
  ELSE 'Mas de 10000 USD' END as ACCOUNT_MONEY

FROM TEMP_45.sell06_doc AS V
FULL OUTER JOIN TEMP_45.account_money am
on am.b2b_id=V.b2b_id
AND am.sit_site_id=V.sit_site_id
where v.SiT_SITE_ID='MLA' or am.sit_site_id='MLA'

) with data primary index (sit_site_id,b2b_id) ;


----------------------------- 19. Crea la tabla final ------------------------------------------

----------------------------- Creo la base de cust de empresas  ------------------------------------------
/* Traigo los datos del Vault de KYC marcados como company segun las relglas para el entity type por pais:
https://docs.google.com/presentation/d/1ExEk8mfT-Z9J6pibfZ8WUqegUOJzDYRpObmZLZzGVHs/edit#slide=id.g7735a7c26a_2_3
Y elimino los usuarios sink de la tabla de customers, son usuarios creados para carritos y cosas internas.
*/


DROP TABLE TEMP_45.segmentacion_doc_mla;
CREATE  TABLE TEMP_45.segmentacion_doc_mla  AS (

SELECT
  a.b2b_id, -- 1
  a.SIT_SITE_ID, -- 2
  a.KYC_COMP_IDNT_NUMBER, -- 3
  a.KYC_ENTITY_TYPE, -- 4
  b.canal_max,
  b.tpv_segment_detail_max,
  b.segmento_final,
  a.customer_final,
  br_mx.cus_tax_payer_type,
  br_mx.cus_tax_regime, 
  CASE WHEN f.TGMVEBILLABLE IS NULL or f.TGMVEBILLABLE=0 THEN 'No Compra' 
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
  g.ACCOUNT_MONEY, -- 16
  CASE WHEN g.ACCOUNT_MONEY ='No tiene AM' THEN 0
    WHEN g.ACCOUNT_MONEY ='Menos 20 USD' or  g.ACCOUNT_MONEY = '100 a 300 USD' THEN 1
    WHEN  g.ACCOUNT_MONEY = '300 a 1000 USD' or  g.ACCOUNT_MONEY = '1000 a 3000 USD' THEN 2
    ELSE 3 
  END as am_rank_am, -- 17
  g.Ratio_AM_VTAS, -- 18
  CASE WHEN g.Ratio_AM_VTAS ='a.No Vende' THEN 0
    WHEN g.Ratio_AM_VTAS ='b.Menos d lo que vende'or  g.Ratio_AM_VTAS = 'c.Menos q el doble de lo que vende' THEN 1
    WHEN  g.Ratio_AM_VTAS = 'd.Hasta x 5 lo que vende' or  g.Ratio_AM_VTAS = 'e.Hasta x 20 lo que vende' THEN 2
    ELSE 3 
  END as am_rank_ventas, -- 19
  CASE WHEN b.VENTAS_USD > 0 THEN am_rank_ventas
  ELSE am_rank_am
  END am_rank, -- 20

  CASE WHEN h.LYL_LEVEL_NUMBER = 1 or h.LYL_LEVEL_NUMBER =2 THEN 1
    WHEN h.LYL_LEVEL_NUMBER = 3 or h.LYL_LEVEL_NUMBER =4 THEN 2
    WHEN h.LYL_LEVEL_NUMBER = 5 or h.LYL_LEVEL_NUMBER =6 THEN 3
    ELSE NULL
  END AS LOYALTY, -- 21
  CASE WHEN l.b2b_id IS null THEN 0 ELSE 1 END AS SEGUROS, -- 22
  CASE WHEN m.b2b_id IS null THEN 0 ELSE 1 END as CREDITOS, -- 23
  CASE WHEN n.b2b_id IS null THEN 0 ELSE 1 END as SHIPPING, -- 24
  CASE WHEN b.Q_SEG + SEGUROS + CREDITOS + SHIPPING = 1 or b.Q_SEG + SEGUROS + CREDITOS + SHIPPING =2 THEN 1
    WHEN  b.Q_SEG + SEGUROS + CREDITOS + SHIPPING = 3 or b.Q_SEG + SEGUROS + CREDITOS + SHIPPING =4  THEN 2
    WHEN b.Q_SEG + SEGUROS + CREDITOS + SHIPPING >= 5 THEN 3
    ELSE NULL
  END AS ECOSISTEMA, -- 25
  CASE WHEN f.TGMVEBILLABLE IS NULL AND o.NB='Nunca Compro' THEN 'Not Buyer'
    WHEN f.TGMVEBILLABLE IS NULL AND o.NB='Compro' THEN 'Recover'
    WHEN f.TGMVEBILLABLE IS not NULL AND o.q_cuenta in ('Menos 1Q','1Q')  AND o.cant_q_compras >=1 THEN 'Frequent_NB'
    WHEN f.TGMVEBILLABLE IS not NULL AND o.q_cuenta in ('Menos 1Q','1Q')  AND o.cant_q_compras <1 THEN 'Non Frequent'
    WHEN f.TGMVEBILLABLE IS not NULL AND o.q_cuenta='2Q' AND o.cant_q_compras >=2 THEN 'Frequent_NB'
    WHEN f.TGMVEBILLABLE IS not NULL AND o.q_cuenta='2Q' AND o.cant_q_compras <2 THEN 'Non Frequent'
    WHEN f.TGMVEBILLABLE IS not NULL AND o.q_cuenta='3Q' AND o.cant_q_compras >=3 THEN 'Frequent_NB'
    WHEN f.TGMVEBILLABLE IS not NULL AND o.q_cuenta='3Q' AND o.cant_q_compras <3 THEN 'Non Frequent'
    WHEN f.TGMVEBILLABLE IS not NULL AND o.q_cuenta='OK' AND o.cant_q_compras >=4 THEN 'Frequent'
    WHEN f.TGMVEBILLABLE IS not NULL AND o.q_cuenta='OK' AND o.cant_q_compras <4 THEN 'Non Frequent'
    ELSE 'TBD'
  END AS Frequencia, -- 28
  CASE WHEN Frequencia='TBD' then 'Non Buyer'
  when Frequencia='Non Frequent' then 'Buyer'
  ELSE 'Frequent Buyer'
  end Agg_Frecuencia, -- 29
  (3*coalesce(LOYALTY,0))+(4*coalesce(ECOSISTEMA,0))+(3*coalesce(am_rank,0)) engagement, -- 30
  CASE WHEN engagement <=10 THEN 1
    WHEN engagement <=20 THEN 2
    ELSE 3 
  END engagement_rank, -- 31 
  
  CASE WHEN Agg_Frecuencia='Non Buyer' then 'Not Buyer'
  when Agg_Frecuencia='Buyer' then 'Buyer Not Engaged'
  when engagement_rank=1 and Agg_Frecuencia= 'Frequent Buyer' then 'Buyer Not Engaged'
  when engagement_rank=2 and Agg_Frecuencia= 'Frequent Buyer' then 'Frequent Engaged Buyer'
  when engagement_rank=3 and Agg_Frecuencia= 'Frequent Buyer' then 'Frequent Engaged Buyer'
  else 'No puede ser'
  end as buyer_segment, 
  
  b.VENTAS_USD VENTAS_USD,
  f.TGMVEBILLABLE  bgmv_cpras,

  a.kyc_comp_corporate_name
FROM TEMP_45.kyc_customer a
LEFT JOIN temp_45.sell06_doc AS b ON a.b2b_id=b.b2b_id and  a.sit_site_id = b.sit_site_id
LEFT JOIN  TEMP_45.LK_br_mx_doc as br_mx ON  a.b2b_id=br_mx.b2b_id  and  a.sit_site_id = br_mx.sit_site_id

LEFT JOIN temp_45.buy01_doc AS f ON a.b2b_id=f.b2b_id   and  a.sit_site_id = f.sit_site_id
LEFT JOIN TEMP_45.LK_account_money_doc_mla g ON a.b2b_id=g.b2b_id
LEFT JOIN TEMP_45.LK_LOYALTY h ON a.b2b_id=h.b2b_id 
LEFT JOIN temp_45.lk_seguros l ON a.b2b_id=l.b2b_id 
LEFT JOIN temp_45.lk_credits m ON a.b2b_id=m.b2b_id
LEFT JOIN temp_45.lk_seller_shipping n ON a.b2b_id=n.b2b_id

LEFT JOIN temp_45.buy03_doc o ON a.b2b_id=o.b2b_id  and  a.sit_site_id = o.sit_site_id


WHERE a.sit_site_id = 'MLA' 
AND ((a.KYC_ENTITY_TYPE = 'company' AND (TIPO_COMPRADOR_TGMV<>'No Compra' OR RANGO_VTA_PURO<> 'a.No Vende') )
OR (a.KYC_ENTITY_TYPE <> 'company' AND  b.VENTAS_USD >= 6000)) AND a.KYC_ENTITY_TYPE = 'company'
--GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22, 23, 24,25,26, 27,28, 29, 30, 31, 32, 33






)  with data primary index (sit_site_id,b2b_id);



