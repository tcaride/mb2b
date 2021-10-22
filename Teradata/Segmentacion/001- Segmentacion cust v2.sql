
CREATE TABLE TEMP_45.segmentacion_cust as (
select
	a.cus_cust_id,
	-- a.cus_cust_id_sel
	-- a.CUS_CUST_ID_BUY
	a.sit_site_id,
	a.KYC_COMP_IDNT_NUMBER,
	a.KYC_ENTITY_TYPE,
	a.TIPO_MEI,
	a.canal_max,
	a.Subcanal,
	a.tpv_segment_detail_max,
	a.SEGMENTO,
	a.CUSTOMER,
	CASE WHEN a.tpv_segment_detail_max ='Selling Marketplace' THEN d.vertical 
      	ELSE c.MCC 
  	END AS RUBRO, 
  	e.cus_tax_payer_type, 
  	e.cus_tax_regime,
  	CASE WHEN a.TGMV_BUY IS NULL or a.TGMV_BUY=0 THEN 'No Compra' 
  		ELSE 'Compra' 
  	END  as TIPO_COMPRADOR_TGMV,
	a.RANGO_VTA_PURO, 
	CASE WHEN TIPO_COMPRADOR_TGMV = 'No Compra'  and a.RANGO_VTA_PURO ='a.No Vende' THEN 'NC y NV'
		  WHEN TIPO_COMPRADOR_TGMV = 'Compra'  and a.RANGO_VTA_PURO ='a.No Vende' THEN 'C y NV' 
		  WHEN TIPO_COMPRADOR_TGMV = 'No Compra'  and a.RANGO_VTA_PURO <>'a.No Vende' THEN 'NC y V'
		  ELSE 'C y V' 
	END AS tipo_consumidor,
	CASE WHEN a.KYC_ENTITY_TYPE='company' AND a.canal_max <>'Not Considered' THEN 'ok'
    WHEN a.KYC_ENTITY_TYPE <>'company' AND a.canal_max<>'Not Considered' AND RANGO_VTA_PURO not IN ('a.No Vende','b.Menos 6.000') THEN 'ok'
    ELSE 'no ok'
  	END as Baseline,
	g.ACCOUNT_MONEY, 
	  CASE WHEN g.ACCOUNT_MONEY ='No tiene AM' THEN 0
	    WHEN g.ACCOUNT_MONEY ='Menos 20 USD' or  g.ACCOUNT_MONEY = '100 a 300 USD' THEN 1
	    WHEN  g.ACCOUNT_MONEY = '300 a 1000 USD' or  g.ACCOUNT_MONEY = '1000 a 3000 USD' THEN 2
	    ELSE 3 
	  END as am_rank_am, -- 17
	  g.Ratio_AM_VTAS, 
	  CASE WHEN g.Ratio_AM_VTAS ='a.No Vende' THEN 0
	    WHEN g.Ratio_AM_VTAS ='b.Menos d lo que vende'or  g.Ratio_AM_VTAS = 'c.Menos q el doble de lo que vende' THEN 1
	    WHEN  g.Ratio_AM_VTAS = 'd.Hasta x 5 lo que vende' or  g.Ratio_AM_VTAS = 'e.Hasta x 20 lo que vende' THEN 2
	    ELSE 3 
	  END as am_rank_ventas, 
	  CASE WHEN a.VENTAS_USD > 0 THEN am_rank_ventas
	  ELSE am_rank_am
	  END am_rank,
	CASE WHEN h.LYL_LEVEL_NUMBER = 1 or h.LYL_LEVEL_NUMBER =2 THEN 1
	    WHEN h.LYL_LEVEL_NUMBER = 3 or h.LYL_LEVEL_NUMBER =4 THEN 2
	    WHEN h.LYL_LEVEL_NUMBER = 5 or h.LYL_LEVEL_NUMBER =6 THEN 3
	    ELSE NULL
  	END AS LOYALTY, -- 21
  	CASE WHEN l.cus_cust_id IS null THEN 0 ELSE 1 END AS SEGUROS, -- 22
  	CASE WHEN m.cus_cust_id IS null THEN 0 ELSE 1 END as CREDITOS, -- 23
  	CASE WHEN n.cus_cust_id_sel IS null THEN 0 ELSE 1 END as SHIPPING, -- 24
  	CASE WHEN a.Q_SEG + SEGUROS + CREDITOS + SHIPPING = 1 or a.Q_SEG + SEGUROS + CREDITOS + SHIPPING =2 THEN 1
    	WHEN  a.Q_SEG + SEGUROS + CREDITOS + SHIPPING = 3 or a.Q_SEG + SEGUROS + CREDITOS + SHIPPING =4  THEN 2
    	WHEN a.Q_SEG + SEGUROS + CREDITOS + SHIPPING >= 5 THEN 3
    	ELSE NULL
  	END AS ECOSISTEMA, -- 25
  	  CASE WHEN a.TGMV_BUY IS NULL AND o.NB='Nunca Compro' THEN 'Not Buyer'
    WHEN a.TGMV_BUY IS NULL AND o.NB='Compro' THEN 'Recover'
    WHEN a.TGMV_BUY IS not NULL AND o.q_cuenta in ('Menos 1Q','1Q')  AND o.cant_q_compras >=1 THEN 'Frequent_NB'
    WHEN a.TGMV_BUY IS not NULL AND o.q_cuenta in ('Menos 1Q','1Q')  AND o.cant_q_compras <1 THEN 'Non Frequent'
    WHEN a.TGMV_BUY IS not NULL AND o.q_cuenta='2Q' AND o.cant_q_compras >=2 THEN 'Frequent_NB'
    WHEN a.TGMV_BUY IS not NULL AND o.q_cuenta='2Q' AND o.cant_q_compras <2 THEN 'Non Frequent'
    WHEN a.TGMV_BUY IS not NULL AND o.q_cuenta='3Q' AND o.cant_q_compras >=3 THEN 'Frequent_NB'
    WHEN a.TGMV_BUY IS not NULL AND o.q_cuenta='3Q' AND o.cant_q_compras <3 THEN 'Non Frequent'
    WHEN a.TGMV_BUY IS not NULL AND o.q_cuenta='OK' AND o.cant_q_compras >=4 THEN 'Frequent'
    WHEN a.TGMV_BUY IS not NULL AND o.q_cuenta='OK' AND o.cant_q_compras <4 THEN 'Non Frequent'
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
  END engagement_rank,

    CASE WHEN Agg_Frecuencia='Non Buyer' then 'Not Buyer'
  when Agg_Frecuencia='Buyer' then 'Buyer Not Engaged'
  when engagement_rank=1 and Agg_Frecuencia= 'Frequent Buyer' then 'Buyer Not Engaged'
  when engagement_rank=2 and Agg_Frecuencia= 'Frequent Buyer' then 'Frequent Engaged Buyer'
  when engagement_rank=3 and Agg_Frecuencia= 'Frequent Buyer' then 'Frequent Engaged Buyer'
  else 'No puede ser'
  end as buyer_segment, 
  a.ventas_usd VENTAS_USD,
  a.TGMV_BUY  TGMV_BUY,

	a.TORDERS_BUY,
	a.TSI_BUY,
	a.TX_BUY,
	a.KYC_KNOWLEDGE_LEVEL,
	a.KYC_COMP_SOCIETY_TYPE,
  	a.KYC_COMP_CORPORATE_NAME
	



	FROM TEMP_45.base02_cust a
	LEFT JOIN temp_45.vert2 d ON a.cus_cust_id=d.cus_cust_id_sel
	LEFT JOIN temp_45.lk_lastmcc4 c ON a.cus_cust_id = c.cus_cust_id
	LEFT JOIN TEMP_45.LK_br_mx  e ON a.cus_cust_id=e.cus_cust_id
	LEFT JOIN TEMP_45.LK_account_money_cust g ON a.cus_cust_id=g.cus_cust_id
	LEFT JOIN BT_LYL_POINTS_SNAPSHOT h ON a.cus_cust_id=h.cus_cust_id AND h.tim_month_id = '202012'
	LEFT JOIN temp_45.lk_seguros l ON a.cus_cust_id=l.CUS_CUST_ID 
	LEFT JOIN temp_45.LK_credits m ON a.CUS_CUST_ID=m.CUS_CUST_ID
	LEFT JOIN temp_45.lk_seller_shipping n ON a.cus_cust_id=n.CUS_CUST_ID_sel
	LEFT JOIN temp_45.buy02_cust o ON a.cus_cust_id=o.cus_cust_id_buy 

	) with data primary index (sit_site_id,cus_cust_id);
