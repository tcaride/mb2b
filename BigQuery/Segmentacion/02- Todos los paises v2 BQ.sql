/* SEGMENTACION  BIGQUERY */


------ SETTINGS ------

DECLARE from_date DATE DEFAULT '2020-11-01';  
DECLARE to_date DATE DEFAULT '2021-10-31';

/* ------------------------- Corro a nivel CUST ID ---------------------- */

----------------------------- 01. Traigo ventas: Uno datos Mercado Pago y Marketplace  ------------------------------------------

/* 00: Traigo todos los datos de Mercado Pago de ventas por seller y canal */

	DROP TABLE IF EXISTS meli-bi-data.SBOX_B2B_MKTPLACE.sell00_cust;
	CREATE TABLE meli-bi-data.SBOX_B2B_MKTPLACE.sell00_cust AS (
		SELECT
		  mp.cus_cust_id_sel cus_cust_id_sel, --- numero de usuario
		  mp.sit_site_id sit_site_id, --- site
		  tpv_segment_id tpv_segment_id, --- Segmento de donde vende
		  tpv_segment_detail tpv_segment_detail, --- + detalle
		  SUM (PAY_TRANSACTION_DOL_AMT) VENTAS_USD, --- suma volumen de ventas
		  SUM (1) Q -- para contar la cantidad de segmentos que se vende   COUNT (tpv_segment_id) Q2
		FROM `meli-bi-data.WHOWNER.BT_MP_PAY_PAYMENTS`  mp
		WHERE mp.tpv_flag = 1
		AND mp.sit_site_id IN ('MLA','MLB','MLM','MLC')
		AND MP.PAY_MOVE_DATE BETWEEN from_date AND to_date
		AND mp.pay_status_id IN ( 'approved')--, 'authorized')
		AND tpv_segment_id <> 'ON'--AND coalesce(i.ITE_TIPO_PROD,0) <> 'U' -----> Saco todo lo que es Marketplace
		GROUP BY 1,2,3,4

		UNION ALL --- para poder hacer union las dos tablas son iguales

	/* Comment: Traigo todos los datos de Marketplace */

		SELECT
			bid.ORD_SELLER.ID  cus_cust_id_sel,  
			bid.sit_site_id sit_site_id,
			'Selling Marketplace' tpv_segment_id,
			'Selling Marketplace' tpv_segment_detail,
	  	 	SUM(( CASE WHEN coalesce(bid.ORD_FVF_BONIF, True) = False 
	    	THEN (bid.ORD_ITEM.BASE_CURRENT_PRICE * 
			bid.ORD_ITEM.QTY) ELSE 0.0 END))  VENTAS_USD,
			SUM (1) Q
		FROM WHOWNER.BT_ORD_ORDERS as bid
		WHERE bid.sit_site_id IN ('MLA','MLB','MLM','MLC')
		-- AND bid.photo_id = 'TODATE' CAMPO DEPRECADO
		AND bid.ORD_CLOSED_DT BETWEEN from_date AND to_date
		AND bid.ORD_GMV_FLG = True -- bid.ite_gmv_flag = 1
		AND bid.ORD_CATEGORY.MARKETPLACE_ID = 'TM' -- bid.mkt_marketplace_id = 'TM'
		AND coalesce(BID.ORD_AUTO_OFFER_FLG, False) <> True --coalesce(BID.AUTO_OFFER_FLAG, 0) <> 1
		AND coalesce(bid.ORD_FVF_BONIF, True) = False --coalesce(bid.BID_FVF_BONIF, 'Y') = 'N'
		GROUP BY 1,2,3,4
);

/* 01: Agrego canal y sub canal */

	DROP TABLE IF EXISTS meli-bi-data.SBOX_B2B_MKTPLACE.sell01_cust;
	CREATE TABLE meli-bi-data.SBOX_B2B_MKTPLACE.sell01_cust as (
	  SELECT   
	  cus_cust_id_sel,
	  sit_site_id,
	  `meli-bi-data.SBOX_B2B_MKTPLACE.get_canal`(tpv_segment_detail) as Canal,
 	  `meli-bi-data.SBOX_B2B_MKTPLACE.get_subcanal`(tpv_segment_detail)  Subcanal,
	  tpv_segment_id, --- Segmento de donde vende
	  tpv_segment_detail,
	  VENTAS_USD,
	  Q
	FROM meli-bi-data.SBOX_B2B_MKTPLACE.sell00_cust
	);

/* 02: Traigo canal max */

	DROP TABLE IF EXISTS meli-bi-data.SBOX_B2B_MKTPLACE.sell02_cust;
	CREATE TABLE meli-bi-data.SBOX_B2B_MKTPLACE.sell02_cust as (
	SELECT
	  cus_cust_id_sel,
	  sit_site_id,
	  Canal
	FROM meli-bi-data.SBOX_B2B_MKTPLACE.sell01_cust
	group by 1,2,3
	qualify row_number () over (partition by cus_cust_id_sel, sit_site_id order by SUM(VENTAS_USD) DESC) = 1
	);

/* 03: Traigo segment detail max */
	
	DROP TABLE IF EXISTS meli-bi-data.SBOX_B2B_MKTPLACE.sell03_cust;
	CREATE TABLE meli-bi-data.SBOX_B2B_MKTPLACE.sell03_cust as (
	SELECT
	  cus_cust_id_sel,
	  sit_site_id,
	  Subcanal,
	  tpv_segment_detail
	FROM meli-bi-data.SBOX_B2B_MKTPLACE.sell01_cust
	group by 1,2,3,4
	qualify row_number () over (partition by cus_cust_id_sel, sit_site_id order by SUM(VENTAS_USD) DESC) = 1
	);

/* 04: Unifico */

	DROP TABLE IF EXISTS meli-bi-data.SBOX_B2B_MKTPLACE.sell04_cust;
	CREATE TABLE meli-bi-data.SBOX_B2B_MKTPLACE.sell04_cust as (
	SELECT
	  a01.cus_cust_id_sel,
	  a01.sit_site_id,
	  a02.canal canal_max,
	  a03.subcanal,
	  a03.tpv_segment_detail tpv_segment_detail_max,
	  sum(a01.VENTAS_USD) ventas_usd,
	  sum(a01.Q) cant_ventas,
	  count(distinct a01.tpv_segment_detail ) q_seg
	FROM meli-bi-data.SBOX_B2B_MKTPLACE.sell01_cust a01
	LEFT JOIN meli-bi-data.SBOX_B2B_MKTPLACE.sell02_cust a02
	on a01.cus_cust_id_sel=a02.cus_cust_id_sel
	LEFT join meli-bi-data.SBOX_B2B_MKTPLACE.sell03_cust a03
	on a01.cus_cust_id_sel=a03.cus_cust_id_sel

	group by 1,2,3,4,5
	);

/* 05: Agrego segmento seller y armo el rango de ventas puro */

	DROP TABLE IF EXISTS meli-bi-data.SBOX_B2B_MKTPLACE.sell05_cust;
	CREATE TABLE meli-bi-data.SBOX_B2B_MKTPLACE.sell05_cust as (
	  SELECT
	  a.cus_cust_id_sel,
	  a.sit_site_id,
	  b.SEGMENTO,
	  a.canal_max,
	  a.subcanal,
	  a.tpv_segment_detail_max,
	  `meli-bi-data.SBOX_B2B_MKTPLACE.get_rango_vta_puro`(ventas_usd) AS RANGO_VTA_PURO, -- 12
	  a.ventas_usd,
	  a.cant_ventas,
	  a.q_seg
	FROM meli-bi-data.SBOX_B2B_MKTPLACE.sell04_cust a
	LEFT JOIN WHOWNER.LK_SEGMENTO_SELLERS  b
	ON a.sit_site_id=b.sit_site_id AND a.cus_cust_id_sel=b.cus_cust_id_sel
	) ;

----------------------------- 02. Agrupo volumen de compras por buyer y categoria ------------------------------------------

/* 00: Traigo todas las compras en marketplace */

	DROP TABLE IF EXISTS meli-bi-data.SBOX_B2B_MKTPLACE.buy00_cust;
	CREATE TABLE meli-bi-data.SBOX_B2B_MKTPLACE.buy00_cust AS ( 
	SELECT
	    bid.ORD_BUYER.ID cus_cust_id_buy,  
	    bid.sit_site_id,
	    SUM((CASE WHEN ORD_TGMV_FLG = True 
	          THEN (bid.ORD_ITEM.BASE_CURRENT_PRICE * bid.ORD_ITEM.QTY) 
	          ELSE 0.0 
	        END))  TGMV_BUY, -- TGMV
	    COUNT((CASE WHEN ORD_TGMV_FLG = True then BID.ORD_ITEM.QTY else 0.0 end))  TORDERS_BUY,
	    SUM((CASE WHEN ORD_TGMV_FLG = True then BID.ORD_ITEM.QTY else 0.0 end))  TSI_BUY, -- TSI
	    COUNT(distinct (case when bid.crt_purchase_id is null then bid.ord_order_id else bid.crt_purchase_id end)) as TX_BUY

	FROM WHOWNER.BT_ORD_ORDERS AS bid
	
	WHERE bid.sit_site_id IN ('MLA','MLB','MLM','MLC')
	AND bid.ORD_CLOSED_DT BETWEEN from_date AND to_date
	AND bid.ORD_GMV_FLG = True --bid.ite_gmv_flag = 1
	AND bid.ORD_CATEGORY.MARKETPLACE_ID = 'TM' -- bid.mkt_marketplace_id = 'TM'
	AND bid.ORD_TGMV_FLG= True --tgmv_flag = 1
	GROUP BY 1,2
	);

/* 01: Traigo los meses en los que hizo compras */

	DROP TABLE IF EXISTS meli-bi-data.SBOX_B2B_MKTPLACE.buy01_cust;
	CREATE TABLE meli-bi-data.SBOX_B2B_MKTPLACE.buy01_cust AS (
	SELECT
	    bid.ORD_BUYER.ID cus_cust_id_buy,
	    bid.sit_site_id,
	    tim.TIM_MONTH_ID month_id,    
	    COUNT(distinct (case when bid.crt_purchase_id is null then bid.ord_order_id else bid.crt_purchase_id end)) as TX_BUY
	FROM WHOWNER.BT_ORD_ORDERS as bid
    LEFT JOIN WHOWNER.LK_TIM_DAYS tim
    on bid.ORD_CLOSED_DT= tim.TIM_DAY
	WHERE bid.sit_site_id IN ('MLA','MLB','MLM','MLC')
    AND bid.ORD_CLOSED_DT BETWEEN from_date AND to_date
	AND bid.ORD_GMV_FLG = True --bid.ite_gmv_flag = 1
	AND bid.ORD_CATEGORY.MARKETPLACE_ID = 'TM' -- bid.mkt_marketplace_id = 'TM'
	AND bid.ORD_TGMV_FLG= True --tgmv_flag = 1
	group by 1,2,3
	);

/* 02: Categorizo tipo de comprador  */

	DROP TABLE IF EXISTS meli-bi-data.SBOX_B2B_MKTPLACE.buy02_cust ;
	CREATE TABLE meli-bi-data.SBOX_B2B_MKTPLACE.buy02_cust AS (
	SELECT
	    tcb.cus_cust_id_buy,  
	    tcb.sit_site_id,
        cdt.cus_first_buy_no_bonif_autoof,
	    CASE WHEN (date_diff(to_date,cdt.cus_first_buy_no_bonif_autoof, month)+1)>12 THEN 12
        ELSE (date_diff(to_date,cdt.cus_first_buy_no_bonif_autoof, month)+1) END AS q_meses_para_compras,
	    CASE WHEN cdt.cus_first_buy_no_bonif_autoof IS null THEN 'Nunca Compro' ELSE 'Compro' END AS NB,
	    COUNT(distinct month_id) q_meses_con_compras  
	FROM meli-bi-data.SBOX_B2B_MKTPLACE.buy01_cust tcb
	LEFT JOIN WHOWNER.LK_CUS_CUSTOMER_DATES CDT ON CDT.CUS_CUST_ID=tcb.CUS_CUST_ID_BUY AND CDT.sit_site_id=tcb.SIT_SITE_ID
	group by 1,2,3,4,5

	);




----------------------------- 03. Unifico compras y ventas en una base comun ------------------------------------------

	DROP TABLE IF EXISTS meli-bi-data.SBOX_B2B_MKTPLACE.base00_cust;
	CREATE TABLE meli-bi-data.SBOX_B2B_MKTPLACE.base00_cust AS (
		SELECT
		coalesce(a.cus_cust_id_sel, b.cus_cust_id_buy) cus_cust_id, 
  		a.cus_cust_id_sel,
  		b.cus_cust_id_buy,
  		coalesce(a.sit_site_id, b.sit_site_id) sit_site_id,
		a.SEGMENTO,
	  	a.canal_max,
	  	a.subcanal,
	  	a.tpv_segment_detail_max,
	  	a.RANGO_VTA_PURO, 
	  	a.ventas_usd,
	  	a.cant_ventas,
	  	a.q_seg,
	  	b.TGMV_BUY,
	  	b.TORDERS_BUY,
	  	b.TSI_BUY,
	  	b.TX_BUY

	FROM meli-bi-data.SBOX_B2B_MKTPLACE.sell05_cust a
	FULL OUTER JOIN meli-bi-data.SBOX_B2B_MKTPLACE.buy00_cust b
	ON a.cus_cust_id_sel=b.cus_cust_id_buy 
	);

	DROP TABLE IF EXISTS meli-bi-data.SBOX_B2B_MKTPLACE.base01_cust;
	CREATE TABLE meli-bi-data.SBOX_B2B_MKTPLACE.base01_cust AS (
		SELECT
		a.*,
		CUS_PARTY_TYPE_ID AS CUSTOMER

	FROM meli-bi-data.SBOX_B2B_MKTPLACE.base00_cust a
	LEFT JOIN WHOWNER.LK_CUS_CUSTOMERS_DATA b
	ON a.cus_cust_id=b.cus_cust_id
	WHERE 'sink' not in unnest(b.CUS_TAGS)
	);

	DROP TABLE IF EXISTS meli-bi-data.SBOX_B2B_MKTPLACE.base02_cust;
	CREATE TABLE meli-bi-data.SBOX_B2B_MKTPLACE.base02_cust AS (
		SELECT
		a.*,
		b.KYC_ENTITY_TYPE,
		b.KYC_KNOWLEDGE_LEVEL,
		b.KYC_NAME_BRAND,
		CASE WHEN CHARACTER_LENGTH(REGEXP_REPLACE(b.KYC_COMP_CORPORATE_NAME, '[^0-9]*', ''))=11 THEN 'MEI' ELSE 'NOT MEI' END AS TIPO_MEI,
		b.KYC_COMP_SOCIETY_TYPE,
		b.KYC_COMP_INCOME,
		b.KYC_COMP_IDNT_ID,
		b.KYC_COMP_IDNT_NUMBER,
		b.KYC_COMP_CORPORATE_NAME
	FROM meli-bi-data.SBOX_B2B_MKTPLACE.base01_cust a
	LEFT JOIN WHOWNER.LK_KYC_VAULT_USER  b
	ON a.cus_cust_id=b.cus_cust_id
	);


------ DOC ------

----------------------------- 01. Traigo ventas: Uno datos Mercado Pago y Marketplace  ------------------------------------------

/* 00: Filtro SINK  */

	DROP TABLE IF EXISTS meli-bi-data.SBOX_B2B_MKTPLACE.sell00_doc;
	CREATE TABLE meli-bi-data.SBOX_B2B_MKTPLACE.sell00_doc as (
		SELECT a.*,   
		`meli-bi-data.SBOX_B2B_MKTPLACE.get_b2b_id`(KYC_COMP_IDNT_NUMBER,a.cus_cust_id_sel) b2b_id
		FROM meli-bi-data.SBOX_B2B_MKTPLACE.sell00_cust a
		LEFT JOIN WHOWNER.LK_CUS_CUSTOMERS_DATA e
		ON a.sit_site_id=e.sit_site_id_cus AND a.cus_cust_id_sel=e.cus_cust_id
		LEFT JOIN WHOWNER.LK_KYC_VAULT_USER b
		ON a.sit_site_id=b.sit_site_id AND a.cus_cust_id_sel=b.cus_cust_id
        WHERE 'sink' not in unnest(e.CUS_TAGS) 
	);

/* 01: Creo b2b_id y me traigo ventas por cust id, segment detail agregando canal y subcanal  */

	DROP TABLE IF EXISTS meli-bi-data.SBOX_B2B_MKTPLACE.sell01_doc ;
	CREATE TABLE meli-bi-data.SBOX_B2B_MKTPLACE.sell01_doc as (
	  SELECT 
      a.b2b_id, 
	  b.kyc_entity_type,
	  b.KYC_COMP_IDNT_NUMBER,
	  cus_cust_id_sel,
	  count(cus_cust_id_sel) over (partition by KYC_COMP_IDNT_NUMBER) count_cust,
	  a.sit_site_id,
	  `meli-bi-data.SBOX_B2B_MKTPLACE.get_canal`(tpv_segment_detail) as Canal,
 	  `meli-bi-data.SBOX_B2B_MKTPLACE.get_subcanal`(tpv_segment_detail)  Subcanal,
	  tpv_segment_id, --- Segmento de donde vende
	  tpv_segment_detail,
	  VENTAS_USD,
	  Q
	FROM meli-bi-data.SBOX_B2B_MKTPLACE.sell00_doc a
	LEFT JOIN meli-bi-data.SBOX_B2B_MKTPLACE.kyc_customer  b
	on a.sit_site_id=b.sit_site_id AND a.b2b_id=b.b2b_id
	-- WHERE kyc_entity_type = 'company'
	);

/* 02: Traigo canal max  */

	DROP TABLE IF EXISTS meli-bi-data.SBOX_B2B_MKTPLACE.sell02_doc;
	CREATE TABLE meli-bi-data.SBOX_B2B_MKTPLACE.sell02_doc as (
	SELECT
	  b2b_id,
	  sit_site_id,
	  Canal
	FROM meli-bi-data.SBOX_B2B_MKTPLACE.sell01_doc
	group by 1,2,3
	qualify row_number () over (partition by b2b_id, sit_site_id order by SUM(VENTAS_USD) DESC) = 1
	);

/* 03: Traigo segment detail max  */

	DROP TABLE IF EXISTS meli-bi-data.SBOX_B2B_MKTPLACE.sell03_doc;
	CREATE TABLE meli-bi-data.SBOX_B2B_MKTPLACE.sell03_doc as (
	SELECT
	  b2b_id,
	  sit_site_id,
	  subcanal,
	  tpv_segment_detail
	FROM meli-bi-data.SBOX_B2B_MKTPLACE.sell01_doc
	group by 1,2,3,4
	qualify row_number () over (partition by b2b_id, sit_site_id order by SUM(VENTAS_USD) DESC) = 1
	);

/* 04: Unifico  */

	DROP TABLE IF EXISTS meli-bi-data.SBOX_B2B_MKTPLACE.sell04_doc;
	CREATE TABLE meli-bi-data.SBOX_B2B_MKTPLACE.sell04_doc as (
		SELECT
		  a01.b2b_id,
		  a01.count_cust,
		  a01.kyc_entity_type,
		  a01.sit_site_id,
		  a02.canal canal_max,
		  a03.subcanal,
		  a03.tpv_segment_detail tpv_segment_detail_max,
		  sum(a01.VENTAS_USD) ventas_usd,
		  sum(a01.Q) cant_ventas,
		  count(distinct a01.tpv_segment_detail ) q_seg
		FROM meli-bi-data.SBOX_B2B_MKTPLACE.sell01_doc a01
		LEFT JOIN meli-bi-data.SBOX_B2B_MKTPLACE.sell02_doc a02
		on a01.b2b_id=a02.b2b_id
		LEFT JOIN meli-bi-data.SBOX_B2B_MKTPLACE.sell03_doc a03
		on a01.b2b_id=a03.b2b_id
		GROUP BY 1,2,3,4,5,6,7
	);

/* 05: Agrego segmento seller  */

	DROP TABLE IF EXISTS meli-bi-data.SBOX_B2B_MKTPLACE.sell05_doc;
	CREATE TABLE meli-bi-data.SBOX_B2B_MKTPLACE.sell05_doc AS (
	WITH 
	temp_segmento_id AS (
	SELECT
	DISTINCT a.b2b_id, a.KYC_COMP_IDNT_NUMBER,  a.cus_cust_id_sel, a.count_cust, b.segmento, 
	CASE WHEN b.segmento='TO' OR b.segmento='CARTERA GESTIONADA' THEN 1
	ELSE 0 END segmento_id
	FROM  meli-bi-data.SBOX_B2B_MKTPLACE.sell01_doc a 
	LEFT JOIN WHOWNER.LK_SEGMENTO_SELLERS  b
	ON a.sit_site_id=b.sit_site_id AND a.cus_cust_id_sel=b.cus_cust_id_sel),

	temp_sum_segmento_id AS (
	SELECT 
	b2b_id , count_cust, SUM(segmento_id) AS sum_segmento_id
	FROM temp_segmento_id
	GROUP BY b2b_id, count_cust
	),

	temp_segmento_final AS (
	SELECT
	b2b_id , count_cust, sum_segmento_id,
	CASE WHEN sum_segmento_id>0 THEN 'TO / CARTERA GESTIONADA'
	ELSE 'MID/LONG/UNKNOWN' END AS segmento_final
	FROM temp_sum_segmento_id   )

	SELECT
	  a.b2b_id,
	  a.count_cust,
	  a.kyc_entity_type,
	  a.sit_site_id,
	  b.segmento_final,
	  a.canal_max,
	  a.subcanal,
	  a.tpv_segment_detail_max,
	  a.ventas_usd,
	  a.cant_ventas,
	  a.q_seg
	  
	FROM meli-bi-data.SBOX_B2B_MKTPLACE.sell04_doc a
	LEFT JOIN temp_segmento_final  b
	on  a.b2b_id=b.b2b_id 
	) ;

/* 06: Agrego customer  */

	DROP TABLE IF EXISTS meli-bi-data.SBOX_B2B_MKTPLACE.sell06_doc;
	CREATE TABLE meli-bi-data.SBOX_B2B_MKTPLACE.sell06_doc as (
		SELECT
		  a.b2b_id,
		  a.count_cust,
		  a.kyc_entity_type,
		  a.sit_site_id,
		  a.segmento_final,
		  b.customer_final,
		  `meli-bi-data.SBOX_B2B_MKTPLACE.get_rango_vta_puro`(ventas_usd) AS RANGO_VTA_PURO, -- 12
		  a.canal_max,
		  a.subcanal,
		  a.tpv_segment_detail_max,
		  a.ventas_usd,
		  a.cant_ventas,
		  a.q_seg ,
		  CASE WHEN a.ventas_usd IS NULL or a.ventas_usd=0 THEN FALSE
  			ELSE TRUE END  as flag_vendedor -- 11
		FROM meli-bi-data.SBOX_B2B_MKTPLACE.sell05_doc a
		LEFT JOIN meli-bi-data.SBOX_B2B_MKTPLACE.kyc_customer  b
		on  a.b2b_id=b.b2b_id 
	)  ;


  DROP TABLE IF EXISTS meli-bi-data.SBOX_B2B_MKTPLACE.LK01_ACCOUNT_MONEY_DOC;
  CREATE TABLE meli-bi-data.SBOX_B2B_MKTPLACE.LK01_ACCOUNT_MONEY_DOC as (
 
  SELECT
    CASE WHEN v.b2b_id IS NULL THEN am.b2b_id ELSE v.b2b_id END AS b2b_id ,
    CASE WHEN v.sit_site_id IS NULL THEN am.sit_site_id ELSE v.sit_site_id  END AS sit_site_id,

    CASE WHEN v.VENTAS_USD is null or v.VENTAS_USD=0 or ((v.VENTAS_USD*cambio_mlm)/365) =0 then 'a.No Vende'
    WHEN am.balance/((v.VENTAS_USD*cambio_mlm)/365) <= 1 THEN 'b.Menos d lo que vende'
    WHEN am.balance/((v.VENTAS_USD*cambio_mlm)/365) <= 2 THEN 'c.Menos q el doble de lo que vende'
    WHEN am.balance/((v.VENTAS_USD*cambio_mlm)/365)  <= 5 THEN 'd.Hasta x 5 lo que vende'
    WHEN am.balance/((v.VENTAS_USD*cambio_mlm)/365) <= 20 THEN 'e.Hasta x 20 lo que vende'
    ELSE 'f.Mas de x 20 lo que vende' END as Ratio_AM_VTAS,

   CASE WHEN am.balance is null or am.balance=0 THEN 'No tiene AM'
    WHEN  am.balance< (20*cambio_mlm) THEN  'Menos 20 USD'
    WHEN am.balance<= (100*cambio_mlm)  THEN '20 a 100 USD'
    WHEN am.balance<= (300*cambio_mlm) THEN '100 a 300 USD'
    WHEN am.balance<= (1000*cambio_mlm)  THEN '300 a 1000 USD'
    WHEN am.balance<= (3000*cambio_mlm) THEN  '1000 a 3000 USD'
    WHEN am.balance<= (1000*cambio_mlm) THEN '3000 a 10000 USD'
    ELSE 'Mas de 10000 USD' END as ACCOUNT_MONEY

  FROM meli-bi-data.SBOX_B2B_MKTPLACE.sell06_doc AS V
  FULL OUTER JOIN meli-bi-data.SBOX_B2B_MKTPLACE.LK00_ACCOUNT_MONEY_DOC am
  on am.b2b_id=V.b2b_id
  AND am.sit_site_id=V.sit_site_id
  where v.SiT_SITE_ID='MLM' or am.sit_site_id='MLM'

UNION ALL

  SELECT
    CASE WHEN v.b2b_id IS NULL THEN am.b2b_id ELSE v.b2b_id END AS b2b_id ,
    CASE WHEN v.sit_site_id IS NULL THEN am.sit_site_id ELSE v.sit_site_id  END AS sit_site_id,

    CASE WHEN v.VENTAS_USD is null or v.VENTAS_USD=0 or ((v.VENTAS_USD*cambio_mlc)/365) =0 then 'a.No Vende'
    WHEN am.balance/((v.VENTAS_USD*cambio_mlc)/365) <= 1 THEN 'b.Menos d lo que vende'
    WHEN am.balance/((v.VENTAS_USD*cambio_mlc)/365) <= 2 THEN 'c.Menos q el doble de lo que vende'
    WHEN am.balance/((v.VENTAS_USD*cambio_mlc)/365)  <= 5 THEN 'd.Hasta x 5 lo que vende'
    WHEN am.balance/((v.VENTAS_USD*cambio_mlc)/365) <= 20 THEN 'e.Hasta x 20 lo que vende'
    ELSE 'f.Mas de x 20 lo que vende' END as Ratio_AM_VTAS,

   CASE WHEN am.balance is null or am.balance=0 THEN 'No tiene AM'
    WHEN  am.balance< (20*cambio_mlc) THEN  'Menos 20 USD'
    WHEN am.balance<= (100*cambio_mlc)  THEN '20 a 100 USD'
    WHEN am.balance<= (300*cambio_mlc) THEN '100 a 300 USD'
    WHEN am.balance<= (1000*cambio_mlc)  THEN '300 a 1000 USD'
    WHEN am.balance<= (3000*cambio_mlc) THEN  '1000 a 3000 USD'
    WHEN am.balance<= (1000*cambio_mlc) THEN '3000 a 10000 USD'
    ELSE 'Mas de 10000 USD' END as ACCOUNT_MONEY

  FROM meli-bi-data.SBOX_B2B_MKTPLACE.sell06_doc AS V
  FULL OUTER JOIN meli-bi-data.SBOX_B2B_MKTPLACE.LK00_ACCOUNT_MONEY_DOC am
  on am.b2b_id=V.b2b_id
  AND am.sit_site_id=V.sit_site_id
  where v.SiT_SITE_ID='MLC' or am.sit_site_id='MLC'

UNION ALL 
  SELECT
    CASE WHEN v.b2b_id IS NULL THEN am.b2b_id ELSE v.b2b_id END AS b2b_id ,
    CASE WHEN v.sit_site_id IS NULL THEN am.sit_site_id ELSE v.sit_site_id  END AS sit_site_id,

    CASE WHEN v.VENTAS_USD is null or v.VENTAS_USD=0 or ((v.VENTAS_USD*cambio_mlb)/365) =0 then 'a.No Vende'
    WHEN am.balance/((v.VENTAS_USD*cambio_mlb)/365) <= 1 THEN 'b.Menos d lo que vende'
    WHEN am.balance/((v.VENTAS_USD*cambio_mlb)/365) <= 2 THEN 'c.Menos q el doble de lo que vende'
    WHEN am.balance/((v.VENTAS_USD*cambio_mlb)/365)  <= 5 THEN 'd.Hasta x 5 lo que vende'
    WHEN am.balance/((v.VENTAS_USD*cambio_mlb)/365) <= 20 THEN 'e.Hasta x 20 lo que vende'
    ELSE 'f.Mas de x 20 lo que vende' END as Ratio_AM_VTAS,

   CASE WHEN am.balance is null or am.balance=0 THEN 'No tiene AM'
    WHEN  am.balance< (20*cambio_mlb) THEN  'Menos 20 USD'
    WHEN am.balance<= (100*cambio_mlb)  THEN '20 a 100 USD'
    WHEN am.balance<= (300*cambio_mlb) THEN '100 a 300 USD'
    WHEN am.balance<= (1000*cambio_mlb)  THEN '300 a 1000 USD'
    WHEN am.balance<= (3000*cambio_mlb) THEN  '1000 a 3000 USD'
    WHEN am.balance<= (1000*cambio_mlb) THEN '3000 a 10000 USD'
    ELSE 'Mas de 10000 USD' END as ACCOUNT_MONEY

  FROM meli-bi-data.SBOX_B2B_MKTPLACE.sell06_doc AS V
  FULL OUTER JOIN meli-bi-data.SBOX_B2B_MKTPLACE.LK00_ACCOUNT_MONEY_DOC am
  on am.b2b_id=V.b2b_id
  AND am.sit_site_id=V.sit_site_id
  where v.SiT_SITE_ID='MLB' or am.sit_site_id='MLB'

  
UNION ALL 
  SELECT
    CASE WHEN v.b2b_id IS NULL THEN am.b2b_id ELSE v.b2b_id END AS b2b_id ,
    CASE WHEN v.sit_site_id IS NULL THEN am.sit_site_id ELSE v.sit_site_id  END AS sit_site_id,

    CASE WHEN v.VENTAS_USD is null or v.VENTAS_USD=0 or ((v.VENTAS_USD*cambio_mla)/365) =0 then 'a.No Vende'
    WHEN am.balance/((v.VENTAS_USD*cambio_mla)/365) <= 1 THEN 'b.Menos d lo que vende'
    WHEN am.balance/((v.VENTAS_USD*cambio_mla)/365) <= 2 THEN 'c.Menos q el doble de lo que vende'
    WHEN am.balance/((v.VENTAS_USD*cambio_mla)/365)  <= 5 THEN 'd.Hasta x 5 lo que vende'
    WHEN am.balance/((v.VENTAS_USD*cambio_mla)/365) <= 20 THEN 'e.Hasta x 20 lo que vende'
    ELSE 'f.Mas de x 20 lo que vende' END as Ratio_AM_VTAS,

   CASE WHEN am.balance is null or am.balance=0 THEN 'No tiene AM'
    WHEN  am.balance< (20*cambio_mla) THEN  'Menos 20 USD'
    WHEN am.balance<= (100*cambio_mla)  THEN '20 a 100 USD'
    WHEN am.balance<= (300*cambio_mla) THEN '100 a 300 USD'
    WHEN am.balance<= (1000*cambio_mla)  THEN '300 a 1000 USD'
    WHEN am.balance<= (3000*cambio_mla) THEN  '1000 a 3000 USD'
    WHEN am.balance<= (1000*cambio_mla) THEN '3000 a 10000 USD'
    ELSE 'Mas de 10000 USD' END as ACCOUNT_MONEY

  FROM meli-bi-data.SBOX_B2B_MKTPLACE.sell06_doc AS V
  FULL OUTER JOIN meli-bi-data.SBOX_B2B_MKTPLACE.LK00_ACCOUNT_MONEY_DOC am
  on am.b2b_id=V.b2b_id
  AND am.sit_site_id=V.sit_site_id
  where v.SiT_SITE_ID='MLA' or am.sit_site_id='MLA'

  ) ;

----------------------------- 02. Agrupo volumen de compras por buyer y categoria ------------------------------------------

/* 00: Traigo todas las compras en marketplace */

	DROP TABLE IF EXISTS meli-bi-data.SBOX_B2B_MKTPLACE.buy00_doc ;
	CREATE TABLE meli-bi-data.SBOX_B2B_MKTPLACE.buy00_doc AS ( --- compras en el marketplace por empresa
		SELECT 
			`meli-bi-data.SBOX_B2B_MKTPLACE.get_b2b_id`(KYC_COMP_IDNT_NUMBER,a.cus_cust_id_buy) b2b_id,
	  		b.kyc_entity_type,
			b.KYC_COMP_IDNT_NUMBER,
			count(cus_cust_id_buy) over (partition by KYC_COMP_IDNT_NUMBER) count_cust,
			cus_cust_id_buy,
			a.sit_site_id,
			TGMV_BUY,
			TORDERS_BUY,
			tsi_buy,
			tx_buy
		FROM meli-bi-data.SBOX_B2B_MKTPLACE.buy00_cust a
		LEFT JOIN WHOWNER.LK_KYC_VAULT_USER  b
		on a.sit_site_id=b.sit_site_id AND a.cus_cust_id_buy=b.cus_cust_id
		LEFT JOIN WHOWNER.LK_CUS_CUSTOMERS_DATA e
		ON a.sit_site_id=e.sit_site_id_cus AND a.cus_cust_id_buy=e.cus_cust_id
		WHERE 'sink' not in unnest(e.CUS_TAGS) -- AND b.kyc_entity_type='company'
	);

/* 01: Agrupo por b2b_id todas las compras en marketplace */

	DROP TABLE IF EXISTS meli-bi-data.SBOX_B2B_MKTPLACE.buy01_doc ;
	CREATE TABLE meli-bi-data.SBOX_B2B_MKTPLACE.buy01_doc AS ( --- compras en el marketplace por empresa
		SELECT 
		  b2b_id,
		  kyc_entity_type,
		  KYC_COMP_IDNT_NUMBER,
		  count_cust,
		  sit_site_id,
		  sum(TGMV_BUY) TGMV_BUY,
		  sum(torders_buy) torders_buy,
		  sum(tsi_buy) tsi_buy,
		  sum(tx_buy) tx_buy,
		  CASE WHEN sum(TGMV_BUY) IS NULL or sum(TGMV_BUY)=0 THEN FALSE
  			ELSE TRUE END  as flag_comprador, -- 11
		FROM meli-bi-data.SBOX_B2B_MKTPLACE.buy00_doc
		GROUP BY 1,2,3,4,5
	);

/* 02: Agrupo por b2b_id todas las compras en marketplace */

	DROP TABLE IF EXISTS meli-bi-data.SBOX_B2B_MKTPLACE.buy02_doc;
	CREATE TABLE meli-bi-data.SBOX_B2B_MKTPLACE.buy02_doc AS ( --- compras en el marketplace por usuario 
    SELECT
    	`meli-bi-data.SBOX_B2B_MKTPLACE.get_b2b_id`(KYC_COMP_IDNT_NUMBER,bid.ORD_BUYER.ID) b2b_id,
	    bid.sit_site_id,
	    tim.TIM_MONTH_ID month_id,    
	    COUNT(distinct (case when bid.crt_purchase_id is null then bid.ord_order_id else bid.crt_purchase_id end)) as TX_BUY
	FROM WHOWNER.BT_ORD_ORDERS as bid
	LEFT JOIN WHOWNER.LK_KYC_VAULT_USER  b
	on bid.sit_site_id=b.sit_site_id AND bid.ORD_BUYER.ID =b.cus_cust_id
	LEFT JOIN WHOWNER.LK_CUS_CUSTOMERS_DATA e
	ON bid.sit_site_id=e.sit_site_id_cus AND bid.ORD_BUYER.ID =e.cus_cust_id
    LEFT JOIN WHOWNER.LK_TIM_DAYS tim
    on bid.ORD_CLOSED_DT= tim.TIM_DAY
	WHERE 'sink' not in unnest(e.CUS_TAGS) 
	AND bid.sit_site_id IN ('MLA','MLB','MLM','MLC')
    AND bid.ORD_CLOSED_DT BETWEEN from_date AND to_date
	AND bid.ORD_GMV_FLG = True --bid.ite_gmv_flag = 1
	AND bid.ORD_CATEGORY.MARKETPLACE_ID = 'TM' -- bid.mkt_marketplace_id = 'TM'
	AND bid.ORD_TGMV_FLG= True --tgmv_flag = 11
	group by 1,2,3


	);

/* 03: Agrupo por b2b_id first buy */

	DROP TABLE IF EXISTS meli-bi-data.SBOX_B2B_MKTPLACE.buy03_doc ;
	CREATE TABLE meli-bi-data.SBOX_B2B_MKTPLACE.buy03_doc AS (
	
	with temp_first_buy as (
    SELECT 
    `meli-bi-data.SBOX_B2B_MKTPLACE.get_b2b_id`(KYC_COMP_IDNT_NUMBER,a.cus_cust_id_buy) b2b_id,
    a.sit_site_id,
    min(a.cus_first_buy_no_bonif_autoof) cus_first_buy_no_bonif_autoof
    FROM meli-bi-data.SBOX_B2B_MKTPLACE.buy02_cust a
    LEFT JOIN WHOWNER.LK_KYC_VAULT_USER  b
	on a.sit_site_id=b.sit_site_id AND a.cus_cust_id_buy =b.cus_cust_id
    group by 1,2

)

	SELECT
	    tcb.b2b_id,  
	    tcb.sit_site_id,
        cdt.cus_first_buy_no_bonif_autoof,
	    CASE WHEN (date_diff(to_date,cdt.cus_first_buy_no_bonif_autoof, month)+1)>12 THEN 12
        ELSE (date_diff(to_date,cdt.cus_first_buy_no_bonif_autoof, month)+1) END AS q_meses_para_compras,
	    CASE WHEN cdt.cus_first_buy_no_bonif_autoof IS null THEN 'Nunca Compro' ELSE 'Compro' END AS NB,
	    COUNT(distinct month_id) q_meses_con_compras  
	FROM meli-bi-data.SBOX_B2B_MKTPLACE.buy02_doc tcb
	LEFT JOIN temp_first_buy CDT ON CDT.b2b_id=tcb.b2b_id AND CDT.sit_site_id=tcb.SIT_SITE_ID
	group by 1,2,3,4,5

	);