/* ------- 00- TRAGIO COMPRAS POR CUST ID POR MES ---------*/


 ------- PREPARAR PARA HACER UPDATE DE ULTIMOS 12 MESES ---------


DECLARE from_date DATE DEFAULT '2021-01-01';  
DECLARE to_date DATE DEFAULT '2021-09-30';
DROP TABLE IF EXISTS meli-bi-data.SBOX_B2B_MKTPLACE.buy_month_cust;
CREATE TABLE meli-bi-data.SBOX_B2B_MKTPLACE.buy_month_cust AS (
	SELECT
	    bid.ORD_BUYER.ID cus_cust_id_buy,  
	    bid.sit_site_id,
	    TIM_YEAR,
	    TIM_SEMESTER_ID,
	    TIM_QUARTER_ID,
	    TIM_MONTH_ID,
	    SUM((CASE WHEN ORD_TGMV_FLG = True 
	        	THEN (bid.ORD_ITEM.BASE_CURRENT_PRICE * bid.ORD_ITEM.QTY) 
	          	ELSE 0.0 
	        END))  TGMV_BUY, 
	    SUM((CASE WHEN ORD_TGMV_FLG = True 
	    		THEN (bid.ORD_ITEM.SITE_CURRENT_PRICE * bid.ORD_ITEM.QTY) 
	    		ELSE 0.0
	    	END))  TGMV_BUY_LC,
	    COUNT((CASE WHEN ORD_TGMV_FLG = True then BID.ORD_ITEM.QTY else 0.0 end))  TORDERS_BUY,
	    SUM((CASE WHEN ORD_TGMV_FLG = True then BID.ORD_ITEM.QTY else 0.0 end))  TSI_BUY, 
	    COUNT(distinct (case when bid.crt_purchase_id is null then bid.ord_order_id else bid.crt_purchase_id end)) as TX_BUY

	FROM WHOWNER.BT_ORD_ORDERS AS bid
	LEFT JOIN WHOWNER.LK_TIM_DAYS tim 
	ON bid.ORD_CLOSED_DT =tim.TIM_DAY
	WHERE bid.sit_site_id IN ('MLA','MLB','MLM','MLC')
	AND bid.ORD_CLOSED_DT BETWEEN from_date AND to_date
	AND bid.ORD_GMV_FLG = True --bid.ite_gmv_flag = 1
	AND bid.ORD_CATEGORY.MARKETPLACE_ID = 'TM' -- bid.mkt_marketplace_id = 'TM'
	AND bid.ORD_TGMV_FLG= True --tgmv_flag = 1
	GROUP BY 1,2, 3,4,5,6
);


/* ------- 01- TRAGIO VENTAS POR CUST ID POR MES ---------*/

DECLARE from_date DATE DEFAULT '2017-01-01';  
DECLARE to_date DATE DEFAULT '2021-09-30';

DROP TABLE IF EXISTS meli-bi-data.SBOX_B2B_MKTPLACE.sell_month_cust_base;
CREATE TABLE meli-bi-data.SBOX_B2B_MKTPLACE.sell_month_cust_base AS (
	SELECT
		TIM_YEAR,
	    TIM_SEMESTER_ID,
	    TIM_QUARTER_ID,
	    TIM_MONTH_ID,
		mp.cus_cust_id_sel cus_cust_id_sel, --- numero de usuario
		mp.sit_site_id sit_site_id, --- site
		SUM (PAY_TRANSACTION_DOL_AMT) VENTAS_USD, --- suma volumen de ventas
		SUM (1) Q -- para contar la cantidad de segmentos que se vende   COUNT (tpv_segment_id) Q2
	FROM `meli-bi-data.WHOWNER.BT_MP_PAY_PAYMENTS`  mp	
	LEFT JOIN WHOWNER.LK_TIM_DAYS tim 
	ON mp.PAY_MOVE_DATE =tim.TIM_DAY
	WHERE mp.tpv_flag = 1
	AND mp.sit_site_id IN ('MLA','MLB','MLM','MLC')
	AND MP.PAY_MOVE_DATE BETWEEN from_date AND to_date
	AND mp.pay_status_id IN ( 'approved')--, 'authorized')
	AND tpv_segment_id <> 'ON'--AND coalesce(i.ITE_TIPO_PROD,0) <> 'U' -----> Saco todo lo que es Marketplace
	GROUP BY 1,2,3,4,5,6

	UNION ALL --- para poder hacer union las dos tablas son iguales

	/* Comment: Traigo todos los datos de Marketplace */

	SELECT
		TIM_YEAR,
	    TIM_SEMESTER_ID,
	    TIM_QUARTER_ID,
	    TIM_MONTH_ID,
		bid.ORD_SELLER.ID  cus_cust_id_sel,  
		bid.sit_site_id sit_site_id,
  	 	SUM(( CASE WHEN coalesce(bid.ORD_FVF_BONIF, True) = False 
	    	THEN (bid.ORD_ITEM.BASE_CURRENT_PRICE * 
			bid.ORD_ITEM.QTY) ELSE 0.0 END))  VENTAS_USD,
		SUM (1) Q
		FROM WHOWNER.BT_ORD_ORDERS as bid
		LEFT JOIN WHOWNER.LK_TIM_DAYS tim 
		ON bid.ORD_CLOSED_DT =tim.TIM_DAY
		WHERE bid.sit_site_id IN ('MLA','MLB','MLM','MLC')
		-- AND bid.photo_id = 'TODATE' CAMPO DEPRECADO
		AND bid.ORD_CLOSED_DT BETWEEN from_date AND to_date
		AND bid.ORD_GMV_FLG = True -- bid.ite_gmv_flag = 1
		AND bid.ORD_CATEGORY.MARKETPLACE_ID = 'TM' -- bid.mkt_marketplace_id = 'TM'
		AND coalesce(BID.ORD_AUTO_OFFER_FLG, False) <> True --coalesce(BID.AUTO_OFFER_FLAG, 0) <> 1
		AND coalesce(bid.ORD_FVF_BONIF, True) = False --coalesce(bid.BID_FVF_BONIF, 'Y') = 'N'
		GROUP BY 1,2,3,4,5,6
);

----- AGRUPO ----
DROP TABLE IF EXISTS meli-bi-data.SBOX_B2B_MKTPLACE.sell_month_cust;
CREATE TABLE meli-bi-data.SBOX_B2B_MKTPLACE.sell_month_cust AS (
	SELECT
		TIM_YEAR,
	    TIM_SEMESTER_ID,
	    TIM_QUARTER_ID,
	    TIM_MONTH_ID,
		cus_cust_id_sel,
		sit_site_id,
		sum(VENTAS_USD) VENTAS_USD,
		sum(q) q_ventas,
	FROM meli-bi-data.SBOX_B2B_MKTPLACE.sell_month_cust_base
	GROUP BY 1,2,3,4,5,6
);

/* ------- 03- CREO TABLA FINAL POR CUST ID POR MES ---------*/

DROP TABLE IF EXISTS meli-bi-data.SBOX_B2B_MKTPLACE.base_month_cust;
CREATE TABLE meli-bi-data.SBOX_B2B_MKTPLACE.base_month_cust AS (
		SELECT
		coalesce(a.TIM_YEAR, b.TIM_YEAR) TIM_YEAR,
		coalesce(a.TIM_SEMESTER_ID, b.TIM_SEMESTER_ID) TIM_SEMESTER_ID,
	    coalesce(a.TIM_QUARTER_ID, b.TIM_QUARTER_ID) TIM_QUARTER_ID,
	    coalesce(a.TIM_MONTH_ID, b.TIM_MONTH_ID) TIM_MONTH_ID,
		coalesce(a.cus_cust_id_sel, b.cus_cust_id_buy) cus_cust_id, 
  		a.cus_cust_id_sel,
  		b.cus_cust_id_buy,
  		coalesce(a.sit_site_id, b.sit_site_id) sit_site_id,
		a.VENTAS_USD,
		a.q_ventas,
		b.TGMV_BUY,
		b.TGMV_BUY_LC,
		b.TORDERS_BUY,
		b.TSI_BUY,
		b.TX_BUY
	FROM meli-bi-data.SBOX_B2B_MKTPLACE.sell_month_cust a
	FULL OUTER JOIN  meli-bi-data.SBOX_B2B_MKTPLACE.buy_month_cust b
	on a.cus_cust_id_sel = b.cus_cust_id_buy
);

/* ------- 04- CREO TABLA FINAL POR B2B ID POR MES ---------*/
