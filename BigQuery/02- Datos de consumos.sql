
----------------------------- 00. Uno datos Mercado Pago y Marketplace  ------------------------------------------

/* Comment Tomas: Traigo todos los datos de Mercado Pago de ventas por seller y segmento */

CREATE multiset volatile TABLE TPV_SEL_1 AS (

SELECT
  mp.cus_cust_id_sel cus_cust_id_sel, --- numero de usuario
  mp.sit_site_id sit_site_id, --- site
  tpv_segment tpv_segment, --- Segmento de donde vende
  tpv_segment_detail tpv_segment_detail, --- + detalle
  SUM (PAY_TRANSACTION_DOL_AMT) VENTAS_USD, --- suma volumen de ventas
  SUM (1) Q -- para contar la cantidad de segmentos que se vende   COUNT (tpv_segment_id) Q2
FROM WHOWNER.BT_MP_PAY_PAYMENTS mp
WHERE mp.tpv_flag = 1
AND mp.sit_site_id IN ('MLA')
AND MP.PAY_MOVE_DATE BETWEEN DATE '2020-01-01' AND DATE '2020-12-31'
AND mp.pay_status_code IN ( 'approved')--, 'authorized')
AND tpv_segment <> 'ON'--AND coalesce(i.ITE_TIPO_PROD,0) <> 'U' -----> Saco todo lo que es Marketplace
GROUP BY 1,2,3,4

UNION --- para poder hacer union las dos tablas son iguales

/* Comment Tomas: Traigo todos los datos de Marketplace */

SELECT
  mkp.ord_seller cus_cust_id_sel,  
  mkp.sit_site_id sit_site_id,
  'Selling ML' tpv_segment_id,
  'Selling ML' tpv_segment_detail,
  SUM((CASE WHEN coalesce(mkp.ord_fvf_bonif, 'Y') = 'N' THEN (bid.BID_BASE_CURRENT_PRICE * bid.BID_QUANTITY_OK) ELSE 0.0 END))  VENTAS_USD,
  SUM (1) Q
FROM WHOWNER.BT_ORD_ORDERS AS mkp
WHERE mkp.sit_site_id IN ('MLA')
AND bid.photo_id = 'TODATE'   --------------------- revisar migracion 
AND bid.tim_day_winning_date BETWEEN DATE '2020-01-01' AND DATE '2020-12-31'  --------------------- revisar migracion 
AND mkp.ord_gmv_flag = 1 
AND bid.mkt_marketplace_id = 'TM'    --------------------- revisar migracion 
AND coalesce(mkp.ord_auto_offer_flag, 0) <> 1 
AND coalesce(mkp.ord_fvf_bonif, 'Y') = 'N'
GROUP BY 1,2,3,4

) WITH data primary index (CUS_CUST_ID_SEL,SIT_SITE_ID) on commit preserve rows;
