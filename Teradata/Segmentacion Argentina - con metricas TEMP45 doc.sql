/* SEGMENTACION ARGENTINA TERADATA */



----- CREO CAMPO CUSTOMER Y KYC POR B2B ID


CREATE TABLE TEMP_45.kyc_customer AS (

WITH temp_base AS
(
SELECT 
coalesce(KYC_IDENTIFICATION_NUMBER,a.cus_cust_id) b2b_id,
a.kyc_identification_number,
a.kyc_entity_type,
a.sit_site_id,
a.cus_cust_id,
e.cus_internal_tags,
CASE WHEN e.cus_internal_tags LIKE '%internal_user%' OR e.cus_internal_tags LIKE '%internal_third_party%' THEN 1
ELSE 0 END customer_id
FROM LK_KYC_VAULT_USER a
LEFT JOIN WHOWNER.LK_CUS_CUSTOMERS_DATA e
ON a.sit_site_id=e.sit_site_id_cus AND a.cus_cust_id=e.cus_cust_id
WHERE a.kyc_entity_type= 'company'
),

temp_sum_customer_id AS (

SELECT
b2b_id, kyc_identification_number,kyc_entity_type, sit_site_id,
-- count(cus_cust_id) over (partition by KYC_IDENTIFICATION_NUMBER) count_cust_kyc,
SUM(customer_id) AS sum_customer_id
FROM temp_base
GROUP BY b2b_id, kyc_identification_number,kyc_entity_type, sit_site_id
)


SELECT
b2b_id, kyc_identification_number,kyc_entity_type, sit_site_id,
Case WHEN sum_customer_id>0 THEN 'MELI'
ELSE '' END AS customer_final
FROM temp_sum_customer_id  

) with data primary index (b2b_id,SIT_SITE_ID) ;



----------------------------- 01. Traigo ventas: Uno datos Mercado Pago y Marketplace  ------------------------------------------

-- Filtro SINK
CREATE TABLE TEMP_45.sell00_doc_mla as (
SELECT * FROM TEMP_45.sell00_cust_mla a
LEFT JOIN WHOWNER.LK_CUS_CUSTOMERS_DATA e
ON a.sit_site_id=e.sit_site_id_cus AND a.cus_cust_id_sel=e.cus_cust_id
WHERE COALESCE(e.CUS_TAGS, '') <> 'sink' 
)WITH data primary index (CUS_CUST_ID_SEL,SIT_SITE_ID);

-- Creo b2b_id y me traigo ventas por cust id, segment detail agregando canal y subcanal

CREATE TABLE TEMP_45.sell01_doc_mla as (
  SELECT 
  coalesce(KYC_IDENTIFICATION_NUMBER,a.cus_cust_id_sel) b2b_id, 
  b.kyc_entity_type,
  b.KYC_IDENTIFICATION_NUMBER,
  cus_cust_id_sel,
  count(cus_cust_id_sel) over (partition by KYC_IDENTIFICATION_NUMBER) count_cust,
  a.sit_site_id,
  CASE WHEN tpv_segment_detail ='Aggregator - Other' THEN 'ON' 
    WHEN tpv_segment_detail ='Checkout OFF' THEN 'ON' 
    WHEN tpv_segment_detail ='Gateway' THEN 'ON' 
    WHEN tpv_segment_detail ='Meli Payments' THEN 'ON' 
    WHEN tpv_segment_detail ='Selling Marketplace' THEN 'ON'
    WHEN tpv_segment_detail ='ON' THEN 'ON'
    WHEN tpv_segment_detail ='Garex' THEN 'Insurtech' 
    WHEN tpv_segment_detail ='Insurtech Compensation' THEN 'Insurtech' 
    WHEN tpv_segment_detail ='Roda OFF' THEN 'Insurtech' 
    WHEN tpv_segment_detail ='Roda ON' THEN 'Insurtech' 
    WHEN tpv_segment_detail ='Instore' THEN 'OF'
    WHEN tpv_segment_detail ='Point' then 'OF'
    WHEN tpv_segment_detail ='Payment to suppliers' THEN 'Services'
    WHEN tpv_segment_detail ='Payroll' THEN 'Services'
    WHEN tpv_segment_detail is null then 'No Vende'
    ELSE 'Not Considered'
  END as Canal,
  CASE WHEN tpv_segment_detail ='Aggregator - Other' THEN 'OP' 
    WHEN tpv_segment_detail ='Checkout OFF' THEN 'OP' 
    WHEN tpv_segment_detail ='Gateway' THEN 'OP' 
    WHEN tpv_segment_detail ='Meli Payments' THEN 'OP' 
    WHEN tpv_segment_detail ='Selling Marketplace' THEN 'OP'
    WHEN tpv_segment_detail ='ON' THEN 'OP'
    WHEN tpv_segment_detail ='Garex' THEN 'Insurtech' 
    WHEN tpv_segment_detail ='Insurtech Compensation' THEN 'Insurtech' 
    WHEN tpv_segment_detail ='Roda OFF' THEN 'Insurtech' 
    WHEN tpv_segment_detail ='Roda ON' THEN 'Insurtech' 
    WHEN tpv_segment_detail ='Instore' THEN 'QR'
    WHEN tpv_segment_detail ='Point' then 'Point'
    WHEN tpv_segment_detail ='Payment to suppliers' THEN 'Services'
    WHEN tpv_segment_detail ='Payroll' THEN 'Services'
    WHEN tpv_segment_detail is null then 'No Vende'
    ELSE 'Not Considered'
  END as Subcanal,
  tpv_segment_id, --- Segmento de donde vende
  tpv_segment_detail,
  VENTAS_USD,
  Q
FROM TEMP_45.sell00_doc_mla a
LEFT JOIN WHOWNER.LK_KYC_VAULT_USER  b
on a.sit_site_id=b.sit_site_id AND a.cus_cust_id_sel=b.cus_cust_id
WHERE kyc_entity_type = 'company'
)WITH data primary index (CUS_CUST_ID_SEL,SIT_SITE_ID);

-- traigo canal max
CREATE TABLE TEMP_45.sell02_doc_mla as (
SELECT
  b2b_id,
  sit_site_id,
  Canal
FROM TEMP_45.sell01_doc_mla
group by 1,2,3
qualify row_number () over (partition by b2b_id, sit_site_id order by SUM(VENTAS_USD) DESC) = 1
)WITH data primary index (b2b_id,SIT_SITE_ID);

-- traigo segment detail max
CREATE TABLE TEMP_45.sell03_doc_mla as (
SELECT
  b2b_id,
  sit_site_id,
  tpv_segment_detail
FROM TEMP_45.sell01_doc_mla
group by 1,2,3
qualify row_number () over (partition by b2b_id, sit_site_id order by SUM(VENTAS_USD) DESC) = 1
)WITH data primary index (b2b_id,SIT_SITE_ID);

-- traigo todo en una tabla
CREATE TABLE TEMP_45.sell04_doc_mla as (
SELECT
  a01.b2b_id,
  a01.count_cust,
  a01.kyc_entity_type,
  a01.sit_site_id,
  a02.canal canal_max,
  a03.tpv_segment_detail tpv_segment_detail_max,
  sum(a01.VENTAS_USD) ventas_usd,
  sum(a01.Q) cant_ventas,
  count(distinct a01.tpv_segment_detail ) q_seg
FROM TEMP_45.sell01_doc_mla a01
left join TEMP_45.sell02_doc_mla a02
on a01.b2b_id=a02.b2b_id
left join TEMP_45.sell03_doc_mla a03
on a01.b2b_id=a03.b2b_id

group by 1,2,3,4,5,6
)WITH data primary index (b2b_id,SIT_SITE_ID);


-- agrego segmento seller
CREATE TABLE TEMP_45.sell05_doc_mla AS (

WITH 
temp_segmento_id AS (
SELECT
DISTINCT a.b2b_id, a.kyc_identification_number,  a.cus_cust_id_sel, a.count_cust, b.segmento, 
CASE WHEN b.segmento='TO' OR b.segmento='CARTERA GESTIONADA' THEN 1
ELSE 0 END segmento_id
FROM  TEMP_45.sell01_doc_mla a 
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
  a.tpv_segment_detail_max,
  a.ventas_usd,
  a.cant_ventas,
  a.q_seg
  
FROM TEMP_45.sell04_doc_mla a
LEFT JOIN temp_segmento_final  b
on  a.b2b_id=b.b2b_id 
) with data primary index (b2b_id,SIT_SITE_ID) ;


-- agrego customer 
CREATE TABLE TEMP_45.sell06_doc_mla as (

SELECT
  a.b2b_id,
  a.count_cust,
  a.kyc_entity_type,
  a.sit_site_id,
  a.segmento_final,
  b.customer_final,
  a.canal_max,
  a.tpv_segment_detail_max,
  a.ventas_usd,
  a.cant_ventas,
  a.q_seg
  
FROM TEMP_45.sell05_doc_mla a
LEFT JOIN TEMP_45.kyc_customer  b
on  a.b2b_id=b.b2b_id 
) with data primary index (b2b_id,SIT_SITE_ID) ;



----------------------------- 02. Agrupo volumen de compras por buyer y categoria ------------------------------------------

CREATE TABLE temp_45.buy00_doc_mla AS ( --- compras en el marketplace por empresa
SELECT 
  coalesce(KYC_IDENTIFICATION_NUMBER,a.cus_cust_id_buy) b2b_id, 
  b.kyc_entity_type,
  b.KYC_IDENTIFICATION_NUMBER,
  count(cus_cust_id_buy) over (partition by KYC_IDENTIFICATION_NUMBER) count_cust,
  cus_cust_id_buy,
  a.sit_site_id,
  tgmv_comp,
  tgmv_auto,
  tgmv_beauty, 
  tgmv_ce,
  TGMVEBILLABLE,
  torders_buy,
  tsie_buy,
  tx_buy

FROM TEMP_45.buy00_cust_mla a
LEFT JOIN WHOWNER.LK_KYC_VAULT_USER  b
on a.sit_site_id=b.sit_site_id AND a.cus_cust_id_buy=b.cus_cust_id
LEFT JOIN WHOWNER.LK_CUS_CUSTOMERS_DATA e
ON a.sit_site_id=e.sit_site_id_cus AND a.cus_cust_id_buy=e.cus_cust_id
WHERE COALESCE(e.CUS_TAGS, '') <> 'sink' AND b.kyc_entity_type='company'
)WITH DATA PRIMARY INDEX (sit_site_id,cus_cust_id_buy);

--- Agrupo por b2b_id
CREATE TABLE temp_45.buy01_doc_mla AS ( --- compras en el marketplace por empresa
select 
  b2b_id,
  kyc_entity_type,
  KYC_IDENTIFICATION_NUMBER,
  count_cust,
  sit_site_id,
  sum(tgmv_comp) tgmv_comp,
  sum(tgmv_auto) tgmv_auto,
  sum(tgmv_beauty) tgmv_beauty, 
  sum(tgmv_ce) tgmv_ce ,
  sum(TGMVEBILLABLE) TGMVEBILLABLE,
  sum(torders_buy) torders_buy,
  sum(tsie_buy) tsie_buy,
  sum(tx_buy) tx_buy

FROM TEMP_45.buy00_doc_mla
group by 1,2,3,4,5

)WITH DATA PRIMARY INDEX (b2b_id,sit_site_id);

----------------------------- 05. Traigo los Q en los que hizo compras. -----------------------------------------


CREATE TABLE TEMP_45.buy02_cust_mla AS ( --- compras en el marketplace por usuario 
SELECT
  coalesce(KYC_IDENTIFICATION_NUMBER,bid.cus_cust_id_buy) b2b_id, 
  b.kyc_entity_type,
  b.KYC_IDENTIFICATION_NUMBER,
    bid.cus_cust_id_buy,  
    bid.sit_site_id,
     ((CAST(EXTRACT(MONTH FROM bid.TIM_DAY_WINNING_DATE) AS BYTEINT)-1)/3)+1
  || 'Q' || substring(bid.TIM_DAY_WINNING_DATE,3,2) quarter,    
    COUNT(distinct (case when bid.crt_purchase_id is null then bid.ord_order_id else bid.crt_purchase_id end)) as TX_BUY
FROM WHOWNER.BT_BIDS as bid
LEFT JOIN WHOWNER.LK_KYC_VAULT_USER  b
on bid.sit_site_id=b.sit_site_id AND bid.cus_cust_id_buy=b.cus_cust_id
LEFT JOIN WHOWNER.LK_CUS_CUSTOMERS_DATA e
ON bid.sit_site_id=e.sit_site_id_cus AND bid.cus_cust_id_buy=e.cus_cust_id
WHERE COALESCE(e.CUS_TAGS, '') <> 'sink' AND b.kyc_entity_type='company'
AND bid.sit_site_id IN ('MLA')
AND bid.photo_id = 'TODATE' 
AND bid.tim_day_winning_date between DATE '2020-01-01' AND DATE '2020-12-31'
AND bid.ite_gmv_flag = 1
AND bid.mkt_marketplace_id = 'TM'
AND tgmv_flag = 1
group by 1,2,3,4,5,6

)WITH DATA PRIMARY INDEX (sit_site_id,cus_cust_id_buy);


----------------------------- 06. Categorizo tipo de comprador -----------------------------------------

CREATE TABLE TEMP_45.buy03_cust_mla AS (
WITH temp_first_buy AS (
SELECT  
coalesce(KYC_IDENTIFICATION_NUMBER,a.cus_cust_id) b2b_id, 
b.kyc_entity_type,
b.KYC_IDENTIFICATION_NUMBER,
a.cus_cust_id,  
a.sit_site_id, 
a.cus_first_buy_no_bonif_autoof
FROM WHOWNER.LK_CUS_CUSTOMER_DATES a
LEFT JOIN WHOWNER.LK_KYC_VAULT_USER  b
on a.sit_site_id=b.sit_site_id AND a.cus_cust_id=b.cus_cust_id
LEFT JOIN WHOWNER.LK_CUS_CUSTOMERS_DATA e
ON a.sit_site_id=e.sit_site_id_cus AND a.cus_cust_id=e.cus_cust_id
WHERE COALESCE(e.CUS_TAGS, '') <> 'sink' AND b.kyc_entity_type='company' AND a.sit_site_id = 'MLA' and a.cus_first_buy_no_bonif_autoof is not null
),

temp_first_buy_b2b as (
SELECT
b2b_id, 
kyc_entity_type,
KYC_IDENTIFICATION_NUMBER,
sit_site_id, 
min(cus_first_buy_no_bonif_autoof) cus_first_buy_no_bonif_autoof
from temp_first_buy
group by 1,2,3,4
)

SELECT
    tcb.b2b_id,  
    tcb.sit_site_id,
    b.cus_first_buy_no_bonif_autoof,
    CASE WHEN b.cus_first_buy_no_bonif_autoof <= '2020-01-01' then 'OK'
      WHEN b.cus_first_buy_no_bonif_autoof <= '2020-03-31' then '3Q'
      WHEN b.cus_first_buy_no_bonif_autoof <= '2020-06-30' then '2Q'
      WHEN b.cus_first_buy_no_bonif_autoof <= '2020-09-30' then '1Q'
    ELSE 'Menos 1Q'  end as Q_cuenta,
    CASE WHEN b.cus_first_buy_no_bonif_autoof IS null THEN 'Nunca Compro' ELSE 'Compro' END AS NB,
    COUNT(distinct quarter) cant_q_compras  
FROM TEMP_45.buy02_cust_mla tcb
LEFT JOIN temp_first_buy_b2b b ON b.b2b_id=tcb.b2b_id AND b.sit_site_id=tcb.SIT_SITE_ID
group by 1,2,3,4
)WITH DATA PRIMARY INDEX (b2b_id,sit_site_id);

----------------------------- 07. Traigo el regimen fiscal del cust: Para argentina no son consistentes los datos pero mantengo para mantener las columnas ------------------------------------------

CREATE TABLE TEMP_45.LK_br_mx as ( -- tipo de regimen
SELECT
  cus_cust_id,
  sit_site_id,
  cus_tax_payer_type,
  cus_tax_regime   ----Charly: NO TIENE SENTIDO XQ SON TODOS NULL en argentina
FROM LK_TAX_CUST_WRAPPER
qualify row_number () over (partition by cus_cust_id, sit_site_id ORDER BY  aud_upd_dt DESC) = 1
) with data primary index (sit_site_id,cus_cust_id);


----------------------------- 14. Trae la plata en cuenta de mercadopago ------------------------------------------

CREATE TABLE TEMP_45.account_money as (
WITH temp_account AS (
SELECT
  coalesce(KYC_IDENTIFICATION_NUMBER,a.cus_cust_id) b2b_id, 
  b.kyc_entity_type,
  b.KYC_IDENTIFICATION_NUMBER,
  a.cus_cust_id,
  a.sit_site_id,
  AVG (AVAILABLE_BALANCE) balance
FROM  BT_MP_SALDOS_SITE a
LEFT JOIN WHOWNER.LK_KYC_VAULT_USER  b
on a.sit_site_id=b.sit_site_id AND a.cus_cust_id=b.cus_cust_id
LEFT JOIN WHOWNER.LK_CUS_CUSTOMERS_DATA e
ON a.sit_site_id=e.sit_site_id_cus AND a.cus_cust_id=e.cus_cust_id
WHERE COALESCE(e.CUS_TAGS, '') <> 'sink' AND b.kyc_entity_type='company'
AND TIM_DAY BETWEEN DATE '2021-03-01' AND DATE '2021-04-30'
AND a.sit_site_id IN ('MLA','MLM','MLC')
GROUP BY 1,2,3,4,5
)

SELECT 
b2b_id,
kyc_entity_type,
KYC_IDENTIFICATION_NUMBER,
sit_site_id,
sum(balance) balance
FROM temp_account
GROUP BY  1,2,3,4
) with data primary index (sit_site_id,b2b_id) ;

----------------------------- 15. Categoriza la account money ------------------------------------------

CREATE TABLE TEMP_45.account_money2_mla as (
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
  WHEN  am.balance<((100*95)/5) THEN 'Menos 1900 Pesos'
  WHEN am.balance<=((500*95)/5) THEN '1900 a 9500 Pesos'
  WHEN am.balance<=((1500*95)/5) THEN '9500 a 28500 Pesos'
  WHEN am.balance<=((5000*95)/5) THEN '28500 a 95000 Pesos'
  WHEN am.balance<=((15000*95)/5) THEN '95000 a 285000 Pesos'
  WHEN am.balance<=((50000*95)/5) THEN '285000 a 950000 Pesos'
  ELSE 'Mas de 950000 Pesos' END as ACCOUNT_MONEY

FROM TEMP_45.sell06_doc_mla AS V
FULL OUTER JOIN TEMP_45.account_money am
on am.b2b_id=V.b2b_id
AND am.sit_site_id=V.sit_site_id
where v.SiT_SITE_ID='MLA' or am.sit_site_id='MLA'

) with data primary index (sit_site_id,b2b_id) ;

----------------------------- 16. Trae datos de creditos ------------------------------------------

CREATE TABLE TEMP_45.LK_credits as (
SELECT
  coalesce(KYC_IDENTIFICATION_NUMBER,a.CUS_CUST_ID_BORROWER) b2b_id, 
  b.kyc_entity_type,
  b.KYC_IDENTIFICATION_NUMBER,
  a.CUS_CUST_ID_BORROWER CUS_CUST_ID,
  a.SIT_SITE_ID,
  COUNT(*) total
FROM WHOWNER.BT_MP_CREDITS a
LEFT JOIN WHOWNER.LK_KYC_VAULT_USER  b
on a.sit_site_id=b.sit_site_id AND a.CUS_CUST_ID_BORROWER=b.cus_cust_id
LEFT JOIN WHOWNER.LK_CUS_CUSTOMERS_DATA e
ON a.sit_site_id=e.sit_site_id_cus AND a.CUS_CUST_ID_BORROWER=e.cus_cust_id
WHERE COALESCE(e.CUS_TAGS, '') <> 'sink' AND b.kyc_entity_type='company'

AND CRD_CREDIT_FINISH_DATE_ID >= DATE '2020-01-01'
AND a.sit_site_id IN ('MLA','MLM','MLC')
GROUP BY 1,2,3,4,5

) with data primary index (sit_site_id,b2b_id);

----------------------------- 17. Trae datos de seguros ------------------------------------------

CREATE TABLE TEMP_45.Lk_seguros as (
SELECT
  coalesce(KYC_IDENTIFICATION_NUMBER,a.CUS_CUST_ID_buy) b2b_id, 
  b.kyc_entity_type,
  b.KYC_IDENTIFICATION_NUMBER,
  a.CUS_CUST_ID_buy CUS_CUST_ID,
  a.SIT_SITE_ID,
  COUNT(*) total
FROM WHOWNER.BT_INSURANCE_PURCHASES a
LEFT JOIN WHOWNER.LK_KYC_VAULT_USER  b
on a.sit_site_id=b.sit_site_id AND a.CUS_CUST_ID_buy=b.cus_cust_id
LEFT JOIN WHOWNER.LK_CUS_CUSTOMERS_DATA e
ON a.sit_site_id=e.sit_site_id_cus AND a.CUS_CUST_ID_buy=e.cus_cust_id
WHERE COALESCE(e.CUS_TAGS, '') <> 'sink' AND b.kyc_entity_type='company'
AND  INSUR_STATUS_ID = 'confirmed'
AND a.sit_site_id IN ('MLA','MLM','MLC')
GROUP BY 1,2,3,4,5
) with data primary index (sit_site_id,b2b_id);

----------------------------- 18. Trae datos de shipping ------------------------------------------

CREATE TABLE temp_45.lk_seller_shipping AS (
SELECT
  coalesce(KYC_IDENTIFICATION_NUMBER,bid.cus_cust_id_sel) b2b_id, 
  b.kyc_entity_type,
  b.KYC_IDENTIFICATION_NUMBER,
  bid.sit_site_id,
  bid.cus_cust_id_sel cus_cust_id_sel,
  COUNT(*) total
FROM WHOWNER.BT_BIDS AS bid
LEFT JOIN WHOWNER.BT_SHP_SHIPMENTS AS shp
on bid.shp_shipment_id = shp.shp_shipment_id AND shp.sit_site_id = bid.sit_site_id

LEFT JOIN WHOWNER.LK_KYC_VAULT_USER  b
on bid.sit_site_id=b.sit_site_id AND bid.cus_cust_id_sel=b.cus_cust_id
LEFT JOIN WHOWNER.LK_CUS_CUSTOMERS_DATA e
ON bid.sit_site_id=e.sit_site_id_cus AND bid.cus_cust_id_sel=e.cus_cust_id
WHERE COALESCE(e.CUS_TAGS, '') <> 'sink' AND b.kyc_entity_type='company'

AND bid.sit_site_id IN ('MLA','MLM','MLC')--- ('MCO', 'MLA', 'MLB', 'MLC', 'MLM', 'MLU', 'MPE')
AND bid.photo_id = 'TODATE'
AND bid.tim_day_winning_date BETWEEN DATE '2020-10-01' AND DATE '2020-12-31' --OR bid.tim_day_winning_date between DATE '2019-01-01' and '2019-10-31')
AND bid.ite_gmv_flag = 1
AND bid.mkt_marketplace_id = 'TM'
AND shp.shp_picking_type_id IN ('xd_drop_off','cross_docking','fulfillment')
AND tgmv_flag = 1
group by 1,2,3,4,5
) with data primary index (sit_site_id,b2b_id) ;


----------------------------- 18. Trae datos de Loyalty ------------------------------------------

 CREATE TABLE TEMP_45.LK_LOYALTY AS (
WITH temp_loyalty AS (
SELECT
  coalesce(KYC_IDENTIFICATION_NUMBER,h.cus_cust_id) b2b_id, 
  b.KYC_IDENTIFICATION_NUMBER, -- 3
  b.KYC_ENTITY_TYPE, -- 4
  h.cus_cust_id, 
  h.sit_site_id,
  h.LYL_LEVEL_NUMBER 
FROM  LK_KYC_VAULT_USER b
LEFT JOIN WHOWNER.LK_CUS_CUSTOMERS_DATA e
ON b.sit_site_id=e.sit_site_id_cus AND b.cus_cust_id=e.cus_cust_id
LEFT JOIN whowner.BT_LYL_POINTS_SNAPSHOT h 
ON b.cus_cust_id=h.cus_cust_id AND h.tim_month_id = '202012'
WHERE COALESCE(e.CUS_TAGS, '') <> 'sink' AND b.kyc_entity_type='company'
)

SELECT 
 b2b_id,

max(LYL_LEVEL_NUMBER ) LYL_LEVEL_NUMBER
FROM temp_loyalty
GROUP BY 1
 ) with data primary index (b2b_id) ;

----------------------------- 19. Crea la tabla final ------------------------------------------

----------------------------- Creo la base de cust de empresas  ------------------------------------------
/* Traigo los datos del Vault de KYC marcados como company segun las relglas para el entity type por pais:
https://docs.google.com/presentation/d/1ExEk8mfT-Z9J6pibfZ8WUqegUOJzDYRpObmZLZzGVHs/edit#slide=id.g7735a7c26a_2_3
Y elimino los usuarios sink de la tabla de customers, son usuarios creados para carritos y cosas internas.
*/


DROP TABLE TEMP_45.segmentacion_mla;
CREATE  TABLE TEMP_45.segmentacion_mla  AS (

SELECT
  a.b2b_id, -- 1
  a.SIT_SITE_ID, -- 2
  a.KYC_IDENTIFICATION_NUMBER, -- 3
  a.KYC_ENTITY_TYPE, -- 4
  b.canal_max,
  b.tpv_segment_detail_max,
  b.segmento_final,
  a.customer_final,
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
    WHEN g.ACCOUNT_MONEY ='Menos 1900 Pesos' or  g.ACCOUNT_MONEY = '1900 a 9500 Pesos' THEN 1
    WHEN  g.ACCOUNT_MONEY = '9500 a 28500 Pesos' or  g.ACCOUNT_MONEY = '28500 a 95000 Pesos' THEN 2
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
  when engagement_rank=2 and Agg_Frecuencia= 'Frequent Buyer' then 'Frequent Engaged Buyer'
  when engagement_rank=3 and Agg_Frecuencia= 'Frequent Buyer' then 'Frequent Engaged Buyer'
  else 'No puede ser'
  end as target, 

  
  b.VENTAS_USD VENTAS_USD,
  f.TGMVEBILLABLE  bgmv_cpras


FROM TEMP_45.kyc_customer a
LEFT JOIN temp_45.sell06_doc_mla AS b ON a.b2b_id=b.b2b_id 

LEFT JOIN temp_45.buy01_doc_mla AS f ON a.b2b_id=f.b2b_id 
LEFT JOIN TEMP_45.account_money2_mla g ON a.b2b_id=g.b2b_id
LEFT JOIN TEMP_45.LK_LOYALTY h ON a.b2b_id=h.b2b_id 
LEFT JOIN temp_45.lk_seguros l ON a.b2b_id=l.b2b_id 
LEFT JOIN temp_45.lk_credits m ON a.b2b_id=m.b2b_id
LEFT JOIN temp_45.lk_seller_shipping n ON a.b2b_id=n.b2b_id

LEFT JOIN temp_45.buy03_cust_mla o ON a.b2b_id=o.b2b_id 


WHERE a.sit_site_id = 'MLA' 
AND ((a.KYC_ENTITY_TYPE = 'company' AND (TIPO_COMPRADOR_TGMV<>'No Compra' OR RANGO_VTA_PURO<> 'a.No Vende') )
OR (a.KYC_ENTITY_TYPE <> 'company' AND  b.VENTAS_USD >= 6000)) AND a.KYC_ENTITY_TYPE = 'company'
--GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22, 23, 24,25,26, 27,28, 29, 30, 31, 32, 33






)  with data primary index (sit_site_id,b2b_id);



