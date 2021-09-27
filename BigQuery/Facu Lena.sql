/* Comment Tomas: Traigo todos los datos de Mercado Pago de ventas por seller y canal */
WITH TPV_SEL_1 AS (
SELECT
  mp.cus_cust_id_sel cus_cust_id_sel, --- numero de usuario
  mp.sit_site_id sit_site_id, --- site
  tpv_segment tpv_segment_id, --- Segmento de donde vende
  tpv_segment_detail tpv_segment_detail, --- + detalle
  SUM (PAY_TRANSACTION_DOL_AMT) VENTAS_USD, --- suma volumen de ventas
  SUM (1) Q -- para contar la cantidad de segmentos que se vende   COUNT (tpv_segment_id) Q2
FROM WHOWNER.BT_MP_PAY_PAYMENTS mp
WHERE mp.tpv_flag = 1
AND MP.PAY_MOVE_DATE BETWEEN DATE '2020-09-01' AND DATE '2021-08-31'
AND mp.pay_status_code IN ( 'approved')--, 'authorized')
AND tpv_segment <> 'ON'--AND coalesce(i.ITE_TIPO_PROD,0) <> 'U' -----> Saco todo lo que es Marketplace
GROUP BY 1,2,3,4

UNION ALL--- para poder hacer union las dos tablas son iguales

/* Comment Tomas: Traigo todos los datos de Marketplace */

SELECT
  bid.ORD_SELLER.ID cus_cust_id_sel,  
  bid.sit_site_id sit_site_id,
  'Selling Marketplace' tpv_segment,
  'Selling Marketplace' tpv_segment_detail,
  SUM((
    CASE WHEN coalesce(bid.ORD_FVF_BONIF, True) = False 
    THEN (
    bid.ORD_ITEM.BASE_CURRENT_PRICE * 
bid.ORD_ITEM.QTY) ELSE 0.0 END))  VENTAS_USD,
  SUM (1) Q
FROM WHOWNER.BT_ORD_ORDERS as bid
where 
 bid.ORD_CLOSED_DT BETWEEN DATE '2020-09-01' AND DATE '2021-08-31'
AND bid.ORD_GMV_FLG = True
AND bid.ORD_CATEGORY.MARKETPLACE_ID = 'TM'
AND coalesce(BID.ORD_AUTO_OFFER_FLG, False) <> True
AND coalesce(bid.ORD_FVF_BONIF, True) = False
GROUP BY 1,2,3,4
),

----------------------------- 03. Query para tener el segmento de MP, tomo el mayor puede que haya seller en mas de un segmento -----------------------------------------


TPV_SEL as ( 
select 
  --tpv.cus_cust_id_sel,
  tpv.sit_site_id,
  kyc.kyc_entity_type,
  kyc.KYC_IDENTIFICATION_NUMBER,
  e.cus_party_type_id customer,
  /*CASE WHEN e.cus_internal_tags LIKE '%internal_user%' OR e.cus_internal_tags LIKE '%internal_third_party%' THEN 'MELI-1P/PL'
    WHEN e.cus_internal_tags LIKE '%cancelled_account%' THEN 'Cuenta_ELIMINADA'
    WHEN e.cus_internal_tags LIKE '%operators_root%' THEN 'Operador_Root'
    WHEN e.cus_internal_tags LIKE '%operator%' THEN 'Operador'
    ELSE 'OK' 
  END AS CUSTOMER, -- 9*/
  count(distinct tpv.cus_cust_id_sel) count_cust_id,
  sum(VENTAS_USD) VENTAS_USD
from TPV_SEL_1 tpv

LEFT JOIN WHOWNER.LK_KYC_VAULT_USER  kyc
on tpv.sit_site_id=kyc.sit_site_id AND tpv.cus_cust_id_sel=kyc.cus_cust_id
LEFT JOIN WHOWNER.LK_CUS_CUSTOMERS_DATA e ON  tpv.cus_cust_id_sel=e.cus_cust_id
--WHERE kyc_entity_type = 'company'
group by 1,2,3,4
),

SEG_SEL as ( 
SELECT
  --tpv1.cus_cust_id_sel,
  tpv1.sit_site_id,
  kyc.KYC_IDENTIFICATION_NUMBER,
  tpv1.tpv_segment_id,
  tpv1.tpv_segment_detail,
  sum(VENTAS_USD) ventas_usd
FROM TPV_SEL_1 tpv1
LEFT JOIN WHOWNER.LK_KYC_VAULT_USER  kyc
on tpv1.sit_site_id=kyc.sit_site_id AND tpv1.cus_cust_id_sel=kyc.cus_cust_id
GROUP BY 1,2,3,4
qualify row_number () over (partition by kyc.KYC_IDENTIFICATION_NUMBER, tpv1.sit_site_id order by VENTAS_USD DESC) = 1
),

final as(
SELECT
  case when a.KYC_IDENTIFICATION_NUMBER is null then a.count_cust_id else a.KYC_IDENTIFICATION_NUMBER
  end as IDENTIFICATION_NUMBER

  ,
  a.count_cust_id,
  a.sit_site_id, 
  a.kyc_entity_type,
  a.customer,
  CASE WHEN b.tpv_segment_detail ='Aggregator - Other' then 'Online Payments' 
  WHEN b.tpv_segment_detail ='Checkout OFF' then 'Online Payments' 
  when b.tpv_segment_detail ='Instore' then 'QR'
  when b.tpv_segment_detail ='Selling Marketplace' then 'Selling Marketplace'
  when b.tpv_segment_detail ='Point' then 'Point'
  when b.tpv_segment_detail is null then 'No Vende'
  else 'Not Considered'end as Canal,
  CASE WHEN b.tpv_segment_detail ='Aggregator - Other' then 'ON' 
  WHEN b.tpv_segment_detail ='Checkout OFF' then 'ON' 
  when b.tpv_segment_detail ='Instore' then 'OF'
  when b.tpv_segment_detail ='Selling Marketplace' then 'ON'
  when b.tpv_segment_detail ='Point' then 'OF'
  when b.tpv_segment_detail is null then 'No Vende'
  else 'Not Considered'end as Agg_Canal,
 CASE WHEN a.VENTAS_USD IS null THEN 'a.No Vende'
  WHEN a.VENTAS_USD= 0 THEN 'a.No Vende'
  WHEN a.VENTAS_USD <= 6000 THEN 'b.Menos 6.000'
  WHEN a.VENTAS_USD <= 40000 THEN 'c.6.000 a 40.000'
  WHEN a.VENTAS_USD<= 200000 THEN 'd.40.000 a 200.000'
  ELSE 'e.Mas de 200.000' 
  END AS RANGO_VTA_PURO,
  a.VENTAS_USD
FROM TPV_SEL a
left join SEG_SEL b
on a.KYC_IDENTIFICATION_NUMBER = b.KYC_IDENTIFICATION_NUMBER
)

SELECT  
sit_site_id, 
kyc_entity_type,
RANGO_VTA_PURO, 
Agg_Canal, -- 5
count(IDENTIFICATION_NUMBER) count_docs,
sum(count_cust_id) count_cust_ids,
sum(VENTAS_USD)

FROM final
Where Agg_Canal <> 'Not Considered' and RANGO_VTA_PURO <> 'a.No Vende' and customer='3P' 
Group by sit_site_id, kyc_entity_type, Agg_Canal, RANGO_VTA_PURO
order by sit_site_id, kyc_entity_type, Agg_Canal, RANGO_VTA_PURO



