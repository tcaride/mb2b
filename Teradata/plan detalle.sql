CREATE multiset volatile TABLE br_mx as (
SELECT
  cus_cust_id,
  sit_site_id,
  cus_tax_payer_type,
  cus_tax_regime
FROM LK_TAX_CUST_WRAPPER
WHERE SIT_SITE_ID IN ('MLB')
qualify row_number () over (partition by cus_cust_id, sit_site_id order by  aud_upd_dt DESC) = 1
) with data primary index (sit_site_id,cus_cust_id) on commit preserve rows;

--drop table base_cust;
CREATE multiset volatile TABLE BASE_CUST as (
SELECT
distinct
    d.cus_cust_id,
    d.sit_site_id_CUS,
    (CAST( d.cus_first_buy As DATE)/100+190000) YEAR_MONTH_buy,
     
    case when (CAST( d.cus_first_sell As DATE)/100+190000)>(CAST( d.cus_mp_first_sale_off_ml As DATE)/100+190000) then (CAST( d.cus_mp_first_sale_off_ml As DATE)/100+190000) else (CAST( d.cus_first_sell As DATE)/100+190000) end as YEAR_MONTH_sel,
     
    CASE WHEN d.cus_internal_tags like '%internal_user%' or d.cus_internal_tags like '%internal_third_party%' then 'MELI-1P/PL'
        WHEN d.cus_internal_tags like '%cancelled_account%' then 'Cuenta_ELIMINADA'
        WHEN d.cus_internal_tags like '%operators_root%' then 'Operador_Root'
        WHEN d.cus_internal_tags like '%operator%' then 'Operador'
        ELSE 'OK' 
    END as CUSTOMER,
    
    case when KYC.KYC_ENTITY_TYPE='company' then 'Company' else 'Not Company' end as CUS_TYPE, -- tipo documento
    tax.cus_tax_payer_type, 
    
/*CASE WHEN KYC.KYC_ENTITY_TYPE='company' then 'ok' 
    when KYC.KYC_ENTITY_TYPE<>'company' and RANGO_VTA_PURO  not in ('a.No Vende','b.Menos 6.000')  then 'ok' 
    else 'not ok'  end as Baseline,
    
CASE WHEN S.tpv_segment_detail ='Aggregator - Other' then 'Online Payments' 
WHEN S.tpv_segment_detail ='Checkout OFF' then 'Online Payments' 
when S.tpv_segment_detail ='Instore' then 'QR'
when S.tpv_segment_detail ='Selling ML' then 'Selling ML'
when S.tpv_segment_detail ='Point' then 'Point'
when S.tpv_segment_detail is null then 'No Vende'
else 'Not Considered'end as Canal_Año,*/

KYC.kyc_comp_corporate_name,
kyc.KYC_IDENTIFICATION_NUMBER,


    COUNT(*) cuenta

FROM WHOWNER.LK_CUS_CUSTOMERS_DATA D -- base de clientes
LEFT JOIN WHOWNER.LK_KYC_VAULT_USER KYC ON KYC.CUS_CUST_ID=D.CUS_CUST_ID
LEFT JOIN br_mx tax on tax.CUS_CUST_ID=D.CUS_CUST_ID and tax.sit_site_id=d.sit_site_id_cus
--LEFT JOIN SEG_AÑO s on d.CUS_CUST_ID=s.CUS_CUST_ID_sel and s.sit_site_id=d.sit_site_id_cus
--LEFT JOIN RANGO_AÑO R on d.CUS_CUST_ID=R.CUS_CUST_ID_sel and R.sit_site_id=d.sit_site_id_cus

WHERE COALESCE(D.CUS_TAGS,'') <> 'sink'
AND D.sit_site_id_cus  IN ('MLB')


group by 1,2,3,4,5,6,7,8,9

--having CUSTOMER not in ('Operador','MELI-1P/PL')
) with data primary index (cus_cust_id,SIT_SITE_ID_cus) on commit preserve rows;





CREATE multiset volatile TABLE BASE_CUST_1C as (
SELECT
distinct
  KYC_IDENTIFICATION_NUMBER,
     sit_site_id_CUS,
    MIN (YEAR_MONTH_buy) YEAR_MONTH_buy

FROM base_cust-- base de clientes

group by 1,2
) with data primary index (KYC_IDENTIFICATION_NUMBER,SIT_SITE_ID_cus) on commit preserve rows;

-------


CREATE MULTISET VOLATILE TABLE BGMV_BUY_MONTH AS (
select
    BID.cus_cust_id_buy,  
       bid.sit_site_id,
       (CAST( bid.TIM_DAY_WINNING_DATE As DATE)/100+190000) YEAR_MONTH,
        --EXTRACT (YEAR from bid.TIM_DAY_WINNING_DATE)  AÑO,
        --(CAST(EXTRACT(YEAR FROM BID.tim_day_winning_date) AS BYTEINT) YEAR,
       --  ((CAST(EXTRACT(MONTH FROM bid.TIM_DAY_WINNING_DATE) AS BYTEINT)-1)/3)+1
--  || 'Q' || substring(bid.TIM_DAY_WINNING_DATE,3,2) quarter,
    --(CASE WHEN EXTRACT(MONTH FROM BID.TIM_DAY_WINNING_DATE)<=6 THEN 'H1' else 'H2' END || EXTRACT(YEAR FROM BID.TIM_DAY_WINNING_DATE))  HALF,
      
    sum((Case when  tgmv_flag = 1 then (bid.BID_BASE_CURRENT_PRICE * bid.BID_QUANTITY_OK) else 0.0 end))  TGMVE_BUY,
     sum((Case when  tgmv_flag = 1 then (bid.BID_SITE_CURRENT_PRICE * bid.BID_QUANTITY_OK) else 0.0 end))  TGMVE_BUY_LC,
  COUNT((Case when  tgmv_flag = 1 then BID.BID_QUANTITY_OK else 0.0 end))  TORDERS_BUY,
   sum((Case when  tgmv_flag = 1 then BID.BID_QUANTITY_OK else 0.0 end))  TSIE_BUY,
  COUNT(distinct (case when bid.crt_purchase_id is null then bid.ord_order_id else bid.crt_purchase_id end)) as TX_BUY
   
from WHOWNER.BT_BIDS as bid
where bid.sit_site_id  IN ('MLB')
and bid.photo_id = 'TODATE' 
and bid.tim_day_winning_date between date '2018-01-01' and date '2021-09-30'
and bid.ite_gmv_flag = 1
and bid.mkt_marketplace_id = 'TM'
and tgmv_flag = 1
--AND coalesce(BID.AUTO_OFFER_FLAG, 0) <> 1 
--AND coalesce(bid.BID_FVF_BONIF, 'Y') = 'N'
group by 1,2,3

)WITH DATA PRIMARY INDEX (sit_site_id,cus_cust_id_buy,YEAR_MONTH) ON COMMIT PRESERVE ROWS;



CREATE MULTISET VOLATILE TABLE TPV_SEL_3 AS (

select
(CAST( MP.PAY_MOVE_DATE As DATE)/100+190000) YEAR_MONTH,
--EXTRACT (YEAR from  MP.PAY_MOVE_DATE) AÑO,
--((CAST(EXTRACT(MONTH FROM MP.PAY_MOVE_DATE) AS BYTEINT)-1)/3)+1
-- || 'Q' || substring(MP.PAY_MOVE_DATE,3,2) quarter,
  --(CASE WHEN EXTRACT(MONTH FROM MP.PAY_MOVE_DATE)<=6 THEN 'H1' else 'H2' END || EXTRACT(YEAR FROM MP.PAY_MOVE_DATE))  HALF,
mp.cus_cust_id_sel cus_cust_id_sel,

mp.sit_site_id sit_site_id,

tpv_segment_id tpv_segment_id,
tpv_segment_detail tpv_segment_detail,

sum (PAY_TRANSACTION_DOL_AMT) VENTAS_USD

from WHOWNER.BT_MP_PAY_PAYMENTS mp
where mp.tpv_flag = 1
and 
mp.sit_site_id  IN ('MLB')
and MP.PAY_MOVE_DATE  between date '2018-01-01' and date '2021-09-30'
and mp.pay_status_id in ( 'approved')--, 'authorized')
and tpv_segment_id <> 'ON'--AND coalesce(i.ITE_TIPO_PROD,0) <> 'U'
--and mp.cus_cust_id_buy not in (185198438)
group by 1,2,3,4,5

Union

select
(CAST( bid.TIM_DAY_WINNING_DATE As DATE)/100+190000) YEAR_MONTH,
--EXTRACT (YEAR from bid.TIM_DAY_WINNING_DATE) AÑO,
  -- ((CAST(EXTRACT(MONTH FROM bid.TIM_DAY_WINNING_DATE) AS BYTEINT)-1)/3)+1
-- || 'Q' || substring(bid.TIM_DAY_WINNING_DATE,3,2) quarter,
  --(CASE WHEN EXTRACT(MONTH FROM BID.TIM_DAY_WINNING_DATE)<=6 THEN 'H1' else 'H2' END || EXTRACT(YEAR FROM BID.TIM_DAY_WINNING_DATE))  HALF,
   BID.cus_cust_id_sel cus_cust_id_sel,  
       bid.sit_site_id sit_site_id,
       'Selling ML' tpv_segment_id,
       'Selling ML' tpv_segment_detail,
 sum((Case when coalesce(bid.BID_FVF_BONIF, 'Y') = 'N' then (bid.BID_BASE_CURRENT_PRICE * bid.BID_QUANTITY_OK) else 0.0 end))  VENTAS_USD
from WHOWNER.BT_BIDS as bid
where bid.sit_site_id  IN ('MLB')
and bid.photo_id = 'TODATE' 
and bid.tim_day_winning_date between date '2018-01-01' and date '2021-09-30'
and bid.ite_gmv_flag = 1
and bid.mkt_marketplace_id = 'TM'
AND coalesce(BID.AUTO_OFFER_FLAG, 0) <> 1 
AND coalesce(bid.BID_FVF_BONIF, 'Y') = 'N'
--AND coalesce(bid.BID_FVF_BONIF, 'Y') = 'N'
group by 1,2,3,4,5


)WITH DATA PRIMARY INDEX (sit_site_id,cus_cust_id_sel,YEAR_MONTH ) ON COMMIT PRESERVE ROWS;


create multiset volatile table VENTAS_USD as (

select
YEAR_MONTH,
--AÑO,
--quarter,
--HALF,
cus_cust_id_sel,
sit_site_id,
sum(VENTAS_USD) VENTAS_USD
from TPV_SEL_3
GROUP BY 1,2,3
) with data primary index (CUS_CUST_ID_SEL,SIT_SITE_ID,YEAR_MONTH ) on commit preserve rows;




create multiset volatile table SEG_SEL as ( --- me quedo con el segmento del seller con mas ventas --- segmento por seller
select
SP.cus_cust_id_sel, 
SP.sit_site_id,
--SP.quarter,
sp.year_month,
SP.tpv_segment_id,
SP.tpv_segment_detail,
--sm.SEGMENTO,

VENTAS_USD

from TPV_SEL_3 sp
--Left join WHOWNER.LK_SEGMENTO_SELLERS  SM on SP.sit_site_id=sM.sit_site_id and SP.cus_cust_id_sel=sM.cus_cust_id_sel
qualify row_number () over (partition by sp.cus_cust_id_sel, sp.sit_site_id,SP.YEAR_MONTH order by VENTAS_USD DESC) = 1
group by 1,2,3,4,5,6

) with data primary index (CUS_CUST_ID_SEL,SIT_SITE_ID,YEAR_MONTH ) on commit preserve rows;




-----------------------------------
-----------GENERAL PLAN------------
-----------------------------------

SELECT

case when a1.sit_site_id is null then b1.sit_site_id else a1.sit_site_id  end as sit_site_id,
case when a1.YEAR_MONTH is null then b1.year_month else a1.year_month end as year_month,
--case when a1.AÑO is null then b1.AÑO else a1.año end as AÑO,
--case when a1.quarter is null then b1.quarter else a1.quarter end as quarter,
CASE WHEN B1.YEAR_MONTH  IS NULL THEN 'No Compra M' else 'Compra M' end as compra_M,
case when b1.year_month=c_1.YEAR_MONTH_BUY then 'NB' when B1.YEAR_MONTH is null then 'S'  else 'RB' end as tipo_comprador,
--case when c1.YEAR_MONTH_BUY < 202001 then '2019 o menos' when c1.YEAR_MONTH_BUY < 202101 then '2020' else '2021' end as year_buy,
case when c_1.YEAR_MONTH_BUY<201801 then 'Previo 2018'
else c_1.YEAR_MONTH_BUY end YEAR_MONTH_BUY
,
--SUBSTRING (c1.YEAR_MONTH_BUY FROM 1 FOR 4) year_fist_buy,
--case when c1.YEAR_MONTH_sel is null then 'No Vendio' else 'Vendio' end as Alguna_Vez_Vendio,

c1.CUS_TYPE,
case when CUS_TYPE='Company' then c1.cus_tax_payer_type else 'Not Company' end as tax_type,
--c1.CUSTOMER,
--C1.Baseline,
--c1.CANAL_AÑO, -- el canal prioritario del ultimo año 
--C1.year_month,
--C1.cus_tax_payer_type,
CASE WHEN S.tpv_segment_detail ='Aggregator - Other' then 'Online Payments' 
WHEN S.tpv_segment_detail ='Checkout OFF' then 'Online Payments' 
when S.tpv_segment_detail ='Instore' then 'QR'
when S.tpv_segment_detail ='Selling ML' then 'Selling ML'
when S.tpv_segment_detail ='Point' then 'Point'
when S.tpv_segment_detail is null then 'No Vende'
else 'Not Considered'end as Canal, --- canal del mes 

--CASE WHEN C1.YEAR_MONTH_SEL IS NULL THEN 'No vendio nunca'
--else 'Vendio Alguna Vez' end as VENTAS_CHECK,



count(distinct A1.cus_cust_id_sel) seller,
count(distinct b1.cus_cust_id_buy) buy,
count(distinct case when b1.cus_cust_id_buy is null then  A1.cus_cust_id_sel else  b1.cus_cust_id_buy end) total,
count(distinct case when A1.cus_cust_id_sel is null then 0 else  c1.KYC_IDENTIFICATION_NUMBER end) usuarios_sel,
count(distinct case when b1.cus_cust_id_buy  is null then 0 else c1.KYC_IDENTIFICATION_NUMBER end) usuarios_buy,
count(distinct case when b1.cus_cust_id_buy is null and  A1.cus_cust_id_sel  is null then 0 else c1.KYC_IDENTIFICATION_NUMBER end) usuarios_totales,
SUM(A1.ventas_usd) ventas_usd,
SUM(B1.TGMVe_BUY) TGMVe_BUY,
SUM(B1.TGMVe_BUY_lc) TGMVe_BUY_LC,
SUM(B1.TORDERS_BUY) TORDERS_BUY,
SUM(B1.TSIE_BUY) TSIE_BUY,
sum(B1.TX_BUY)  TX_BUY

FROM ventas_usd a1

full OUTER JOIN BGMV_BUY_MONTH b1 on a1.sit_site_id=b1.sit_site_id and a1.cus_cust_id_sel=b1.cus_cust_id_buy and  a1.YEAR_MONTH =b1.YEAR_MONTH  --and  a1.year_month=b1.year_month

LEFT JOIN BASE_CUST C1 ON COALESCE(a1.sit_site_id,b1.sit_site_id)=C1.sit_site_id_cus and COALESCE(a1.cus_cust_id_sel,b1.cus_cust_id_buy)=C1.cus_cust_id

LEFT JOIN SEG_SEL S on a1.sit_site_id=s.sit_site_id and a1.cus_cust_id_sel=s.cus_cust_id_SEL and  a1.YEAR_MONTH =s.YEAR_MONTH 

---LEFT JOIN BGMV_INCR inc ON COALESCE(a1.sit_site_id,b1.sit_site_id)=inc.sit_site_id and COALESCE(a1.cus_cust_id_sel,b1.cus_cust_id_buy)=inc.cus_cust_id_buy

left join BASE_CUST_1C c_1 on c_1.sit_site_id_cus=C1.sit_site_id_cus and c1.KYC_IDENTIFICATION_NUMBER =c_1.KYC_IDENTIFICATION_NUMBER 

group by 1,2,3,4,5,6,7,8

--having Canal='No Vende' and CUS_TYPE='Company'
;
