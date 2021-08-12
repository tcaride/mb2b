----------------------------------------------------------------------------
----- QUERY PARA CREAR LA BASE // PASAR A TABLA DEL ESQUEMA CUANDO ESTE ----
----------------------------------------------------------------------------

-- Esta es solo la tabla para armar el baseline y a todo cliente diferenciarlo como B2B Company o B2B Not Company o B2C // la idea es que esto pase a una tabla 

----------------------------- 00. Uno datos Mercado Pago y Marketplace  ------------------------------------------

/* Comment Tomas: Traigo todos los datos de Mercado Pago de ventas por seller y canal */

CREATE multiset volatile TABLE TPV_SEL_1 as (

SELECT
    mp.cus_cust_id_sel cus_cust_id_sel, --- numero de usuario
    mp.sit_site_id sit_site_id, --- site
    tpv_segment_id tpv_segment_id, --- Segmento de donde vende
    tpv_segment_detail tpv_segment_detail, --- + detalle
    sum (PAY_TRANSACTION_DOL_AMT) VENTAS_USD, --- suma volumen de ventas
    sum (1) Q -- para contar la cantidad de segmentos que se vende
FROM WHOWNER.BT_MP_PAY_PAYMENTS mp
WHERE mp.tpv_flag = 1
AND mp.sit_site_id IN ('MLB','MLA','MLM','MLC')
AND MP.PAY_MOVE_DATE BETWEEN DATE '2020-08-01' AND DATE '2021-07-31'
AND mp.pay_status_id IN ( 'approved')--, 'authorized')
AND tpv_segment_id <> 'ON'--AND coalesce(i.ITE_TIPO_PROD,0) <> 'U' -----> Saco todo lo que es Marketplace
GROUP BY 1,2,3,4

UNION --- para poder hacer union las dos tablas son iguales

SELECT
    bid.cus_cust_id_sel cus_cust_id_sel,  
    bid.sit_site_id sit_site_id,
    'Selling Marketplace' tpv_segment_id,
    'Selling Marketplace' tpv_segment_detail,
    sum((Case when coalesce(bid.BID_FVF_BONIF, 'Y') = 'N' then (bid.BID_BASE_CURRENT_PRICE * bid.BID_QUANTITY_OK) else 0.0 end))  VENTAS_USD,
    sum (1) Q
FROM WHOWNER.BT_BIDS as bid
WHERE bid.sit_site_id IN ('MLB','MLA','MLM','MLC')
AND bid.photo_id = 'TODATE' 
AND bid.tim_day_winning_date BETWEEN DATE '2020-08-01' AND DATE '2021-07-31'
AND bid.ite_gmv_flag = 1
AND bid.mkt_marketplace_id = 'TM'
AND coalesce(BID.AUTO_OFFER_FLAG, 0) <> 1 
AND coalesce(bid.BID_FVF_BONIF, 'Y') = 'N'
group by 1,2,3,4

) with data primary index (CUS_CUST_ID_SEL,SIT_SITE_ID) on commit preserve rows;

----------------------------- 01. Agrego Volumen de ventas por seller.  ------------------------------------------

CREATE multiset volatile TABLE TPV_SEL as ( --- sumo los volumenes por seller --- volumen total por seller

SELECT
  cus_cust_id_sel,
  sit_site_id,
  SUM(VENTAS_USD) VENTAS_USD,
  SUM(Q) Q,
  COUNT(distinct tpv_segment_id) Q_SEG
FROM TPV_SEL_1
GROUP BY 1,2
) with data primary index (CUS_CUST_ID_SEL,SIT_SITE_ID) on commit preserve rows;

----------------------------- 02. Query para tener el segmento de MP, tomo el mayor puede que haya seller en mas de un segmento -----------------------------------------

CREATE multiset volatile TABLE SEG_SEL as ( --- me quedo con el segmento del seller con mas ventas --- segmento por seller
SELECT 
  SP.cus_cust_id_sel, 
  SP.sit_site_id,
  SP.tpv_segment_id,
  SP.tpv_segment_detail,
  sm.SEGMENTO,
  VENTAS_USD
FROM TPV_SEL_1 sp
LEFT JOIN WHOWNER.LK_SEGMENTO_SELLERS  SM 
on SP.sit_site_id=sM.sit_site_id AND SP.cus_cust_id_sel=sM.cus_cust_id_sel
qualify row_number () over (partition by sp.cus_cust_id_sel, sp.sit_site_id order by VENTAS_USD DESC) = 1
GROUP BY 1,2,3,4,5,6
) with data primary index (CUS_CUST_ID_SEL,SIT_SITE_ID) on commit preserve rows;

---- query para tener el TPV compras

/*create multiset volatile table TPV_BUY as ( ---no se usa
select
cus_cust_id_buy, 
sit_site_id,

tpv_segment_id,
tpv_segment_detail,
--PAY_MOVE_DATE TIM_DAY,

--sum (TPV_DOL_AMT) TPV_DOL_AMT_BUY,
sum (PAY_TRANSACTION_DOL_AMT) TPV_PURO_DOL_AMT_BUY
--sum (TPV_AMT) TPV_AMT,
--sum (1) TPN,

from WHOWNER.BT_MP_PAY_PAYMENTS

where tpv_flag = 1
and sit_site_id IN ('MLB','MLA','MLM')
and PAY_MOVE_DATE between date '2020-01-01' and date '2020-12-31'
and pay_status_id = 'approved'
and tpv_segment_id = 'ON'
group by 1,2,3,4

) with data primary index (CUS_CUST_ID_buy,SIT_SITE_ID) on commit preserve rows;
*/
----------------------------- 03. Agrego compras por seller ------------------------------------------

CREATE MULTISET VOLATILE TABLE BGMV_BUY AS ( --- compras en el marketplace por usuario 
SELECT
    bid.cus_cust_id_buy,  
    bid.sit_site_id,
    SUM((Case when tgmv_flag = 1 then (bid.BID_BASE_CURRENT_PRICE * bid.BID_QUANTITY_OK) else 0.0 end))  GMVEBILLABLE,
    COUNT((Case when  tgmv_flag = 1 then BID.BID_QUANTITY_OK else 0.0 end))  TORDERS_BUY,
    SUM((Case when  tgmv_flag = 1 then BID.BID_QUANTITY_OK else 0.0 end))  TSIE_BUY,
    COUNT(distinct (case when bid.crt_purchase_id is null then bid.ord_order_id else bid.crt_purchase_id end)) as TX_BUY
FROM WHOWNER.BT_BIDS as bid
WHERE bid.sit_site_id IN ('MLB','MLA','MLM','MLC')
AND bid.photo_id = 'TODATE' 
AND bid.tim_day_winning_date BETWEEN DATE '2020-08-01' AND DATE '2021-07-31'
AND bid.ite_gmv_flag = 1
AND bid.mkt_marketplace_id = 'TM'
AND tgmv_flag = 1
GROUP BY 1,2

)WITH DATA PRIMARY INDEX (sit_site_id,cus_cust_id_buy) ON COMMIT PRESERVE ROWS;

----------------------------- 04. Traigo el regimen fiscal del pais ------------------------------------------

CREATE multiset volatile TABLE br_mx as (
SELECT
  cus_cust_id,
  sit_site_id,
  cus_tax_payer_type,
  cus_tax_regime
FROM LK_TAX_CUST_WRAPPER
WHERE SIT_SITE_ID IN ('MLA')
qualify row_number () over (partition by cus_cust_id, sit_site_id order by  aud_upd_dt DESC) = 1
) with data primary index (sit_site_id,cus_cust_id) on commit preserve rows;

----------------------------- 05. Crea la tabla company/not company para filtrar cus_id ---------------------------------------

CREATE multiset volatile TABLE BASE_CUST as (
SELECT
    d.cus_cust_id,
    d.sit_site_id_CUS,
    CASE WHEN d.cus_internal_tags like '%internal_user%' or d.cus_internal_tags like '%internal_third_party%' then 'MELI-1P/PL'
        WHEN d.cus_internal_tags like '%cancelled_account%' then 'Cuenta_ELIMINADA'
        WHEN d.cus_internal_tags like '%operators_root%' then 'Operador_Root'
        WHEN d.cus_internal_tags like '%operator%' then 'Operador'
        ELSE 'OK' 
    END as CUSTOMER,

    (CASE WHEN  (d.sit_site_id_cus='MLA') THEN
            CASE WHEN R.REG_CUST_DOC_NUMBER IS NOT NULL and R.REG_CUST_DOC_TYPE IN ('CUIL','CUIT') and left(R.REG_CUST_DOC_NUMBER,1)='3' THEN 'Company'
                WHEN R1.REG_DATA_TYPE='company' then 'Company' 
                WHEN R1.REG_DATA_TYPE IS NULL AND R.REG_CUST_DOC_NUMBER IS NOT NULL and R.REG_CUST_DOC_TYPE IN ('CUIL','CUIT') and left(R.REG_CUST_DOC_NUMBER,1)='3' THEN 'Company'
                WHEN D.cus_cust_doc_type IN ('CUIL','CUIT') and (left(D.cus_cust_doc_number,1)='3'  OR left(D.cus_cust_doc_number,1)='55') THEN 'Company'
                WHEN KYC.cus_doc_type IN ('CUIL','CUIT') and left(KYC.cus_doc_number,1)='3' THEN 'Company'
                WHEN KYC2.CUS_KYC_ENTITY_TYPE IN('company') THEN 'Company'
                ELSE 'Not Company' 
            END
        WHEN (d.sit_site_id_cus='MLB') THEN
            CASE WHEN R1.REG_DATA_TYPE='company' then 'Company' 
                WHEN R1.reg_cust_doc_type In ('CNPJ') THEN 'Company'
                WHEN D.cus_cust_doc_type In ('CNPJ') THEN 'Company'
                WHEN KYC.cus_doc_type In ('CNPJ') THEN 'Company'
                WHEN KYC2.CUS_KYC_ENTITY_TYPE IN('company') THEN 'Company'
                ELSE 'Not Company'
            END
        WHEN  (d.sit_site_id_cus='MLM') THEN
            CASE WHEN D.cus_cust_doc_type In ('RFC') and length( D.cus_cust_doc_number) = 12  THEN 'Company'
                WHEN KYC.cus_doc_type In ('RFC') and (length( KYC.cus_doc_number)= 12 or length( KYC.CUS_BUSINESS_DOC)= 12)   THEN 'Company'
                WHEN KYC2.CUS_KYC_ENTITY_TYPE IN('company') THEN 'Company'
                ELSE 'Not Company'
            END        
         WHEN  (d.sit_site_id_cus='MLC') THEN
            CASE WHEN KYC2.CUS_KYC_ENTITY_TYPE IN('company') THEN 'Company'
               WHEN R1.REG_DATA_TYPE='company' then 'Company' 
               WHEN cast(regexp_substr(regexp_replace(D.cus_cust_doc_number,'[.$+*/&¿?! ]'),'^[0-9]+') AS bigint) between 50000000 and 9999999 THEN 'Company'
               ELSE 'Not Company'
            END
        ELSE 'ERROR_SITE'
    END) AS REG_DATA_TYPE_group, -- tipo documento
    tax.cus_tax_payer_type, 
    COUNT(*) cuenta

FROM WHOWNER.LK_CUS_CUSTOMERS_DATA D  -- base de clientes
LEFT JOIN WHOWNER.LK_REG_CUSTOMERS R ON d.CUS_CUST_ID=R.CUS_CUST_ID and d.sit_site_id_cus=r.sit_site_id
LEFT JOIN WHOWNER.LK_REG_PERSON R1 ON R.REG_CUST_DOC_TYPE = R1.REG_CUST_DOC_TYPE and R.REG_CUST_DOC_NUMBER = R1.REG_CUST_DOC_NUMBER and r1.sit_site_id=r.sit_site_id
LEFT JOIN WHOWNER.LK_KYC_CUSTOMERS KYC ON KYC.CUS_CUST_ID=D.CUS_CUST_ID
LEFT JOIN br_mx tax on tax.CUS_CUST_ID=D.CUS_CUST_ID and tax.sit_site_id=d.sit_site_id_cus

WHERE COALESCE(D.CUS_TAGS, '') <> 'sink'
AND D.sit_site_id_cus IN ('MLB','MLA','MLM','MLC')

group by 1,2,3,4,5
) with data primary index (cus_cust_id,SIT_SITE_ID_cus) on commit preserve rows;











create multiset volatile table BASE as (

select
--v.cus_cust_id_sel,
d.sit_site_id_CUS,
d.cus_cust_id,
--v.Q_SEG,
--s.tpv_segment_id,
CASE WHEN S.tpv_segment_detail ='Aggregator - Other' then 'Online Payments' 
when S.tpv_segment_detail ='Instore' then 'QR'
when S.tpv_segment_detail ='Selling ML' then 'Selling ML'
when S.tpv_segment_detail ='Point' then 'Point'
when S.tpv_segment_detail is null then 'No Vende'
else 'Not Considered'end as Canal, -- cambio el nombre de segmento

d.CUSTOMER,

d.REG_DATA_TYPE_group, --- tipo documento

CASE when BB.GMVEBILLABLE IS NULL THEN 'No Compra' else 'Compra' end  TIPO_COMPRADOR_TGMV, 


 case 
     when v.VENTAS_USD is null then 'a.No Vende'
     when v.VENTAS_USD= 0 then 'a.No Vende'
     when v.VENTAS_USD <= 6000 then 'b.Menos 6.000'
     when v.VENTAS_USD <= 40000 then 'c.6.000 a 40.000'
     when v.VENTAS_USD<= 200000 then 'd.40.000 a 200.000'
     else 'e.Mas de 200.000' end  RANGO_VTA_PURO, 

     
Case when REG_DATA_TYPE_group='Company' and Canal<>'Not Considered' then 'ok'
 when REG_DATA_TYPE_group <>'Company' and Canal<>'Not Considered' and RANGO_VTA_PURO not in ('a.No Vende','b.Menos 6.000')  then 'ok'
 else 'no ok'end as Baseline,
     
     
count(distinct v.cus_cust_id_sel) cust_sel,
count(distinct b.cus_cust_id_buy)cust_buy_tpv,
count(distinct bb.cus_cust_id_buy)cust_buy_bgmv,
sum(v.VENTAS_USD) VENTAS_USD,
sum(s.VENTAS_USD) VENTAS_USD_SEG,
--sum(b.tpv_puro_dol_amt_buy) tpv_cpras,
SUM (BB.GMVEBILLABLE)  bgmv_cpras


FROM BASE_CUST D  --- base de clientes
left JOIN TPV_SEL AS V ON v.CUS_CUST_ID_SEL=D.CUS_CUST_ID and v.sit_site_id=d.sit_site_id_cus --AND COALESCE(D.CUS_TAGS, '') <> 'sink'--and bid.sit_site_id=d.sit_site_id
LEFT JOIN TPV_BUY AS b on d.cus_cust_id=b.cus_cust_id_buy and d.sit_site_id_cus=b.sit_site_id
LEFT JOIN BGMV_BUY AS BB on d.cus_cust_id=bB.cus_cust_id_buy and d.sit_site_id_cus=bB.sit_site_id --and 
left join Seg_Sel  as s on v.sit_site_id=s.sit_site_id and v.cus_cust_id_sel=s.cus_cust_id_sel
where --COALESCE(D.CUS_TAGS, '') <> 'sink'
D.sit_site_id_cus IN ('MLB','MLA','MLM','MLC')
group by 1,2,3,4,5,6,7

) with data primary index (cus_cust_id,SIT_SITE_ID_cus) on commit preserve rows;



--------------------------------
----- COMPRA VENTA  MENSUAL ----
--------------------------------

--drop table BGMV_BUY_MONTH 

CREATE MULTISET VOLATILE TABLE BGMV_BUY_MONTH AS (
select
    BID.cus_cust_id_buy,  
       bid.sit_site_id,
      (CAST( bid.TIM_DAY_WINNING_DATE As DATE)/100+190000) YEAR_MONTH,
        --EXTRACT (YEAR from bid.TIM_DAY_WINNING_DATE)  AÑO,
        --(CAST(EXTRACT(YEAR FROM BID.tim_day_winning_date) AS BYTEINT) YEAR,
         -- ((CAST(EXTRACT(MONTH FROM bid.TIM_DAY_WINNING_DATE) AS BYTEINT)-1)/3)+1
 -- || 'Q' || substring(bid.TIM_DAY_WINNING_DATE,3,2) quarter,
  
  CASE WHEN (CAST( cus.cus_first_buy_no_bonif_autoof As DATE)/100+190000)=(CAST( bid.TIM_DAY_WINNING_DATE As DATE)/100+190000) THEN '1' else '0' end as NB,
  
  --CASE WHEN ((FLOOR((2+EXTRACT(MONTH FROM cus.cus_first_buy_no_bonif_autoof))/3)  = FLOOR((2+EXTRACT(MONTH FROM bid.TIM_DAY_WINNING_DATE))/3)) AND (YEAR (cus.cus_first_buy_no_bonif_autoof) = YEAR (bid.TIM_DAY_WINNING_DATE))) THEN 'NEW' ELSE 'OLD' END AS NB,
      
    sum((Case when  tgmv_flag = 1 then (bid.BID_BASE_CURRENT_PRICE * bid.BID_QUANTITY_OK) else 0.0 end))  TGMVE_BUY,
  COUNT((Case when  tgmv_flag = 1 then BID.BID_QUANTITY_OK else 0.0 end))  TORDERS_BUY,
   sum((Case when  tgmv_flag = 1 then BID.BID_QUANTITY_OK else 0.0 end))  TSIE_BUY,
  COUNT(distinct (case when bid.crt_purchase_id is null then bid.ord_order_id else bid.crt_purchase_id end)) as TX_BUY,
  COUNT(distinct CUS_CUST_ID_BUY) as BUY
   
from WHOWNER.BT_BIDS as bid

inner join whowner.lk_cus_customer_dates as cus
on (bid.cus_cust_id_buy=cus.cus_cust_id and bid.sit_site_id=cus.sit_site_id)

where bid.sit_site_id IN ('MLB','MLA','MLM','MLC')
and bid.photo_id = 'TODATE' 
and bid.tim_day_winning_date BETWEEN DATE '2020-01-01' AND DATE '2021-12-31'
and bid.ite_gmv_flag = 1
and bid.mkt_marketplace_id = 'TM'
and tgmv_flag = 1
--AND coalesce(BID.AUTO_OFFER_FLAG, 0) <> 1 
--AND coalesce(bid.BID_FVF_BONIF, 'Y') = 'N'
group by 1,2,3,4

)WITH DATA PRIMARY INDEX (sit_site_id,cus_cust_id_buy) ON COMMIT PRESERVE ROWS;



CREATE MULTISET VOLATILE TABLE TPV_SEL_3 AS (

select
(CAST( MP.PAY_MOVE_DATE As DATE)/100+190000) YEAR_MONTH,
--EXTRACT (YEAR from  MP.PAY_MOVE_DATE) AÑO,
--((CAST(EXTRACT(MONTH FROM MP.PAY_MOVE_DATE) AS BYTEINT)-1)/3)+1
  --|| 'Q' || substring(MP.PAY_MOVE_DATE,3,2) quarter,
mp.cus_cust_id_sel cus_cust_id_sel,

mp.sit_site_id sit_site_id,

tpv_segment_id tpv_segment_id,
tpv_segment_detail tpv_segment_detail,

sum (PAY_TRANSACTION_DOL_AMT) VENTAS_USD

from WHOWNER.BT_MP_PAY_PAYMENTS mp
where mp.tpv_flag = 1
and 
mp.sit_site_id IN ('MLB','MLA','MLM','MLC')
and MP.PAY_MOVE_DATE BETWEEN DATE '2020-01-01' AND DATE '2021-12-31'
and mp.pay_status_id in ( 'approved')--, 'authorized')
and tpv_segment_id <> 'ON'--AND coalesce(i.ITE_TIPO_PROD,0) <> 'U'
--and mp.cus_cust_id_buy not in (185198438)
group by 1,2,3,4,5

Union

select
(CAST( bid.TIM_DAY_WINNING_DATE As DATE)/100+190000) YEAR_MONTH,
--EXTRACT (YEAR from bid.TIM_DAY_WINNING_DATE) AÑO,
  -- ((CAST(EXTRACT(MONTH FROM bid.TIM_DAY_WINNING_DATE) AS BYTEINT)-1)/3)+1
  --|| 'Q' || substring(bid.TIM_DAY_WINNING_DATE,3,2) quarter,
   BID.cus_cust_id_sel cus_cust_id_sel,  
       bid.sit_site_id sit_site_id,
       'Selling ML' tpv_segment_id,
       'Selling ML' tpv_segment_detail,
    sum((Case when  tgmv_flag = 1  then (bid.BID_BASE_CURRENT_PRICE * bid.BID_QUANTITY_OK) else 0.0 end))  VENTAS_USD
from WHOWNER.BT_BIDS as bid
where bid.sit_site_id IN  ('MLB','MLA','MLM','MLC')
and bid.photo_id = 'TODATE' 
and bid.tim_day_winning_date BETWEEN DATE '2020-01-01' AND DATE '2021-12-31'
and bid.ite_gmv_flag = 1
and bid.mkt_marketplace_id = 'TM'
--AND  BID.tgmv_flag = 1 
AND coalesce(bid.BID_FVF_BONIF, 'Y') = 'N'
group by 1,2,3,4,5


)WITH DATA PRIMARY INDEX (sit_site_id,cus_cust_id_sel) ON COMMIT PRESERVE ROWS;


create multiset volatile table VENTAS_USD as (

select
YEAR_MONTH,
--AÑO,
--quarter,
cus_cust_id_sel,
sit_site_id,
sum(VENTAS_USD) VENTAS_USD
from TPV_SEL_3
GROUP BY 1,2,3
) with data primary index (CUS_CUST_ID_SEL,SIT_SITE_ID) on commit preserve rows;

--select*from BGMV_BUY_MONTH

SELECT
--'2020' BASE,
case when a1.sit_site_id is null then b1.sit_site_id else a1.sit_site_id  end as sit_site_id,
case when a1.YEAR_MONTH is null then b1.year_month else a1.year_month end as year_month,
CASE WHEN B1.YEAR_MONTH IS NULL THEN 'No Compra Mes' else 'Compra Mes' end as compra_mes,
--case when a1.AÑO is null then b1.AÑO else a1.año end as AÑO,
--case when a1.quarter is null then b1.quarter else a1.quarter end as quarter,
--CASE WHEN B1.QUARTER IS NULL THEN 'No Compra Q' else 'Compra Q' end as compra_q,
c1.RANGO_VTA_PURO,
c1.REG_DATA_TYPE_group,
c1.Baseline,
c1.CUSTOMER,
c1.Canal,
c1.TIPO_COMPRADOR_TGMV,
count(distinct A1.cus_cust_id_sel) seller,
count(distinct b1.cus_cust_id_buy) buy,
SUM(B1.NB) new_buyer,
SUM(A1.ventas_usd) ventas_usd,
SUM(B1.TGMVe_BUY) TGMVe_BUY,
SUM(B1.TGMVe_BUY*NB) TGMVe_NB_BUY,
SUM(B1.TORDERS_BUY) TORDERS_BUY,
SUM(B1.TORDERS_BUY*NB) TORDERS_NB_BUY,
SUM(B1.TSIE_BUY) TSIE_BUY,
SUM(B1.TSIE_BUY*NB) TSIE_NB_BUY,
sum(B1.TX_BUY)  TX_BUY,
sum(B1.TX_BUY*NB)  TX_NB_BUY

FROM ventas_usd a1 

 full OUTER JOIN BGMV_BUY_MONTH b1 on a1.sit_site_id=b1.sit_site_id and a1.cus_cust_id_sel=b1.cus_cust_id_buy and  a1.year_month=b1.year_month

JOIN BASE C1 ON COALESCE(a1.sit_site_id,b1.sit_site_id)=C1.sit_site_id_cus and COALESCE(a1.cus_cust_id_sel,b1.cus_cust_id_buy)=C1.cus_cust_id

group by 1,2,3,4,5,6,7,8,9;


---------------------------------
-----------PAGOS-----------------
---------------------------------



CREATE MULTISET VOLATILE TABLE BGMV_ORDER_CAT AS (

select
    BID.OPE_OPER_ID,
    BID.ord_order_id,
    bid.sit_site_id,
       CAT.cat_categ_name_l1,
       --CAT.cat_categ_name_l2,
       bid.BID_BASE_CURRENT_PRICE * bid.BID_QUANTITY_OK MONTO,
      
    sum(bid.BID_BASE_CURRENT_PRICE * bid.BID_QUANTITY_OK)  GMVE_BUY
   
from WHOWNER.BT_BIDS as bid

left join WHOWNER.LK_ITE_ITEMS_PH ite
on 	(bid.ITE_ITEM_ID = ite.ITE_ITEM_ID and     
bid.PHOTO_ID = ite.PHOTO_ID and     
bid.SiT_SITE_ID = ite.SIT_SITE_ID)   
	
  
left join WHOWNER.AG_LK_CAT_CATEGORIES_PH as cat    
on (ite.sit_site_id = cat.sit_site_id and ite.cat_Categ_id = cat.cat_Categ_id_l7 and cat.photo_id = 'TODATE')   

where bid.sit_site_id IN ('MLB','MLA','MLM','MLC')
and bid.photo_id = 'TODATE' 
and bid.tim_day_winning_date BETWEEN DATE '2020-01-01' AND DATE '2021-12-31'
and bid.ite_gmv_flag = 1
and bid.mkt_marketplace_id = 'TM'
--and tgmv_flag = 1
--AND coalesce(BID.AUTO_OFFER_FLAG, 0) <> 1 
--AND coalesce(bid.BID_FVF_BONIF, 'Y') = 'N'
qualify row_number () over (partition by BID.OPE_OPER_ID, BID.sit_site_id order by MONTO DESC) = 1
group by 1,2,3,4,5


)WITH DATA PRIMARY INDEX (sit_site_id, OPE_OPER_ID) ON COMMIT PRESERVE ROWS;



select
c1.RANGO_VTA_PURO,
c1.REG_DATA_TYPE_group,
c1.Baseline,
--c1.CUSTOMER,
c1.Canal,
a11.PAY_COMBO_ID,
pm.PAY_PM_TYPE_DESC,
a11.SIT_SITE_ID SIT_SITE_ID,
b1.cat_categ_name_l1,
--b1.cat_categ_name_l2,

(CAST(a11.PAY_MOVE_DATE As DATE)/100+190000) YEAR_MONTH,

	COALESCE(a11.PAY_CCD_INSTALLMENTS_QTY,1) PAY_CCD_INSTALLMENTS_QTY,
		count(distinct A11.PAY_PAYMENT_ID ) TPN,
sum(CAST(a11.PAY_TRANSACTION_DOL_AMT AS DECIMAL(38,4))) TPV_puro

from	BT_MP_PAY_PAYMENTS	a11
LEFT join BASE C1 ON a11.sit_site_id=C1.sit_site_id_cus and a11.cus_cust_id_buy=C1.cus_cust_id
LEFT JOIN BGMV_ORDER_CAT B1 ON A11.sit_site_id=B1.sit_site_id AND A11.PAY_PAYMENT_ID=b1.OPE_OPER_ID 
left join whowner.LK_MP_PAY_PAYMENT_METHODS pm on a11.sit_site_id=pm.sit_site_id and a11.pay_pm_id = pm.pay_pm_id

where	a11.TPV_FLAG = 1
 and a11.sit_site_id IN ('MLB','MLA','MLM','MLC')
 and A11.pay_status_id in ( 'approved')--, 'authorized')
and A11.tpv_segment_id = 'ON'
and a11.PAY_MOVE_DATE BETWEEN DATE '2020-01-01' AND DATE '2021-12-31'
and a11.PAY_COMBO_ID in ('A','J','L')
group by	
1,2,3,4,5,6,7,8,9,10;

-------------------------------------
-----------COMPRA DETALLE------------
-------------------------------------


select
c1.RANGO_VTA_PURO,
c1.REG_DATA_TYPE_group,
c1.Baseline,
--c1.CUSTOMER,
c1.Canal,
c1.TIPO_COMPRADOR_TGMV,
bid.sit_site_id,
cat.cat_categ_name_l1,
CAT.cat_categ_name_l2,
bid.BID_SELL_REP_LEVEL,
shp.shp_free_flag_id,
bid.BID_SEL_REP_REAL_LEVEL,
bid.cbo_combo_id,
--ite.ite_dom_domain_id,
(CAST( bid.TIM_DAY_WINNING_DATE As DATE)/100+190000) YEAR_MONTH,
        --EXTRACT (YEAR from bid.TIM_DAY_WINNING_DATE)  AÑO,
        --(CAST(EXTRACT(YEAR FROM BID.tim_day_winning_date) AS BYTEINT) YEAR,
          --((CAST(EXTRACT(MONTH FROM bid.TIM_DAY_WINNING_DATE) AS BYTEINT)-1)/3)+1
 -- || 'Q' || substring(bid.TIM_DAY_WINNING_DATE,3,2) quarter,
      
       case when shp.SHP_SHIPPING_MODE_ID='me2' and shp.shp_picking_type_id='fulfillment' then 'fbm' 
     when shp.SHP_SHIPPING_MODE_ID='me2' and shp.shp_picking_type_id='self_service' then 'flex'
    -- when shp.SHP_SHIPPING_MODE_ID='me2' and shp.shp_picking_type_id='drop_off' then 'ds'
     when shp.SHP_SHIPPING_MODE_ID='me2' and shp.shp_picking_type_id in ('xd_drop_off','cross_docking') then 'xd'
     when shp.SHP_SHIPPING_MODE_ID='me2' and shp.shp_picking_type_id not in ('self_service','fulfillment', 'xd_drop_off','cross_docking') then 'other me2'
     when shp.SHP_SHIPPING_MODE_ID='me1'  then 'me1'
    -- when pick.odr_order_id is not null then 'puis'
    else 'other' end as ENVIO,
      
   case when    bid.BID_SITE_CURRENT_PRICE * bid.BID_QUANTITY_OK > 99 then 'ORDEN_MAYOR_99'
   ELSE 'ORDEN_MENOR_IGUAL_99' END AS ORDEN_RANGO,
    case when    bid.BID_SITE_CURRENT_PRICE > 99 then 'ITEM_MAYOR_99'
   ELSE 'ITEM_MENOR_IGUAL_99' END AS ITEM_RANGO,
   
      
      
     
        sum((BID.BID_SITE_CURRENT_PRICE * bid.BID_QUANTITY_OK) )  GMVELC,
    sum((bid.BID_BASE_CURRENT_PRICE * bid.BID_QUANTITY_OK) )  GMVE,
  COUNT(( BID.BID_QUANTITY_OK ))  ORDERS,
   sum( BID.BID_QUANTITY_OK)  SIE,

    sum((Case when  tgmv_flag = 1 then (bid.BID_BASE_CURRENT_PRICE * bid.BID_QUANTITY_OK) else 0.0 end))  TGMVE_BUY,
  COUNT((Case when  tgmv_flag = 1 then BID.BID_QUANTITY_OK else 0.0 end))  TORDERS_BUY,
   sum((Case when  tgmv_flag = 1 then BID.BID_QUANTITY_OK else 0.0 end))  TSIE_BUY
   --COUNT(distinct (bid.shp_shipment_id)) as TCAJAS,
  -- COUNT(distinct  (case when bid.crt_purchase_id is null then bid.ord_order_id else bid.crt_purchase_id end) ) as T_TX_BUY
   
from WHOWNER.BT_BIDS as bid

left join WHOWNER.LK_ITE_ITEMS_PH ite
on 	(bid.ITE_ITEM_ID = ite.ITE_ITEM_ID and     
bid.PHOTO_ID = ite.PHOTO_ID and     
	bid.SiT_SITE_ID = ite.SIT_SITE_ID) 
	
	left join WHOWNER.BT_SHP_SHIPMENTS as shp 
on bid.shp_shipment_id = shp.shp_shipment_id and shp.sit_site_id = bid.sit_site_id

left join WHOWNER.AG_LK_CAT_CATEGORIES_PH as cat    
on (ite.sit_site_id = cat.sit_site_id and ite.cat_Categ_id = cat.cat_Categ_id_l7 and cat.photo_id = 'TODATE')   

join BASE C1 ON bID.sit_site_id=C1.sit_site_id_cus and bID.cus_cust_id_buy=C1.cus_cust_id

where bid.sit_site_id IN ('MLB','MLA','MLM','MLC')
and bid.photo_id = 'TODATE' 
and bid.tim_day_winning_date BETWEEN DATE '2020-01-01' AND DATE '2021-12-31'
and bid.ite_gmv_flag = 1
and bid.mkt_marketplace_id = 'TM'
--and CAT.cat_categ_name_l1 IN ('Indústria e Comércio','Informática','Indústria e Comércio')
--and tgmv_flag = 1
--AND coalesce(BID.AUTO_OFFER_FLAG, 0) <> 1 
--AND coalesce(bid.BID_FVF_BONIF, 'Y') = 'N'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
;



-------------------------------
-----------CREDITS ------------
-------------------------------


select
 --- sit_site_id,
b.RANGO_VTA_PURO,
b.REG_DATA_TYPE_group,
b.Baseline,
b.Canal,
  case
    when q_active_cred = 1 then '1.Credito Activo'
    when flag_de = 1 or KILLER_OFERTA_MC_VIGENTE = 1 or KILLER_OFERTA_MC_VIGENTE_DE = 1 then '2.Propuesta Vigente'
    when a.sit_site_id is null then '3.Sin Propuesta o Credito - No esta en base'
    else '3.Sin Propuesta o Credito'
  end as Situacion,
  APROBACION,
  sum(case when tipo_persona = 'PF' then 1 else 0 end) as PF,
  sum(case when tipo_persona = 'PJ' then 1 else 0 end) as PJ,
  sum(case when tipo_persona in ('PF','PJ') then 0 else 1 end) as Sin_identif,
  count(cus_cust_id_sel) as Total,
  sum(SUM_AMOUNT_ACTIVE_CRED ) as SUM_AMOUNT_ACTIVE_CRED ,
  sum(PROP_AMOUNT_LAST_PROPOSAL) as PROP_AMOUNT_LAST_PROPOSAL,
avg(SUM_AMOUNT_ACTIVE_CRED ) as avg_SUM_AMOUNT_ACTIVE_CRED ,
  avg(PROP_AMOUNT_LAST_PROPOSAL) as avg_PROP_AMOUNT_LAST_PROPOSAL,
  median(SUM_AMOUNT_ACTIVE_CRED ) as median_SUM_AMOUNT_ACTIVE_CRED ,
  median(PROP_AMOUNT_LAST_PROPOSAL) as median_PROP_AMOUNT_LAST_PROPOSAL
  
from credits.mc_jarvis a
full outer join BASE b ON b.cus_cust_id=a.cus_cust_id_sel and b.sit_site_id_cus=a.sit_site_id 
where 
  sit_site_id IN ('MLB','MLA','MLM','MLC')
group by 1,2,3,4,5,6
order by 1,2,3,4,5,6;


----------------------------
----------- NPS ------------
----------------------------

drop table NPS;
CREATE MULTISET VOLATILE TABLE NPS, NO LOG          
AS ( SELECT
    ENV.ORDER_ID, 
    env.ite_item_id,
    ENV.SIT_SITE_ID AS Site,        
    ENV.CUS_CUST_ID_SEL AS SELLER_ID,      
    ENV.CUS_CUST_ID AS BUYER_ID,        
    ENV.SURVEY_SENT_DATE,      
    RESP.END_DATE,      
    ENV.TRANSACTION_TYPE AS Rol,        
    RESP.NPS AS NOTA_NPS,      
    CASE        
        WHEN RESP.NPS >=9 THEN 1    
        WHEN RESP.NPS=7 or RESP.NPS=8 THEN 0    
        WHEN RESP.NPS <=6 THEN -1  
    END AS NPS,    
    RESP.DETRACTION_REASON_NPS,    
    RESP.PROMOTION_REASON_NPS,      
    RESP.COMMENTS,      
    RESP.SURVEY_ID,    
    (CAST(RESP.END_DATE As DATE)/100+190000) AS END_DATE_MONTH,    
    ENV.BUYER_SEGMENT,      
    ENV.SELLER_SEGMENT,    
    ENV.SHIPPING_SELLER,
    CASE WHEN UPPER(ENV.SHIPPING_SELLER) IN('DROP OFF', 'PICK UP') THEN 'DS' ELSE ENV.SHIPPING_SELLER END FLAG_SHIPPING_SELLER,
    ENV.SHIPPING_BUYER,    
    ENV.PAYMENT_METHOD,    
    SG.GROUP_SHP_SELLER,        
    SG.GROUP_SHP_BUYER,    
    SG.GROUP_SELLER_SEGMENT,        
    SG.SEGMENT_GROUP_ID,        
    (CASE      
        WHEN RESP.NPS >=9 THEN 1    
        ELSE 0  
    END) AS PROMOTERS,      
    (CASE      
        WHEN RESP.NPS=8 or RESP.NPS=7 THEN 1    
        ELSE 0  
    END) AS PASSIVES,      
    (CASE      
        WHEN RESP.NPS <=6 THEN 1    
        ELSE 0  
    END) AS DETRACTORS    
    FROM BT_CX_SURVEYS_RESPONSES RESP
    JOIN BT_CX_SURVEYS_HIS_CROUPIER ENV    
        ON (RESP.SURVEY_ID = ENV.SURVEY_ID) AND RESP.SEGMENT_ID = ENV.SEGMENT_ID    
            LEFT JOIN LK_CX_SEGMENT_GROUP SG
              ON (RESP.SEGMENT_ID = SG.E_CODE AND END_DATE_MONTH = SG.TIM_MONTH_ID)
WHERE
  ENV.TRANSACTION_TYPE in ('B') and
  cast(RESP.END_DATE as date) BETWEEN DATE '2020-01-01' AND DATE '2021-12-31'
  AND ENV.SIT_SITE_ID IN (${vars})-,'MLC','MCO','MLU')
) WITH DATA PRIMARY INDEX(ORDER_ID) on commit preserve rows;    



create multiset VOLATILE TABLE NPS_CATEG, no log as(

SELECT
      --NPS.*,
      END_DATE_MONTH,
      site,
      avg (NPS)
      
     -- CAT.cat_categ_name_l1,
      --CAT.cat_categ_name_l2,
      --CAT.cat_categ_name_l3,
      --CAT.vertical
from NPS NPS
      LEFT join WHOWNER.BT_BIDS BIDS on
                NPS.order_id=BIDS.ord_order_id
                and BIDS.photo_id='TODATE'
                and NPS.SITE = BIDS.SIT_SITE_ID
      LEFT JOIN WHOWNER.LK_ITE_ITEMS_PH ITE ON
                ITE.sit_site_id = NPS.Site
                and ITE.ite_item_id = NPS.ite_item_id
                AND ITE.photo_id = 'TODATE'
      LEFT JOIN WHOWNER.AG_LK_CAT_CATEGORIES_PH CAT ON
                CAT.sit_site_id = NPS.Site
                and CAT.photo_id = 'TODATE'
                AND CAT.CAT_CATEG_ID_L7 = ITE.cat_categ_id
                
join BASE C1 ON bID.sit_site_id=C1.sit_site_id_cus and bID.cus_cust_id_buy=C1.cus_cust_id                
                
GROUP BY 1,2
) WITH DATA PRIMARY INDEX(ORDER_ID) ON COMMIT PRESERVE ROWS;

SELECT * FROM NPS_CATEG;


-----------------------------------------------------
----------- CANCELACIONES Y DEVOLUCIONES ------------
-----------------------------------------------------



select
bid.sit_site_id,
(CAST( bid.TIM_DAY_WINNING_DATE As DATE)/100+190000) YEAR_MONTH,
D.RANGO_VTA_PURO,
D.Canal,
D.REG_DATA_TYPE_group,
D.TIPO_COMPRADOR_BGMV,
cat.cat_categ_name_l1,
can.shipping_status,
can.tipo_cancelacion,



case when shp.SHP_SHIPPING_MODE_ID='me2' and shp.shp_picking_type_id='fulfillment' then 'fbm' 
-- when shp.SHP_SHIPPING_MODE_ID='me2' and shp.shp_picking_type_id='self_service' then 'flex'
-- when shp.SHP_SHIPPING_MODE_ID='me2' and shp.shp_picking_type_id='drop_off' then 'ds'
when shp.SHP_SHIPPING_MODE_ID='me2' and shp.shp_picking_type_id in ('xd_drop_off','cross_docking') then 'xd'
when shp.SHP_SHIPPING_MODE_ID='me2' and shp.shp_picking_type_id not in ('self_service','fulfillment', 'xd_drop_off','cross_docking','drop_off') then 'other me2'
when shp.SHP_SHIPPING_MODE_ID='me1'  then 'me1'
    -- when pick.odr_order_id is not null then 'puis'
else 'other' end as ENVIO,


  sum(( (bid.BID_BASE_CURRENT_PRICE * bid.BID_QUANTITY_OK) ))  GMVE,
  COUNT((BID.BID_QUANTITY_OK ))  ORDERS,
   sum(( BID.BID_QUANTITY_OK))  SIE  

  sum((Case when  tgmv_flag = 1  then (BID.BID_SITE_CURRENT_PRICE * bid.BID_QUANTITY_OK) else 0.0 end))  TGMVELC,
  sum((Case when  tgmv_flag = 1 then (bid.BID_BASE_CURRENT_PRICE * bid.BID_QUANTITY_OK) else 0.0 end))  TGMVE,
  COUNT((Case when  tgmv_flag = 1 then BID.BID_QUANTITY_OK else 0.0 end))  TORDERS,
   sum((Case when  tgmv_flag = 1 then BID.BID_QUANTITY_OK else 0.0 end))  TSIE
    
    --count(distinct bid.ite_item_id),
    --count(bid.ite_item_id)
    
from WHOWNER.BT_BIDS as bid

left join WHOWNER.LK_ITE_ITEMS_PH ite
on 	(bid.ITE_ITEM_ID = ite.ITE_ITEM_ID and     
bid.PHOTO_ID = ite.PHOTO_ID and     
	bid.SiT_SITE_ID = ite.SIT_SITE_ID)   


left join WHOWNER.AG_LK_CAT_CATEGORIES_PH as cat    
on (ite.sit_site_id = cat.sit_site_id and ite.cat_Categ_id = cat.cat_Categ_id_l7 and cat.photo_id = 'TODATE')   

JOIN BASE3 D on bid.cus_cust_id_buy=d.cus_cust_id and bid.sit_site_id=d.sit_site_id_cus
join WHOWNER.LK_SEGMENTO_SELLERS SEG ON D.CUS_CUST_ID=SEG.cus_cust_id_sel AND  d.sit_site_id_cus=SEG.sit_site_id -- AND SEG.SEGMENTO IN ('TO','CARTERA GESTIONADA','MIDTAIL')

left join WHOWNER.BT_SHP_SHIPMENTS as shp 
on bid.shp_shipment_id = shp.shp_shipment_id and shp.sit_site_id = bid.sit_site_id

left join WHOWNER.BT_CM_ORDERS_CANCELLED can
 ON Bid.ORD_ORDER_ID = Can.ord_order_id
   AND Bid.SIT_SITE_ID = Can.sit_site_id
   and can.sit_site_id='MLB'


where bid.sit_site_id IN ('MLB','MLA','MLM','MLC')
and bid.photo_id = 'TODATE' 
and bid.tim_day_winning_date BETWEEN DATE '2020-01-01' AND DATE '2021-12-31'
and bid.ite_gmv_flag = 1
and bid.mkt_marketplace_id = 'TM'


--and tgmv_flag = 1
group by 1,2,3,4,5,6,7,8,9,10;




