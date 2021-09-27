
-----
-- Cambios vs Metricas Anteriores:
-- Me saltaba un error cuando queremos correr historia, por que tal vez eran sellers que compraban y no vendian entonces no me trae datos de company ni del canal Con lo cual voy a poner dos canales
-- a. Canal ulitmo año rolling
-- b. Canal mensual
-- Baseline o no lo defino con el ultimo año rolling
-- Esta informacion la agrego en la BASE_CUST y no hago compras por que no se necesitan









CREATE multiset volatile table TPV_SEL_1_AÑO as (

select
mp.cus_cust_id_sel cus_cust_id_sel, --- numero de usuario
--case when scl.mpb_splitter_id is not null then mpb_splitter_id else cus_cust_id_sel end cus_cust_id_sel,
mp.sit_site_id sit_site_id, --- site

tpv_segment_id tpv_segment_id, --- Segmento de donde vende
tpv_segment_detail tpv_segment_detail, --- + detalle
--mp.pay_status_id,
--PAY_MOVE_DATE TIM_DAY,
-- count (distinct mp.cus_cust_id_sel),
--sum (TPV_DOL_AMT) TPV_DOL_AMT_SEL,
sum (PAY_TRANSACTION_DOL_AMT) VENTAS_USD, --- suma volumen de ventas

--sum(pay_total_paid_dol_amt) TOTAL_PAY
--sum (TPV_AMT) TPV_AMT,
sum (1) Q -- para contar la cantidad de segmentos que se vende

from WHOWNER.BT_MP_PAY_PAYMENTS mp
--left join (select distinct mpb_splitter_id,pay_created_from from mp_mpb.lk_splitter_classification) scl on scl.pay_created_from = mp.pay_created_from
--left join whowner.lk_ite_items_ph i on i.sit_site_id=mp.sit_site_id and i.ite_item_id=mp.ite_item_id --and i.ITE_TIPO_PROD = 'N'
where mp.tpv_flag = 1
and 
mp.sit_site_id IN (${vars})
and MP.PAY_MOVE_DATE between date ${start_date_AÑO} and date ${end_date_AÑO} 
and mp.pay_status_id in ( 'approved')--, 'authorized')
and tpv_segment_id <> 'ON'--AND coalesce(i.ITE_TIPO_PROD,0) <> 'U' -----> Saco todo lo que es Marketplace
--and mp.cus_cust_id_buy not in (185198438)
group by 1,2,3,4

Union --- para poder hacer union las dos tablas son iguales

select
   BID.cus_cust_id_sel cus_cust_id_sel,  
       bid.sit_site_id sit_site_id,
       'Selling ML' tpv_segment_id,
       'Selling ML' tpv_segment_detail,
    --sum((bid.BID_BASE_CURRENT_PRICE * bid.BID_QUANTITY_OK))  GMVEBILLABLE,   
    sum((Case when coalesce(bid.BID_FVF_BONIF, 'Y') = 'N' then (bid.BID_BASE_CURRENT_PRICE * bid.BID_QUANTITY_OK) else 0.0 end))  VENTAS_USD,
    sum (1) Q
from WHOWNER.BT_BIDS as bid
where bid.sit_site_id IN (${vars})
and bid.photo_id = 'TODATE' 
and bid.tim_day_winning_date between date ${start_date_AÑO} and date ${end_date_AÑO} 
and bid.ite_gmv_flag = 1
and bid.mkt_marketplace_id = 'TM'
AND coalesce(BID.AUTO_OFFER_FLAG, 0) <> 1 
AND coalesce(bid.BID_FVF_BONIF, 'Y') = 'N'
group by 1,2,3,4

) with data primary index (CUS_CUST_ID_SEL,SIT_SITE_ID) on commit preserve rows;




create multiset volatile table TPV_SEL_AÑO as ( --- sumo los volumenes por seller --- volumen total por seller

select
cus_cust_id_sel,
sit_site_id,
sum(VENTAS_USD) VENTAS_USD,
sum(Q) Q,
count(distinct tpv_segment_id) Q_SEG

from TPV_SEL_1_AÑO
GROUP BY 1,2

) with data primary index (CUS_CUST_ID_SEL,SIT_SITE_ID) on commit preserve rows;



create multiset volatile table RANGO_AÑO as ( --- sumo los volumenes por seller --- volumen total por seller

select
cus_cust_id_sel,
sit_site_id,

 case 
     when v.VENTAS_USD is null then 'a.No Vende'
     when v.VENTAS_USD= 0 then 'a.No Vende'
     when v.VENTAS_USD <= 6000 then 'b.Menos 6.000'
     when v.VENTAS_USD <= 40000 then 'c.6.000 a 40.000'
     when v.VENTAS_USD<= 200000 then 'd.40.000 a 200.000'
     else 'e.Mas de 200.000' end  RANGO_VTA_PURO


from TPV_SEL_AÑO V

) with data primary index (CUS_CUST_ID_SEL,SIT_SITE_ID) on commit preserve rows;


CREATE multiset volatile TABLE SEG_AÑO as ( --- me quedo con el segmento del seller con mas ventas --- segmento por seller
SELECT 
  SP.cus_cust_id_sel, 
  SP.sit_site_id,
  SP.tpv_segment_id,
  SP.tpv_segment_detail,
  sm.SEGMENTO,
  VENTAS_USD
FROM TPV_SEL_1_AÑO sp
LEFT JOIN WHOWNER.LK_SEGMENTO_SELLERS  SM 
on SP.sit_site_id=sM.sit_site_id AND SP.cus_cust_id_sel=sM.cus_cust_id_sel
qualify row_number () over (partition by sp.cus_cust_id_sel, sp.sit_site_id order by VENTAS_USD DESC) = 1
GROUP BY 1,2,3,4,5,6
) with data primary index (CUS_CUST_ID_SEL,SIT_SITE_ID) on commit preserve rows;





CREATE multiset volatile TABLE br_mx as (
SELECT
  cus_cust_id,
  sit_site_id,
  cus_tax_payer_type,
  cus_tax_regime
FROM LK_TAX_CUST_WRAPPER
WHERE SIT_SITE_ID IN ('MLB','MLA','MLM','MLC')
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
    
CASE WHEN KYC.KYC_ENTITY_TYPE='company' then 'ok' 
    when KYC.KYC_ENTITY_TYPE<>'company' and RANGO_VTA_PURO  not in ('a.No Vende','b.Menos 6.000')  then 'ok' 
    else 'not ok'  end as Baseline,
    
CASE WHEN S.tpv_segment_detail ='Aggregator - Other' then 'Online Payments' 
WHEN S.tpv_segment_detail ='Checkout OFF' then 'Online Payments' 
when S.tpv_segment_detail ='Instore' then 'QR'
when S.tpv_segment_detail ='Selling ML' then 'Selling ML'
when S.tpv_segment_detail ='Point' then 'Point'
when S.tpv_segment_detail is null then 'No Vende'
else 'Not Considered'end as Canal_Año,


    COUNT(*) cuenta

FROM WHOWNER.LK_CUS_CUSTOMERS_DATA D -- base de clientes
LEFT JOIN WHOWNER.LK_KYC_VAULT_USER KYC ON KYC.CUS_CUST_ID=D.CUS_CUST_ID
LEFT JOIN br_mx tax on tax.CUS_CUST_ID=D.CUS_CUST_ID and tax.sit_site_id=d.sit_site_id_cus
LEFT JOIN SEG_AÑO s on d.CUS_CUST_ID=s.CUS_CUST_ID_sel and s.sit_site_id=d.sit_site_id_cus
LEFT JOIN RANGO_AÑO R on d.CUS_CUST_ID=R.CUS_CUST_ID_sel and R.sit_site_id=d.sit_site_id_cus

WHERE COALESCE(D.CUS_TAGS,'') <> 'sink'
AND D.sit_site_id_cus IN ('MLB','MLA','MLM','MLC')

group by 1,2,3,4,5,6,7,8,9

--having CUSTOMER not in ('Operador','MELI-1P/PL')
) with data primary index (cus_cust_id,SIT_SITE_ID_cus) on commit preserve rows;




-----


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
where bid.sit_site_id IN (${vars})
and bid.photo_id = 'TODATE' 
and bid.tim_day_winning_date between date ${start_date} and date ${end_date} 
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
mp.sit_site_id IN (${vars})
and MP.PAY_MOVE_DATE between date ${start_date} and date ${end_date} 
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
where bid.sit_site_id IN (${vars})
and bid.photo_id = 'TODATE' 
and bid.tim_day_winning_date between date ${start_date} and date ${end_date} 
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





----------------------------
-----------GENERAL------------
----------------------------

SELECT

case when a1.sit_site_id is null then b1.sit_site_id else a1.sit_site_id  end as sit_site_id,
case when a1.YEAR_MONTH is null then b1.year_month else a1.year_month end as year_month,
--case when a1.AÑO is null then b1.AÑO else a1.año end as AÑO,
--case when a1.quarter is null then b1.quarter else a1.quarter end as quarter,
CASE WHEN B1.YEAR_MONTH  IS NULL THEN 'No Compra M' else 'Compra M' end as compra_q,
case when b1.year_month=c1.YEAR_MONTH_BUY then 'NB' when B1.YEAR_MONTH is null then 'S'  else 'RB' end as tipo_comprador,
c1.CUS_TYPE,
c1.CUSTOMER,
C1.Baseline,
--c1.CANAL_AÑO,
--C1.year_month,
--C1.cus_tax_payer_type,
CASE WHEN S.tpv_segment_detail ='Aggregator - Other' then 'Online Payments' 
WHEN S.tpv_segment_detail ='Checkout OFF' then 'Online Payments' 
when S.tpv_segment_detail ='Instore' then 'QR'
when S.tpv_segment_detail ='Selling ML' then 'Selling ML'
when S.tpv_segment_detail ='Point' then 'Point'
when S.tpv_segment_detail is null then 'No Vende'
else 'Not Considered'end as Canal,

--CASE WHEN C1.YEAR_MONTH_SEL IS NULL THEN 'No vendio nunca'
--else 'Vendio Alguna Vez' end as VENTAS_CHECK,

count(distinct A1.cus_cust_id_sel) seller,
count(distinct b1.cus_cust_id_buy) buy,
count(distinct case when b1.cus_cust_id_buy is null then   A1.cus_cust_id_sel else  b1.cus_cust_id_buy end) total,
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

group by 1,2,3,4,5,6,7,8;

--

----------------------------
-----------PAGOS------------
----------------------------


select
--c1.RANGO_VTA_PURO,
c1.CUS_TYPE,
c1.Baseline,
--C1.Canal_Año
--c1.CUSTOMER,

/*CASE WHEN S.tpv_segment_detail ='Aggregator - Other' then 'Online Payments' 
WHEN S.tpv_segment_detail ='Checkout OFF' then 'Online Payments' 
when S.tpv_segment_detail ='Instore' then 'QR'
when S.tpv_segment_detail ='Selling ML' then 'Selling ML'
when S.tpv_segment_detail ='Point' then 'Point'
when S.tpv_segment_detail is null then 'No Vende'
else 'Not Considered'end as Canal,*/

a11.PAY_COMBO_ID,
pm.PAY_PM_TYPE_DESC,
a11.SIT_SITE_ID SIT_SITE_ID,
--b1.cat_categ_name_l1,
--b1.cat_categ_name_l2,

(CAST(a11.PAY_MOVE_DATE As DATE)/100+190000) YEAR_MONTH,

	COALESCE(a11.PAY_CCD_INSTALLMENTS_QTY,1) PAY_CCD_INSTALLMENTS_QTY,
		count(distinct A11.PAY_PAYMENT_ID ) TPN,
sum(CAST(a11.PAY_TRANSACTION_DOL_AMT AS DECIMAL(38,4))) TPV_puro

from	BT_MP_PAY_PAYMENTS	a11
LEFT join BASE_CUST C1 ON a11.sit_site_id=C1.sit_site_id_cus and a11.cus_cust_id_buy=C1.cus_cust_id
--LEFT JOIN BGMV_ORDER_CAT B1 ON A11.sit_site_id=B1.sit_site_id AND A11.PAY_PAYMENT_ID=b1.OPE_OPER_ID 
left join whowner.LK_MP_PAY_PAYMENT_METHODS pm on a11.sit_site_id=pm.sit_site_id and a11.pay_pm_id = pm.pay_pm_id
LEFT JOIN SEG_SEL S on a11.sit_site_id=s.sit_site_id and a11.cus_cust_id_buy=s.cus_cust_id_SEL and (CAST(a11.PAY_MOVE_DATE As DATE)/100+190000)=s.YEAR_MONTH 

where	a11.TPV_FLAG = 1
 and a11.sit_site_id IN (${vars})
 and A11.pay_status_id in ( 'approved')--, 'authorized')
and A11.tpv_segment_id = 'ON'
and a11.PAY_MOVE_DATE between date ${start_date} and date ${end_date} 
and a11.PAY_COMBO_ID in ('A','J','L')
group by	
1,2,3,4,5,6,7;

-------------------------------------
-----------COMPRA DETALLE------------
-------------------------------------

select
--c1.RANGO_VTA_PURO,
C1.CUS_TYPE,
c1.Baseline,
--C1.CANAL_AÑO,
--c1.CUSTOMER,
--CASE WHEN S.tpv_segment_detail ='Aggregator - Other' then 'Online Payments' 
--WHEN S.tpv_segment_detail ='Checkout OFF' then 'Online Payments' 
--when S.tpv_segment_detail ='Instore' then 'QR'
--when S.tpv_segment_detail ='Selling ML' then 'Selling ML'
--when S.tpv_segment_detail ='Point' then 'Point'
--when S.tpv_segment_detail is null then 'No Vende'
--else 'Not Considered'end as Canal,
--c1.Canal,
--c1.TIPO_COMPRADOR_TGMV,
---BID.CUS_CUST_ID_BUY,
bid.sit_site_id,
DOM.VERTICAL,
--cat.cat_categ_name_l1,
--CAT.cat_categ_name_l2,
CASE WHEN bid.BID_SEL_REP_REAL_LEVEL IS NULL THEN bid.BID_SELL_REP_LEVEL ELSE bid.BID_SEL_REP_REAL_LEVEL end AS REP,
--shp.shp_free_flag_id,
--bid.BID_SEL_REP_REAL_LEVEL,
bid.cbo_combo_id,
--ite.ite_dom_domain_id,
(CAST( bid.TIM_DAY_WINNING_DATE As DATE)/100+190000) YEAR_MONTH,
bid.shp_shipping_mode_id,
        --EXTRACT (YEAR from bid.TIM_DAY_WINNING_DATE)  AÑO,
        --(CAST(EXTRACT(YEAR FROM BID.tim_day_winning_date) AS BYTEINT) YEAR,
          --((CAST(EXTRACT(MONTH FROM bid.TIM_DAY_WINNING_DATE) AS BYTEINT)-1)/3)+1
 -- || 'Q' || substring(bid.TIM_DAY_WINNING_DATE,3,2) quarter,
      
  /*  case when shp.SHP_SHIPPING_MODE_ID='me2' and shp.shp_picking_type_id='fulfillment' then 'fbm' 
      when shp.SHP_SHIPPING_MODE_ID='me2' and shp.shp_picking_type_id='self_service' then 'flex'
    -- when shp.SHP_SHIPPING_MODE_ID='me2' and shp.shp_picking_type_id='drop_off' then 'ds'
      when shp.SHP_SHIPPING_MODE_ID='me2' and shp.shp_picking_type_id in ('xd_drop_off','cross_docking') then 'xd'
      when shp.SHP_SHIPPING_MODE_ID='me2' and shp.shp_picking_type_id not in ('self_service','fulfillment', 'xd_drop_off','cross_docking') then 'other me2'
     when shp.SHP_SHIPPING_MODE_ID='me1'  then 'me1'
    -- when pick.odr_order_id is not null then 'puis'
    else 'other' end as ENVIO,*/
      
   --case when    bid.BID_SITE_CURRENT_PRICE * bid.BID_QUANTITY_OK > 99 then 'ORDEN_MAYOR_99'
   --ELSE 'ORDEN_MENOR_IGUAL_99' END AS ORDEN_RANGO,
   -- case when    bid.BID_SITE_CURRENT_PRICE > 99 then 'ITEM_MAYOR_99'
  -- ELSE 'ITEM_MENOR_IGUAL_99' END AS ITEM_RANGO,
   
           sum(( (bid.BID_SITE_CURRENT_PRICE * bid.BID_QUANTITY_OK) ))  GMVE_LC,
       sum(( (bid.BID_BASE_CURRENT_PRICE * bid.BID_QUANTITY_OK) ))  GMVE,
  COUNT((BID.BID_QUANTITY_OK ))  ORDERS,
   sum(( BID.BID_QUANTITY_OK))  SIE , 
       
      
     

   
 sum((Case when  tgmv_flag = 1 then (bid.BID_SITE_CURRENT_PRICE * bid.BID_QUANTITY_OK) else 0.0 end))  TGMVE_BUY_LC,
    sum((Case when  tgmv_flag = 1 then (bid.BID_BASE_CURRENT_PRICE * bid.BID_QUANTITY_OK) else 0.0 end))  TGMVE_BUY,
  COUNT((Case when  tgmv_flag = 1 then BID.BID_QUANTITY_OK else 0.0 end))  TORDERS_BUY,
   sum((Case when  tgmv_flag = 1 then BID.BID_QUANTITY_OK else 0.0 end))  TSIE_BUY
   --COUNT(distinct (bid.shp_shipment_id)) as TCAJAS,
  -- COUNT(distinct  (case when bid.crt_purchase_id is null then bid.ord_order_id else bid.crt_purchase_id end) ) as T_TX_BUY
   
from WHOWNER.BT_BIDS as bid

--inner join WHOWNER.LK_ITE_ITEMS_PH ite
--on 	(bid.ITE_ITEM_ID = ite.ITE_ITEM_ID and     
--bid.PHOTO_ID = ite.PHOTO_ID and     
--	bid.SiT_SITE_ID = ite.SIT_SITE_ID) 
	
--left join WHOWNER.BT_SHP_SHIPMENTS as shp 
--on bid.shp_shipment_id = shp.shp_shipment_id and shp.sit_site_id = bid.sit_site_id

inner join WHOWNER.LK_DOM_DOMAINS as DOM  
on (BID.sit_site_id = DOM.sit_site_id and BID.dom_domain_id = DOM.dom_domain_id )   

join BASE_CUST C1 ON bid.sit_site_id=C1.sit_site_id_cus and bid.cus_cust_id_buy=C1.cus_cust_id  --AND Baseline='ok'

LEFT JOIN SEG_SEL S on bid.sit_site_id=s.sit_site_id and bid.cus_cust_id_buy=s.cus_cust_id_SEL and (CAST(bid.tim_day_winning_date As DATE)/100+190000)=s.YEAR_MONTH 

where bid.sit_site_id IN (${vars})
and bid.photo_id = 'TODATE' 
and bid.tim_day_winning_date between date ${start_date} and date ${end_date} 
and bid.ite_gmv_flag = 1
and bid.mkt_marketplace_id = 'TM'
--and CAT.cat_categ_name_l1 IN ('Indústria e Comércio','Informática','Indústria e Comércio')
--and tgmv_flag = 1
--AND coalesce(BID.AUTO_OFFER_FLAG, 0) <> 1 
--AND coalesce(bid.BID_FVF_BONIF, 'Y') = 'N'
group by 1,2,3,4,5,6,7,8




-------------------------------
-----------CREDITS ------------
-------------------------------


select
 --- sit_site_id,
--b.RANGO_VTA_PURO,
b.CUS_TYPE,
b.Baseline,
b.Canal_AÑO,
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
 join BASE_cust b ON b.cus_cust_id=a.cus_cust_id_sel and b.sit_site_id_cus=a.sit_site_id 
where 
  sit_site_id IN (${vars})
group by 1,2,3,4,5,6
--order by 1,2,3,4,5,6;


----------------------------
----------- NPS ------------
----------------------------

--drop table NPS;
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
  cast(RESP.END_DATE as date) between date ${start_date} and date ${end_date} 
  AND ENV.SIT_SITE_ID IN (${vars})
) WITH DATA PRIMARY INDEX(ORDER_ID) on commit preserve rows;    





SELECT
      --NPS.*,
      END_DATE_MONTH,
      C1.CUS_TYPE,
      C1.BASELINE,
      site,
      avg (NPS) nps
      
     -- CAT.cat_categ_name_l1,
      --CAT.cat_categ_name_l2,
      --CAT.cat_categ_name_l3,
      --CAT.vertical
from NPS NPS
      LEFT join WHOWNER.BT_BIDS BIDS on
                NPS.order_id=BIDS.ord_order_id
                and BIDS.photo_id='TODATE'
                and NPS.SITE = BIDS.SIT_SITE_ID
      --LEFT JOIN WHOWNER.LK_ITE_ITEMS_PH ITE ON
         --       ITE.sit_site_id = NPS.Site
         --       and ITE.ite_item_id = NPS.ite_item_id
          --     AND ITE.photo_id = 'TODATE'
      --LEFT JOIN WHOWNER.AG_LK_CAT_CATEGORIES_PH CAT ON
        --        CAT.sit_site_id = NPS.Site
         --       and CAT.photo_id = 'TODATE'
           --     AND CAT.CAT_CATEG_ID_L7 = ITE.cat_categ_id
                
join BASE_cust C1 ON bIDs.sit_site_id=C1.sit_site_id_cus and bIDs.cus_cust_id_buy=C1.cus_cust_id                
                
GROUP BY 1,2,3,4



-----------------------------------------------------
----------- CANCELACIONES Y DEVOLUCIONES ------------
-----------------------------------------------------



select
bid.sit_site_id,
(CAST( bid.TIM_DAY_WINNING_DATE As DATE)/100+190000) YEAR_MONTH,
--D.RANGO_VTA_PURO,
--D.Canal,
D.CUS_TYPE,
D.BASELINE,
--D.TIPO_COMPRADOR_BGMV,
--cat.cat_categ_name_l1,
--dom.vertical,
can.shipping_status,
can.tipo_cancelacion,

bid.shp_shipping_mode_id,

/*case when shp.SHP_SHIPPING_MODE_ID='me2' and shp.shp_picking_type_id='fulfillment' then 'fbm' 
-- when shp.SHP_SHIPPING_MODE_ID='me2' and shp.shp_picking_type_id='self_service' then 'flex'
-- when shp.SHP_SHIPPING_MODE_ID='me2' and shp.shp_picking_type_id='drop_off' then 'ds'
when shp.SHP_SHIPPING_MODE_ID='me2' and shp.shp_picking_type_id in ('xd_drop_off','cross_docking') then 'xd'
when shp.SHP_SHIPPING_MODE_ID='me2' and shp.shp_picking_type_id not in ('self_service','fulfillment', 'xd_drop_off','cross_docking','drop_off') then 'other me2'
when shp.SHP_SHIPPING_MODE_ID='me1'  then 'me1'
    -- when pick.odr_order_id is not null then 'puis'
else 'other' end as ENVIO,
*/

  sum(( (bid.BID_BASE_CURRENT_PRICE * bid.BID_QUANTITY_OK) ))  GMVE,
  COUNT((BID.BID_QUANTITY_OK ))  ORDERS,
   sum(( BID.BID_QUANTITY_OK))  SIE , 

  sum((Case when  tgmv_flag = 1  then (BID.BID_SITE_CURRENT_PRICE * bid.BID_QUANTITY_OK) else 0.0 end))  TGMVELC,
  sum((Case when  tgmv_flag = 1 then (bid.BID_BASE_CURRENT_PRICE * bid.BID_QUANTITY_OK) else 0.0 end))  TGMVE,
  COUNT((Case when  tgmv_flag = 1 then BID.BID_QUANTITY_OK else 0.0 end))  TORDERS,
   sum((Case when  tgmv_flag = 1 then BID.BID_QUANTITY_OK else 0.0 end))  TSIE
    
    --count(distinct bid.ite_item_id),
    --count(bid.ite_item_id)
    
from WHOWNER.BT_BIDS as bid

--left join WHOWNER.LK_ITE_ITEMS_PH ite
--on 	(bid.ITE_ITEM_ID = ite.ITE_ITEM_ID and     
--bid.PHOTO_ID = ite.PHOTO_ID and     
--	bid.SiT_SITE_ID = ite.SIT_SITE_ID)   


--left join WHOWNER.LK_DOM_DOMAINS as dom    
--on (ite.sit_site_id = dom.sit_site_id and ite.ite_dom_domain_id = dom.dom_domain_id ) 

JOIN BASE_CUST D on bid.cus_cust_id_buy=d.cus_cust_id and bid.sit_site_id=d.sit_site_id_cus
join WHOWNER.LK_SEGMENTO_SELLERS SEG ON D.CUS_CUST_ID=SEG.cus_cust_id_sel AND  d.sit_site_id_cus=SEG.sit_site_id -- AND SEG.SEGMENTO IN ('TO','CARTERA GESTIONADA','MIDTAIL')

--left join WHOWNER.BT_SHP_SHIPMENTS as shp 
--on bid.shp_shipment_id = shp.shp_shipment_id and shp.sit_site_id = bid.sit_site_id

left join WHOWNER.BT_CM_ORDERS_CANCELLED can
 ON Bid.ORD_ORDER_ID = Can.ord_order_id
   AND Bid.SIT_SITE_ID = Can.sit_site_id
   and can.sit_site_id='MLB'


where bid.sit_site_id IN (${vars})
and bid.photo_id = 'TODATE' 
and bid.tim_day_winning_date between date ${start_date} and date ${end_date} 
and bid.ite_gmv_flag = 1
and bid.mkt_marketplace_id = 'TM'


--and tgmv_flag = 1
group by 1,2,3,4,5,6,7;



