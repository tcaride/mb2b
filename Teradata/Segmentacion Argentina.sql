/* SEGMENTACION ARGENTINA TERADATA */

----------------------------- 00. Creo la base de cust de companias  ------------------------------------------

CREATE MULTISET VOLATILE TABLE d00_cust AS (

-- Traigo company de Customer 67300
SELECT DISTINCT CUS_CUST_ID FROM LK_CUS_CUSTOMERS_DATA
WHERE (COALESCE(CUS_TAGS, '') <> 'sink') AND (sit_site_id_cus in ('MLA')) AND 
(cus_cust_doc_type IN ('CUIL','CUIT') AND (left(cus_cust_doc_number,1)='3'  OR left(cus_cust_doc_number,1)='55'))

UNION

-- Traigo company de KYC 36528
SELECT  DISTINCT CUS_CUST_ID FROM LK_KYC_CUSTOMERS 
WHERE (cus_doc_type IN ('CUIL','CUIT') AND (left(cus_doc_number,1)='3' OR left(cus_doc_number,1)='55' ))

UNION
-- Traigo company de KYC2 11814705 (2908056)
SELECT DISTINCT cus_cust_id FROM BT_MP_KYC_LEVEL WHERE CUS_KYC_ENTITY_TYPE IN('company') AND sit_site_id= 'MLA'

UNION
-- Traigo company de R1 2289259 (169763)
-- select  count(distinct REG_CUST_DOC_NUMBER) from LK_REG_PERSON where REG_DATA_TYPE IN('company') and sit_site_id= 'MLA'

-- Traigo company de R 8174454 (137825)
SELECT DISTINCT CUS_CUST_ID FROM LK_REG_CUSTOMERS
WHERE (REG_CUST_DOC_NUMBER IS NOT NULL AND REG_CUST_DOC_TYPE IN ('CUIL','CUIT') AND (left(REG_CUST_DOC_NUMBER,1)='3' OR left(REG_CUST_DOC_NUMBER,1)='55' ) AND sit_site_id= 'MLA')
) WITH data primary index (CUS_CUST_ID) on commit preserve rows;


CREATE MULTISET VOLATILE TABLE d01_cust AS (
SELECT distinct a.cus_cust_id from d00_cust a
LEFT JOIN LK_CUS_CUSTOMERS_DATA b
ON a.cus_cust_id = b.cus_cust_id
WHERE (COALESCE(b.CUS_TAGS, '') <> 'sink')
) WITH data primary index (CUS_CUST_ID) on commit preserve rows;


----------------------------- 00. Uno datos Mercado Pago y Marketplace  ------------------------------------------

/* Comment Tomas: Traigo todos los datos de Mercado Pago de ventas por seller y canal */

CREATE multiset volatile TABLE TPV_SEL_1 AS (
SELECT
  mp.cus_cust_id_sel cus_cust_id_sel, --- numero de usuario
  mp.sit_site_id sit_site_id, --- site
  tpv_segment_id tpv_segment_id, --- Segmento de donde vende
  tpv_segment_detail tpv_segment_detail, --- + detalle
  SUM (PAY_TRANSACTION_DOL_AMT) VENTAS_USD, --- suma volumen de ventas
  SUM (1) Q -- para contar la cantidad de segmentos que se vende   COUNT (tpv_segment_id) Q2
FROM WHOWNER.BT_MP_PAY_PAYMENTS mp
WHERE mp.tpv_flag = 1
AND mp.sit_site_id IN ('MLA')
AND MP.PAY_MOVE_DATE BETWEEN DATE '2020-01-01' AND DATE '2020-12-31'
AND mp.pay_status_id IN ( 'approved')--, 'authorized')
AND tpv_segment_id <> 'ON'--AND coalesce(i.ITE_TIPO_PROD,0) <> 'U' -----> Saco todo lo que es Marketplace
AND cus_cust_id_sel FROM LK_CUS_CUSTOMERS_DATA WHERE CUS_CUST_ID IN (SELECT * FROM d01_cust)
GROUP BY 1,2,3,4

UNION --- para poder hacer union las dos tablas son iguales

/* Comment Tomas: Traigo todos los datos de Marketplace */

SELECT
  bid.cus_cust_id_sel cus_cust_id_sel,  
  bid.sit_site_id sit_site_id,
  'Selling ML' tpv_segment_id,
  'Selling ML' tpv_segment_detail,
  SUM((CASE WHEN coalesce(bid.BID_FVF_BONIF, 'Y') = 'N' THEN (bid.BID_BASE_CURRENT_PRICE * bid.BID_QUANTITY_OK) ELSE 0.0 END))  VENTAS_USD,
  SUM (1) Q
FROM WHOWNER.BT_BIDS as bid
where bid.sit_site_id IN ('MLA')
AND bid.photo_id = 'TODATE'
AND bid.tim_day_winning_date BETWEEN DATE '2020-01-01' AND DATE '2020-12-31'
AND bid.ite_gmv_flag = 1
AND bid.mkt_marketplace_id = 'TM'
AND coalesce(BID.AUTO_OFFER_FLAG, 0) <> 1
AND coalesce(bid.BID_FVF_BONIF, 'Y') = 'N'
AND cus_cust_id_sel FROM LK_CUS_CUSTOMERS_DATA WHERE CUS_CUST_ID IN (SELECT * FROM d01_cust)

GROUP BY 1,2,3,4

) WITH data primary index (CUS_CUST_ID_SEL,SIT_SITE_ID) on commit preserve rows;

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


----------------------------- 02. Crea la tabla company/not company para filtrar cus_id ---------------------------------------
create multiset volatile table BASE_CUST_2 as (
select
d.CUS_CUST_ID
d.sit_site_id_CUS,

CASE WHEN S.tpv_segment_detail ='Aggregator - Other' then 'Online Payments' 
when S.tpv_segment_detail ='Instore' then 'QR'
when S.tpv_segment_detail ='Selling Marketplace' then 'Selling Marketplace'
when S.tpv_segment_detail ='Point' then 'Point'
when S.tpv_segment_detail is null then 'No Vende'
else 'Not Considered'end as Canal, -- cambio el nombre de segmento
S.SEGMENTO SEGMENTO_MKTPLACE,

D.CUS_STATE ESTADO,


case  when d.cus_internal_tags like '%internal_user%' or d.cus_internal_tags like '%internal_third_party%' then 'MELI-1P/PL'
    when d.cus_internal_tags like '%cancelled_account%' then 'Cuenta_ELIMINADA'
     when d.cus_internal_tags like '%operators_root%' then 'Operador_Root'
     when d.cus_internal_tags like '%operator%' then 'Operador'
      ELSE 'OK' end as CUSTOMER,

(CASE WHEN  (d.sit_site_id_cus='MLA') THEN
             CASE   
              when R.REG_CUST_DOC_NUMBER IS NOT NULL and R.REG_CUST_DOC_TYPE IN ('CUIL','CUIT') and left(R.REG_CUST_DOC_NUMBER,1)='3' THEN 'Company'
                 when R1.REG_DATA_TYPE='company' then 'Company' 
                 when R1.REG_DATA_TYPE IS NULL AND R.REG_CUST_DOC_NUMBER IS NOT NULL and R.REG_CUST_DOC_TYPE IN ('CUIL','CUIT') and left(R.REG_CUST_DOC_NUMBER,1)='3' THEN 'Company'
                  WHEN D.cus_cust_doc_type IN ('CUIL','CUIT') and (left(D.cus_cust_doc_number,1)='3'  OR left(D.cus_cust_doc_number,1)='55') THEN 'Company'
                  WHEN KYC.cus_doc_type IN ('CUIL','CUIT') and left(KYC.cus_doc_number,1)='3' THEN 'Company'
                  WHEN KYC2.CUS_KYC_ENTITY_TYPE IN('company') THEN 'Company'
                 ELSE 'Not Company' END
        WHEN (d.sit_site_id_cus='MLB') THEN
             CASE   
                  when R1.REG_DATA_TYPE='company' then 'Company' 
                  WHEN R1.reg_cust_doc_type In ('CNPJ') THEN 'Company'
                  WHEN D.cus_cust_doc_type In ('CNPJ') THEN 'Company'
                  WHEN KYC.cus_doc_type In ('CNPJ') THEN 'Company'
                  WHEN KYC2.CUS_KYC_ENTITY_TYPE IN('company') THEN 'Company'
                  ELSE 'Not Company' END

         WHEN  (d.sit_site_id_cus='MLM') THEN
             CASE   
                  WHEN D.cus_cust_doc_type In ('RFC') and length( D.cus_cust_doc_number) = 12  THEN 'Company'
                  WHEN KYC.cus_doc_type In ('RFC') and (length( KYC.cus_doc_number)= 12 or length( KYC.CUS_BUSINESS_DOC)= 12)   THEN 'Company'
                  WHEN KYC2.CUS_KYC_ENTITY_TYPE IN('company') THEN 'Company'
                 -- WHEN KYC.cus_doc_type In ('RFC') and (length( KYC.cus_doc_number)= 13  or length( KYC.CUS_BUSINESS_DOC)= 13 )  THEN 'Not Company'
                  --WHEN D.cus_cust_doc_type In ('RFC') and length( D.cus_cust_doc_number) = 13  THEN 'Not Company'
                  ELSE 'Not Company' END
                  
         WHEN  (d.sit_site_id_cus='MLC') THEN
               CASE
               WHEN KYC2.CUS_KYC_ENTITY_TYPE IN('company') THEN 'Company'
               when R1.REG_DATA_TYPE='company' then 'Company' 
               WHEN cast(regexp_substr(regexp_replace(D.cus_cust_doc_number,'[.$+*/&¿?! ]'),'^[0-9]+') AS bigint) between 50000000 and 9999999 THEN 'Company'
               ELSE 'Not Company' END
        ELSE 'ERROR_SITE'

        END) AS REG_DATA_TYPE_group, -- tipo documento

 case 
     when v.VENTAS_USD is null then 'a.No Vende'
     when v.VENTAS_USD= 0 then 'a.No Vende'
     when v.VENTAS_USD <= 6000 then 'b.Menos 6.000'
     when v.VENTAS_USD <= 40000 then 'c.6.000 a 40.000'
     when v.VENTAS_USD<= 200000 then 'd.40.000 a 200.000'
     else 'e.Mas de 200.000' end as RANGO_VTA_PURO, 

     
Case when REG_DATA_TYPE_group='Company' and Canal<>'Not Considered' then 'ok'
 when REG_DATA_TYPE_group <>'Company' and Canal<>'Not Considered' and RANGO_VTA_PURO not in ('a.No Vende','b.Menos 6.000')  then 'ok'
 else 'no ok'end as Baseline,
     
FROM WHOWNER.LK_CUS_CUSTOMERS_DATA D  -- base de clientes
LEFT JOIN TPV_SEL AS V ON v.CUS_CUST_ID_SEL=D.CUS_CUST_ID and v.sit_site_id=d.sit_site_id_cus 
LEFT JOIN Seg_Sel  as s on v.sit_site_id=s.sit_site_id and v.cus_cust_id_sel=s.cus_cust_id_sel

LEFT JOIN WHOWNER.LK_REG_CUSTOMERS R ON d.CUS_CUST_ID=R.CUS_CUST_ID and d.sit_site_id_cus=r.sit_site_id
LEFT JOIN WHOWNER.LK_REG_PERSON R1 ON R.REG_CUST_DOC_TYPE = R1.REG_CUST_DOC_TYPE and R.REG_CUST_DOC_NUMBER = R1.REG_CUST_DOC_NUMBER and r1.sit_site_id=r.sit_site_id
LEFT JOIN WHOWNER.LK_KYC_CUSTOMERS KYC ON KYC.CUS_CUST_ID=D.CUS_CUST_ID
LEFT JOIN WHOWNER.BT_MP_KYC_LEVEL KYC2 ON  KYC2.CUS_CUST_ID=D.CUS_CUST_ID

where COALESCE(D.CUS_TAGS, '') <> 'sink'
AND D.sit_site_id_cus in ('MLA') --IN ('MLA','MLB','MLM')

having Baseline='ok'

) with data primary index (cus_cust_id,SIT_SITE_ID_cus) on commit preserve rows;