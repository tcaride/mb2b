
---- Query Segmentacion Seller Buyer ---



create multiset volatile table TPV_SEL_1 as ( ---- ventas por canal

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
mp.sit_site_id IN ('MLB')
and MP.PAY_MOVE_DATE between DATE '2020-01-01' and DATE '2020-12-31'
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
where bid.sit_site_id IN ('MLB')
and bid.photo_id = 'TODATE' 
and bid.tim_day_winning_date between DATE '2020-01-01' and DATE '2020-12-31'
and bid.ite_gmv_flag = 1
and bid.mkt_marketplace_id = 'TM'
AND coalesce(BID.AUTO_OFFER_FLAG, 0) <> 1 
AND coalesce(bid.BID_FVF_BONIF, 'Y') = 'N'
group by 1,2,3,4

) with data primary index (CUS_CUST_ID_SEL,SIT_SITE_ID) on commit preserve rows;




create multiset volatile table TPV_SEL as ( --- sumo los volumenes por seller --- volumen total por seller

select
cus_cust_id_sel,
sit_site_id,
sum(VENTAS_USD) VENTAS_USD,
sum(Q) Q,
count(distinct tpv_segment_id) Q_SEG

from TPV_SEL_1
GROUP BY 1,2
) with data primary index (CUS_CUST_ID_SEL,SIT_SITE_ID) on commit preserve rows;

---- query para tener el segmento de MP, tomo el mayor puede que haya seller en mas de un segmento 


create multiset volatile table SEG_SEL as ( --- me quedo con el segmento del seller con mas ventas --- segmento por seller
select
SP.cus_cust_id_sel, 
SP.sit_site_id,
SP.tpv_segment_id,
SP.tpv_segment_detail,
sm.SEGMENTO,

VENTAS_USD

from TPV_SEL_1 sp
left join WHOWNER.LK_SEGMENTO_SELLERS  SM on SP.sit_site_id=sM.sit_site_id and SP.cus_cust_id_sel=sM.cus_cust_id_sel
qualify row_number () over (partition by sp.cus_cust_id_sel, sp.sit_site_id order by VENTAS_USD DESC) = 1
group by 1,2,3,4,5,6

) with data primary index (CUS_CUST_ID_SEL,SIT_SITE_ID) on commit preserve rows;



---------- bgmv buyer

CREATE MULTISET VOLATILE TABLE BGMV_BUY AS ( --- compras en el marketplace por usuario 
select
    BID.cus_cust_id_buy,  
       bid.sit_site_id,
       
         sum((case when tgmv_flag = 1 and cat.cat_categ_name_l1 in ('Informática','Ferramentas e Construção','Indústria e Comércio','Eletrônicos, Áudio e Vídeo','Arte, Papelaria e Armarinho') then (bid.BID_BASE_CURRENT_PRICE * bid.BID_QUANTITY_OK) else 0.0 end )) TGMV_COMP,
       sum((case when tgmv_flag = 1 and cat.cat_categ_name_l1 in ('Acessórios para Veículos') then (bid.BID_BASE_CURRENT_PRICE * bid.BID_QUANTITY_OK) else 0.0 end )) TGMV_AUTO,
       sum((case when tgmv_flag = 1 and cat.cat_categ_name_l1 in ('Beleza e Cuidado Pessoal') then (bid.BID_BASE_CURRENT_PRICE * bid.BID_QUANTITY_OK) else 0.0 end )) TGMV_BEAUTY,
       sum((case when tgmv_flag = 1 and cat.cat_categ_name_l1 in ('Informática','Eletrônicos, Áudio e Vídeo','Eletrodomésticos','Celulares e Telefones','Câmeras e Acessórios') then (bid.BID_BASE_CURRENT_PRICE * bid.BID_QUANTITY_OK) else 0.0 end))  TGMV_CE,
       
       
    sum((Case when tgmv_flag = 1 then (bid.BID_BASE_CURRENT_PRICE * bid.BID_QUANTITY_OK) else 0.0 end))  GMVEBILLABLE,
      COUNT((Case when  tgmv_flag = 1 then BID.BID_QUANTITY_OK else 0.0 end))  TORDERS_BUY,
   sum((Case when  tgmv_flag = 1 then BID.BID_QUANTITY_OK else 0.0 end))  TSIE_BUY,
  COUNT(distinct (case when bid.crt_purchase_id is null then bid.ord_order_id else bid.crt_purchase_id end)) as TX_BUY
    
    --sum( (bid.BID_BASE_CURRENT_PRICE * bid.BID_QUANTITY_OK) )  GMVE
from WHOWNER.BT_BIDS as bid

left join WHOWNER.LK_ITE_ITEMS_PH ite
on 	(bid.ITE_ITEM_ID = ite.ITE_ITEM_ID and     
bid.PHOTO_ID = ite.PHOTO_ID and     
bid.SiT_SITE_ID = ite.SIT_SITE_ID)   
	
left join WHOWNER.AG_LK_CAT_CATEGORIES_PH as cat    
on (ite.sit_site_id = cat.sit_site_id and ite.cat_Categ_id = cat.cat_Categ_id_l7 and cat.photo_id = 'TODATE')   

where bid.sit_site_id IN ('MLB')
and bid.photo_id = 'TODATE' 
and bid.tim_day_winning_date between DATE '2020-01-01' and DATE '2020-12-31'
and bid.ite_gmv_flag = 1
and bid.mkt_marketplace_id = 'TM'
and tgmv_flag = 1
--AND coalesce(BID.AUTO_OFFER_FLAG, 0) <> 1 
--AND coalesce(bid.BID_FVF_BONIF, 'Y') = 'N'
group by 1,2

)WITH DATA PRIMARY INDEX (sit_site_id,cus_cust_id_buy) ON COMMIT PRESERVE ROWS;





create multiset volatile table br_mx as ( -- tipo de regimen
select
cus_cust_id,
sit_site_id,
cus_tax_payer_type,
cus_tax_regime
from LK_TAX_CUST_WRAPPER
qualify row_number () over (partition by cus_cust_id, sit_site_id order by  aud_upd_dt DESc) = 1
) with data primary index (sit_site_id,cus_cust_id) on commit preserve rows;


Create multiset volatile table lastmcc3 as ( --- rubro
SELECT *
FROM 
(
  SELECT 
    MCC.cus_cust_id, 
    sit_site_id,
    MCC.mcc_last_adding_datetime,
    CASE WHEN (MCC.mcc_last_adding_datetime = MAX(MCC.mcc_last_adding_datetime) OVER (PARTITION BY cus_cust_id)) THEN 'Y' ELSE 'N' END AS MAX_NUM_IND,
     mcc_id
  FROM 
    WHOWNER.LK_CUS_CUSTOMER_MCC MCC
    where mcc_source='USER_ASSIGN'
    
) DAT2
WHERE MAX_NUM_IND = 'Y'
qualify row_number () over (partition by  cus_cust_id, sit_site_id order by mcc_last_adding_datetime DESC) = 1

) with data primary index (cus_cust_id) on commit preserve rows;

-- DROP TABLE lastmcc4;
Create multiset volatile table lastmcc4 as (
select
y.cus_cust_id,
sit_site_id,
y.MCC_ID MCC_ID,
x.mcc_description MCC1,
case when MCC1 in ('Automotive Service Shops (Non-Dealer)','Car Washes','Parking Lots and Garages') then 'ACC'
when MCC1 in ('Auto Parts and Accessories Stores','Car and Truck Dealers (New and Used) Sales Service Repairs Parts and Leasing','Motor Vehicle Supplies and New Parts','Motorcycle Shops and Dealers','ACC') then 'ACC'
when MCC1 in ('Family Clothing Stores','Precious Stones and Metals Watches and Jewelry','Sewing Needlework Fabric and Piece Goods Stores','Shoe Stores','APPAREL') then 'APPAREL'
when MCC1 in ('Beauty and Barber Shops','Cosmetic Stores','Health and Beauty Spas') then 'APPAREL_BEAUTY'
when MCC1 in ('Book Stores','Books Periodicals and Newspapers','News Dealers and Newsstands','Music Stores - Musical Instruments Pianos and Sheet Music') then 'ENTERTAINMENT'
when MCC1 in ('Computer Maintenance and Repair Services - Not Elsewhere Classified','Computer Software Stores','Durable Goods - Not Elsewhere Classified','Electrical Parts and Equipment','Electronics Stores','Hardware Equipment and Supplies','Household Appliance Stores','CE') then 'CE'
when MCC1 in ('Advertising Services','Architectural Engineering and Surveying Services','Business Services -Not Elsewhere Classified','Detective Agencies Protective Agencies and Security Services including Armored Cars and Guard Dogs','Direct Marketing/Direct Marketers - Not Elsewhere Classified','Dry Cleaners','Funeral Services and Crematories','Insurance Sales Underwriting and Premiums','Legal Services and Attorneys','Management Consulting and Public Relations Services','Miscellaneous Personal Services - Not Elsewhere Classified','Miscellaneous Publishing and Printing Services','Motion Picture Theaters','Photographic Studios','Quick Copy Reproduction and Blueprinting Services','Real Estate (e,g, rent)','Theatrical Producers (except Motion Pictures) and Ticket Agencies','Travel Agencies and Tour Operators','Typesetting Plate Making and Related Services','Electrical Contractors','General Contractors - Residential and Commercial','Masonry Stonework Tile-Setting Plastering and Insulation Contractors','Metal Service Centers and Offices','Roofing Siding and Sheet Metal Work Contractors','Courier Services - Air and Ground and Freight Forwarders','Motor Freight Carriers and Trucking - Local and Long Distance Moving and Storage Companies and Local Delivery','Fuel Dealers - Fuel Oil Wood Coal and Liquefied Petroleum','Cleaning Maintenance and Janitorial Services') then 'CONSULTING & SERVICES'
when MCC1 in ('Grocery Stores and Supermarkets','Miscellaneous Food Stores - Convenience Stores and Specialty Markets','Package Stores - Beer Wine and Liquor','CPG') then 'CPG'
when MCC1 in ('Construction Materials - Not Elsewhere Classified','Furniture Home Furnishings and Equipment Stores and Manufacturers except Appliances','Lumber and Building Materials Stores','Miscellaneous Home Furnishings Specialty Stores','HOME & INDUSTRIES','Lodging Hotels Motels and Resorts','Stationery Office Supplies Printing and Writing Paper') then 'HOME & INDUSTRIES'
when MCC1 in ('Dentists and Orthodontists','Doctors and Physicians - Not Elsewhere Classified','Hospitals','Medical Services and Health Practitioners - Not Elsewhere Classified','Dental/Laboratory/Medical/ Ophthalmic Hospital Equipment and Supplies','Drug Stores and Pharmacies','Opticians Optical Goods and Eyeglasses') then 'APPAREL_MEDICAL'
when MCC1 in ('Civic Social and Fraternal Associations','Membership Clubs (Sports Recreation Athletic) Country Clubs and Private Golf Courses','Religious Organizations','Child Care Services','Political Organizations','Recreation Services - Not Elsewhere Classified','Schools and Educational Services - Not Elsewhere Classified','Tourist Attractions and Exhibits','Charitable and Social Service Organizations') then 'SOCIAL'
when MCC1 in ('Bicycle Shops - Sales and Service','Sporting Goods Stores') then 'APPAREL_SPORTS'
when MCC1 in ('Hobby Toy and Game Stores') then 'TOYS & BABIES'
when MCC1 in ('Bus Lines','Taxicabs and Limousines','Transportation Services - Not Elsewhere Classified') then 'TRAVEL'
ELSE 'OTROS' END AS MCC,

x.mcc_category mcc_group
--COUNT(*)

from lastmcc3 y 
left join lk_mp_mcc_code x on cast(y.MCC_ID as varchar(4))=cast(x.mcc_code as varchar(4))
group by 1,2,3,4,5,6
--having count(*)>1

) with data primary index (cus_cust_id) on commit preserve rows;


create multiset volatile table vert as ( -- vertical maxima

select
      bid.sit_site_id,
    bid.cus_cust_id_sel,
     cat.vertical,
     sum((Case when coalesce(bid.BID_FVF_BONIF, 'Y') = 'N' then (BID.BID_base_CURRENT_PRICE * bid.BID_QUANTITY_OK) else 0.0 end))  GMVEBILLABLE 
from WHOWNER.BT_BIDS as bid
left join WHOWNER.LK_ITE_ITEMS_PH ite
on 	(bid.ITE_ITEM_ID = ite.ITE_ITEM_ID and     
bid.PHOTO_ID = ite.PHOTO_ID and     
	bid.SiT_SITE_ID = ite.SIT_SITE_ID)   

left join WHOWNER.AG_LK_CAT_CATEGORIES_PH as cat    
on (ite.sit_site_id = cat.sit_site_id and ite.cat_Categ_id = cat.cat_Categ_id_l7 and cat.photo_id = 'TODATE')   

left join WHOWNER.LK_SEGMENTO_SELLERS seg
on bid.cus_cust_id_sel=seg.cus_cust_id_sel and seg.sit_site_id=bid.sit_site_id


left join WHOWNER.LK_ITE_ATTRIBUTE_VALUES att
on bid.ITE_ITEM_ID = att.ITE_ITEM_ID and     
bid.PHOTO_ID = ite.PHOTO_ID and     
bid.SiT_SITE_ID = att.SIT_SITE_ID
	


where bid.sit_site_id IN ('MLB')
and bid.photo_id = 'TODATE' 
and bid.tim_day_winning_date between DATE '2020-01-01' and DATE '2020-12-31'
and bid.ite_gmv_flag = 1
and bid.mkt_marketplace_id = 'TM'
AND coalesce(BID.AUTO_OFFER_FLAG, 0) <> 1 
AND coalesce(bid.BID_FVF_BONIF, 'Y') = 'N'

group by 1,2,3

) with data primary index (sit_site_id,cus_cust_id_sel) on commit preserve rows;


create multiset volatile table vert2 as ( -- vertical maxima
select
cus_cust_id_sel,
sit_site_id,
vertical,
GMVEBILLABLE
from vert
--group by 1,2,3
qualify row_number () over (partition by cus_cust_id_sel, sit_site_id order by  GMVEBILLABLE DESc) = 1
) with data primary index (sit_site_id,cus_cust_id_sel) on commit preserve rows;



create multiset volatile table vert3 as ( --- cantidad de verticales de vta
select
cus_cust_id_sel,
sit_site_id,
COUNT(distinct VERTICAL) AS CANT_VERT_VTA
from vert
GROUP BY 1,2 
) with data primary index (sit_site_id,cus_cust_id_sel) on commit preserve rows;






--- tipo comprador

CREATE MULTISET VOLATILE TABLE BGMV_TIPO_COMPRADOR AS ( --- compras en el marketplace por usuario 
select
    BID.cus_cust_id_buy,  
       bid.sit_site_id,
      
      ((CAST(EXTRACT(MONTH FROM bid.TIM_DAY_WINNING_DATE) AS BYTEINT)-1)/3)+1
  || 'Q' || substring(bid.TIM_DAY_WINNING_DATE,3,2) quarter,
       
       
    --sum((Case when tgmv_flag = 1 then (bid.BID_BASE_CURRENT_PRICE * bid.BID_QUANTITY_OK) else 0.0 end))  GMVEBILLABLE,
      ---COUNT((Case when  tgmv_flag = 1 then BID.BID_QUANTITY_OK else 0.0 end))  TORDERS_BUY,
   -- sum((Case when  tgmv_flag = 1 then BID.BID_QUANTITY_OK else 0.0 end))  TSIE_BUY,
  COUNT(distinct (case when bid.crt_purchase_id is null then bid.ord_order_id else bid.crt_purchase_id end)) as TX_BUY
    
    --sum( (bid.BID_BASE_CURRENT_PRICE * bid.BID_QUANTITY_OK) )  GMVE
from WHOWNER.BT_BIDS as bid

left join WHOWNER.LK_ITE_ITEMS_PH ite
on 	(bid.ITE_ITEM_ID = ite.ITE_ITEM_ID and     
bid.PHOTO_ID = ite.PHOTO_ID and     
bid.SiT_SITE_ID = ite.SIT_SITE_ID)   
	
left join WHOWNER.AG_LK_CAT_CATEGORIES_PH as cat    
on (ite.sit_site_id = cat.sit_site_id and ite.cat_Categ_id = cat.cat_Categ_id_l7 and cat.photo_id = 'TODATE')  



where bid.sit_site_id IN ('MLB')
and bid.photo_id = 'TODATE' 
and bid.tim_day_winning_date between DATE '2020-01-01' and DATE '2020-12-31'
and bid.ite_gmv_flag = 1
and bid.mkt_marketplace_id = 'TM'
and tgmv_flag = 1
--AND coalesce(BID.AUTO_OFFER_FLAG, 0) <> 1 
--AND coalesce(bid.BID_FVF_BONIF, 'Y') = 'N'
group by 1,2,3

)WITH DATA PRIMARY INDEX (sit_site_id,cus_cust_id_buy) ON COMMIT PRESERVE ROWS;



CREATE MULTISET VOLATILE TABLE BGMV_TIPO_COMPRADOR_2 AS (

SELECT
  tcb.cus_cust_id_buy,  
  tcb.sit_site_id,
   case when cdt.cus_first_buy_no_bonif_autoof <= '2020-01-01' then 'OK'
  WHEN cdt.cus_first_buy_no_bonif_autoof <= '2020-03-31' then '3Q'
  WHEN cdt.cus_first_buy_no_bonif_autoof <= '2020-06-30' then '2Q'
   WHEN cdt.cus_first_buy_no_bonif_autoof <= '2020-09-30' then '1Q'
   ELSE 'Menos 1Q'  end as Q_cuenta,
  
  count(distinct quarter) cant_q_compras
  
 
  
from BGMV_TIPO_COMPRADOR tcb
LEFT JOIN WHOWNER.LK_CUS_CUSTOMER_DATES CDT ON CDT.CUS_CUST_ID=tcb.CUS_CUST_ID_BUY AND CDT.sit_site_id=tcb.SIT_SITE_ID
group by 1,2,3

)WITH DATA PRIMARY INDEX (sit_site_id,cus_cust_id_buy) ON COMMIT PRESERVE ROWS;





create multiset volatile table BASE_CUST as (

select
D.cus_cust_id,
d.sit_site_id_CUS,


case  when d.cus_internal_tags like '%internal_user%' or d.cus_internal_tags like '%internal_third_party%' then 'MELI-1P/PL'
    when d.cus_internal_tags like '%cancelled_account%' then 'Cuenta_ELIMINADA'
     when d.cus_internal_tags like '%operators_root%' then 'Operador_Root'
     when d.cus_internal_tags like '%operator%' then 'Operador'
      ELSE 'OK' end as CUSTOMER,


 (CASE --WHEN  (d.sit_site_id_cus='MLA') THEN
         --     CASE   
           --    when R.REG_CUST_DOC_NUMBER IS NOT NULL and R.REG_CUST_DOC_TYPE IN ('CUIL','CUIT') and left(R.REG_CUST_DOC_NUMBER,1)='3' THEN 'Company'
             --     when R1.REG_DATA_TYPE='company' then 'Company' 
               --   when R1.REG_DATA_TYPE IS NULL AND R.REG_CUST_DOC_NUMBER IS NOT NULL and R.REG_CUST_DOC_TYPE IN ('CUIL','CUIT') and left(R.REG_CUST_DOC_NUMBER,1)='3' THEN 'Company'
                 -- WHEN D.cus_cust_doc_type IN ('CUIL','CUIT') and left(D.cus_cust_doc_number,1)='3' THEN 'Company'
                  --WHEN KYC.cus_doc_type IN ('CUIL','CUIT') and left(KYC.cus_doc_number,1)='3' THEN 'Company'
                  --WHEN KYC.cus_doc_type IN ('CUIL') and left(KYC.cus_doc_number,1)='2' THEN 'person'
                 -- WHEN KYC.cus_doc_type IN ('DNI','CI','LC','LE') THEN 'person'
                 --WHEN D.cus_cust_doc_type IN ('CUIT') and left(D.cus_cust_doc_number,1)='2' THEN 'Not Company'
                  --WHEN KYC.cus_doc_type IN ('CUIT') and left(KYC.cus_doc_number,1)='2' THEN 'Not Company'
                --  ELSE 'Not Company' END
        WHEN (d.sit_site_id_cus='MLB') THEN
             CASE   
             
                  when R1.REG_DATA_TYPE='company' then 'Company' 
                  WHEN R1.reg_cust_doc_type In ('CNPJ') THEN 'Company'
                  WHEN D.cus_cust_doc_type In ('CNPJ') THEN 'Company'
                 -- WHEN KYC.cus_doc_type In ('CNPJ') THEN 'Company'
                  ELSE 'Not Company' END

         --WHEN  (d.sit_site_id_cus='MLM') THEN
           --  CASE   
             --     WHEN D.cus_cust_doc_type In ('RFC') and length( D.cus_cust_doc_number) = 12  THEN 'Company'
               --   WHEN KYC.cus_doc_type In ('RFC') and (length( KYC.cus_doc_number)= 12 or length( KYC.CUS_BUSINESS_DOC)= 12)   THEN 'Company'
                  --WHEN KYC.cus_doc_type In ('RFC') and (length( KYC.cus_doc_number)= 13  or length( KYC.CUS_BUSINESS_DOC)= 13 )  THEN 'Not Company'
                  --WHEN D.cus_cust_doc_type In ('RFC') and length( D.cus_cust_doc_number) = 13  THEN 'Not Company'
                 -- ELSE 'Not Company' END

        ELSE 'ERROR_SITE'

        END) AS REG_DATA_TYPE_group,
        REG_CUST_BRAND_NAME,
        CASE WHEN CHARACTER_LENGTH(REGEXP_REPLACE(REG_CUST_BRAND_NAME, '[^0-9]*', ''))=11 THEN 'MEI' ELSE 'NOT MEI' END AS TIPO, -- tipo documento

tax.cus_tax_payer_type, 
--tax.cus_tax_regime,
case when cdt.cus_first_buy_no_bonif_autoof is null then 'Nunca Compro' else 'Compro' end as NB,

COUNT(*) cuenta

FROM WHOWNER.LK_CUS_CUSTOMERS_DATA D  -- base de clientes
LEFT JOIN WHOWNER.LK_REG_CUSTOMERS R ON d.CUS_CUST_ID=R.CUS_CUST_ID and d.sit_site_id_cus=r.sit_site_id
LEFT JOIN WHOWNER.LK_REG_PERSON R1 ON R.REG_CUST_DOC_TYPE = R1.REG_CUST_DOC_TYPE and R.REG_CUST_DOC_NUMBER = R1.REG_CUST_DOC_NUMBER and r1.sit_site_id=r.sit_site_id
-- LEFT JOIN WHOWNER.LK_KYC_CUSTOMERS KYC ON KYC.CUS_CUST_ID=D.CUS_CUST_ID
LEFT JOIN br_mx tax on tax.CUS_CUST_ID=D.CUS_CUST_ID and tax.sit_site_id=d.sit_site_id_cus
LEFT JOIN WHOWNER.LK_CUS_CUSTOMER_DATES CDT ON CDT.CUS_CUST_ID=d.CUS_CUST_ID AND CDT.sit_site_id=d.SIT_SITE_ID_cus

where COALESCE(D.CUS_TAGS, '') <> 'sink'
AND D.sit_site_id_cus in ('MLB') 
--and d.cus_internal_tags not like '%operator%' or d.cus_internal_tags not like '%cancelled_account%'
group by 1,2,3,4,5,6,7,8
-- having  REG_DATA_TYPE_group='Company' and tipo='MEI'


) with data primary index (cus_cust_id,SIT_SITE_ID_cus) on commit preserve rows;



create multiset volatile table intergrados as ( --- cantidad de integraciones a partir del get
select 
cus_cust_id,
sit_site_id,
count(*) CANT_INTEGRADOS
from  WHOWNER.lk_op_active_users us 
where us.date_end >=  '2020-12-31' --- check con Adri M a ver como es exacto
and us.sit_site_id in ('MLB') 
group by 1,2

) with data primary index (sit_site_id,cus_cust_id) on commit preserve rows;


create multiset volatile table account_money as ( --- cantidad de integraciones a partir del get
select 
cus_cust_id,
sit_site_id,
avg	(AVAILABLE_BALANCE) balance
from  BT_MP_SALDOS_SITE 
where TIM_DAY between DATE '2020-01-01' and DATE '2020-12-31'
and sit_site_id IN ('MLB')
group by 1,2

) with data primary index (sit_site_id,cus_cust_id) on commit preserve rows;

create multiset volatile table account_money2 as (
SELECT
CASE WHEN cus_cust_id_sel IS NULL THEN CUS_CUST_ID ELSE cus_cust_id_sel END AS cus_cust_id ,
CASE WHEN v.sit_site_id IS NULL THEN am.sit_site_id ELSE v.sit_site_id  END AS sit_site_id,

case when v.VENTAS_USD is null or (v.VENTAS_USD/365*5)=0 then 'a.No Vende'
when am.balance/(v.VENTAS_USD/365*5) <= 1 THEN 'b.Menos d lo que vende'
when am.balance/(v.VENTAS_USD/365*5) <= 2 THEN 'c.Menos q el doble de lo que vende'
when am.balance/(v.VENTAS_USD/365*5)  <= 5 THEN 'd.Hasta x 5 lo que vende'
when am.balance/(v.VENTAS_USD/365*5) <= 20 THEN 'e.Hasta x 20 lo que vende'
else 'F.Mas de x 20 lo que vende' end as Ratio_AM_VTAS,

case when am.balance is null or am.balance=0 then 'No tiene AM'
when  am.balance<100 then 'Menos 100 Reales'
when am.balance<=500 then '100 a 500 Reales'
when am.balance<=1500 then '500 a 1500 Reales'
when am.balance<=5000 then '1500 a 5000 Reales'
when am.balance<=15000 then '5000 a 15000 Reales'
when am.balance<=50000 then '15000 a 50000 Reales'
else 'Mas de 50000 Reales' end as ACCOUNT_MONEY

from TPV_SEL AS V
 full outer JOIN account_money am on am.CUS_CUST_ID=V.CUS_CUST_ID_SEL and am.sit_site_id=V.sit_site_id

) with data primary index (sit_site_id,cus_cust_id) on commit preserve rows;






create multiset volatile table credits as (
SELECT
CUS_CUST_ID_BORROWER CUS_CUST_ID,
SIT_SITE_ID,
count(*) total
FROM WHOWNER.BT_MP_CREDITS
WHERE CRD_CREDIT_FINISH_DATE_ID >= DATE '2020-01-01'
and sit_site_id='MLB'
group by 1,2
) with data primary index (sit_site_id,cus_cust_id) on commit preserve rows;


create multiset volatile table seguros as (
SELECT
CUS_CUST_ID_buy,
count(*) total
FROM WHOWNER.BT_INSURANCE_PURCHASES
WHERE INSUR_STATUS_ID = 'confirmed'
and SIT_SITE_ID='mlb'
group by 1
) with data primary index (cus_cust_id_buy) on commit preserve rows;


create multiset volatile table seller_shipping as (

select
bid.sit_site_id,
bid.cus_cust_id_sel cus_cust_id_sel,
count(*) total

from WHOWNER.BT_BIDS as bid

left join WHOWNER.BT_SHP_SHIPMENTS as shp 
on bid.shp_shipment_id = shp.shp_shipment_id and shp.sit_site_id = bid.sit_site_id

where bid.sit_site_id = 'MLB'--- ('MCO', 'MLA', 'MLB', 'MLC', 'MLM', 'MLU', 'MPE')
and bid.photo_id = 'TODATE' 
and bid.tim_day_winning_date between DATE '2020-10-01' and DATE '2020-12-31' --OR bid.tim_day_winning_date between DATE '2019-01-01' and '2019-10-31')
and bid.ite_gmv_flag = 1
and bid.mkt_marketplace_id = 'TM'
and shp.shp_picking_type_id in ('xd_drop_off','cross_docking','fulfillment') 

and tgmv_flag = 1

group by 1,2

) with data primary index (sit_site_id,cus_cust_id_SEL) on commit preserve rows;






SELECT

d.sit_site_id_CUS,
--d.cus_cust_id,

CASE WHEN S.tpv_segment_detail ='Aggregator - Other' then 'Online Payments' 
when S.tpv_segment_detail ='Instore' then 'QR'
when S.tpv_segment_detail ='Selling ML' then 'Selling ML'
when S.tpv_segment_detail ='Point' then 'Point'
when S.tpv_segment_detail is null then 'No Vende'
else 'Not Considered'end as Canal, -- cambio el nombre de segmento
S.SEGMENTO SEGMENTO_MKTPLACE,


CASE WHEN S.tpv_segment_detail ='Selling ML' THEN vt.vertical ELSE y.MCC END AS RUBRO,  -- cambiar rubro 


D.CUSTOMER,

D.REG_DATA_TYPE_group, --- tipo documento

d.cus_tax_payer_type,
d.tipo,
--d.cus_tax_regime,

CASE when BB.GMVEBILLABLE IS NULL THEN 'No Compra' else 'Compra' end  as TIPO_COMPRADOR_TGMV, 

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
     
 case when  ing.cus_cust_id is null then 'Sin Integrador'
 when ing.CANT_INTEGRADOS = 1 then 'Integrador Unico'  
 when ing.CANT_INTEGRADOS <= 3 then '2 a 3 Integradores'
 when ing.CANT_INTEGRADOS <= 10 then '4 a 10 Integradores'
 else 'Mas de 10 integradores' end as INTEGRACION, 
  
  
 AM.ACCOUNT_MONEY,

 AM.Ratio_AM_VTAS,

     
-- LYL.LYL_BUYER_SEGMENT SEGMENTO_BUY,
LYL.LYL_LEVEL_NUMBER LOYALTY,
--LYL.Q_MONTHS_WITH_PURCHASES,

 v.Q_SEG ECOSISTEMA,
  case when seg.cus_cust_id_buy is null then 0 else 1 end as SEGUROS,
 case when cred.cus_cust_id is null then 0 else 1 end as CREDITOS,
 case when SHIP2.cus_cust_id_sel is null then 0 else 1 end as shipping,

 case when BB.GMVEBILLABLE>0 then
  case when bb.TGMV_COMP/BB.GMVEBILLABLE >=0.45 then 'Compras Perfil Empresa' else 'Compras Perfil No Empresa' end 
  else 'No Compras' end as Tipo_Compras,
  
case when BB.GMVEBILLABLE>0 then
case when RUBRO='ACC' and TGMV_AUTO/BB.GMVEBILLABLE >=0.40 then 'Resale'
when RUBRO='APPAREL_BEAUTY' and TGMV_BEAUTY/BB.GMVEBILLABLE >=0.40 then 'Resale' 
when RUBRO='CE' and TGMV_CE/BB.GMVEBILLABLE >=0.40 then 'Resale' else 'No Resale'end 
else 'No Compras' end as Objetivo_Compras,


case when BB.GMVEBILLABLE IS NULL and d.NB='Nunca Compro' THEN 'Not Buyer'
 when BB.GMVEBILLABLE IS NULL and d.NB='Compro' then 'Recover'
when BB.GMVEBILLABLE IS not NULL and tdc.q_cuenta in ('Menos 1Q','1Q')  and cant_q_compras >=1 then 'Frequent_NB'
when BB.GMVEBILLABLE IS not NULL and tdc.q_cuenta in ('Menos 1Q','1Q')  and cant_q_compras <1 then 'Non Frequent'
when BB.GMVEBILLABLE IS not NULL and tdc.q_cuenta='2Q' and cant_q_compras >=2 then 'Frequent_NB'
when  BB.GMVEBILLABLE IS not NULL and tdc.q_cuenta='2Q' and cant_q_compras <2 then 'Non Frequent'
when  BB.GMVEBILLABLE IS not NULL and tdc.q_cuenta='3Q' and cant_q_compras >=3 then 'Frequent_NB'
when  BB.GMVEBILLABLE IS not NULL and tdc.q_cuenta='3Q' and cant_q_compras <3 then 'Non Frequent'
when  BB.GMVEBILLABLE IS not NULL and tdc.q_cuenta='OK' and cant_q_compras >=4 then 'Frequent'
when  BB.GMVEBILLABLE IS not NULL and tdc.q_cuenta='OK' and cant_q_compras <4 then 'Non Frequent'
ELSE 'TBD'end AS Frequencia,


count(distinct case when  bb.cus_cust_id_buy is null then v.cus_cust_id_sel else bb.cus_cust_id_buy end)  as cust_total,
count(distinct bb.cus_cust_id_buy) cust_buy,
sum (v.VENTAS_USD) VENTAS_USD,
SUM (BB.GMVEBILLABLE)  bgmv_cpras


FROM BASE_CUST D  --- base de clientes
left JOIN TPV_SEL AS V ON v.CUS_CUST_ID_SEL=D.CUS_CUST_ID and v.sit_site_id=d.sit_site_id_cus --AND COALESCE(D.CUS_TAGS, '') <> 'sink'--and bid.sit_site_id=d.sit_site_id
--LEFT JOIN TPV_BUY AS b on d.cus_cust_id=b.cus_cust_id_buy and d.sit_site_id_cus=b.sit_site_id
LEFT JOIN BGMV_BUY AS BB on d.cus_cust_id=bB.cus_cust_id_buy and d.sit_site_id_cus=bB.sit_site_id --and 
left join Seg_Sel  as s on v.sit_site_id=s.sit_site_id and v.cus_cust_id_sel=s.cus_cust_id_sel
left join lastmcc4 y on d.cus_cust_id = y.cus_cust_id
left join vert2 vt on v.sit_site_id=vt.sit_site_id and v.cus_cust_id_sel=vt.cus_cust_id_sel
LEFT JOIN intergrados ing on ing.CUS_CUST_ID=D.CUS_CUST_ID and ing.sit_site_id=d.sit_site_id_cus
 LEFT JOIN WHOWNER.BT_LYL_POINTS_SNAPSHOT lyl on D.cus_cust_id = lyl.cus_cust_id and D.sit_site_id_CUS = lyl.sit_site_id and lyl.tim_month_id = '202012'
LEFT JOIN account_money2 am on am.CUS_CUST_ID=D.CUS_CUST_ID and am.sit_site_id=d.sit_site_id_cus
 LEFT JOIN seller_shipping Ship2 on ship2.CUS_CUST_ID_sel=D.CUS_CUST_ID and ship2.sit_site_id=d.sit_site_id_cus
 LEFT JOIN credits cred on cred.CUS_CUST_ID=D.CUS_CUST_ID and cred.sit_site_id=d.sit_site_id_cus
 LEFT JOIN seguros seg on seg.CUS_CUST_ID_buy=D.CUS_CUST_ID 
left join BGMV_TIPO_COMPRADOR_2 tdc on d.cus_cust_id=tdc.cus_cust_id_buy and d.sit_site_id_cus=tdc.sit_site_id

group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22
having Baseline='ok';



create multiset volatile table BASE as (

SELECT

d.sit_site_id_CUS,
d.cus_cust_id,

CASE WHEN S.tpv_segment_detail ='Aggregator - Other' then 'Online Payments' 
when S.tpv_segment_detail ='Instore' then 'QR'
when S.tpv_segment_detail ='Selling ML' then 'Selling ML'
when S.tpv_segment_detail ='Point' then 'Point'
when S.tpv_segment_detail is null then 'No Vende'
else 'Not Considered'end as Canal, -- cambio el nombre de segmento
S.SEGMENTO SEGMENTO_MKTPLACE,


CASE WHEN S.tpv_segment_detail ='Selling ML' THEN vt.vertical ELSE y.MCC END AS RUBRO,  -- cambiar rubro 


D.CUSTOMER,

D.REG_DATA_TYPE_group, --- tipo documento

d.cus_tax_payer_type, 
--d.cus_tax_regime,

CASE when BB.GMVEBILLABLE IS NULL THEN 'No Compra' else 'Compra' end  as TIPO_COMPRADOR_TGMV, 

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
     
 case when  ing.cus_cust_id is null then 'Sin Integrador'
 when ing.CANT_INTEGRADOS = 1 then 'Integrador Unico'  
 when ing.CANT_INTEGRADOS <= 3 then '2 a 3 Integradores'
 when ing.CANT_INTEGRADOS <= 10 then '4 a 10 Integradores'
 else 'Mas de 10 integradores' end as INTEGRACION, 
  
  
 AM.ACCOUNT_MONEY,

 AM.Ratio_AM_VTAS,

     
-- LYL.LYL_BUYER_SEGMENT SEGMENTO_BUY,
LYL.LYL_LEVEL_NUMBER LOYALTY,
--LYL.Q_MONTHS_WITH_PURCHASES,

 v.Q_SEG ECOSISTEMA,
  case when seg.cus_cust_id_buy is null then 0 else 1 end as SEGUROS,
 case when cred.cus_cust_id is null then 0 else 1 end as CREDITOS,
 case when SHIP2.cus_cust_id_sel is null then 0 else 1 end as shipping,

 case when BB.GMVEBILLABLE>0 then
  case when bb.TGMV_COMP/BB.GMVEBILLABLE >=0.45 then 'Compras Perfil Empresa' else 'Compras Perfil No Empresa' end 
  else 'No Compras' end as Tipo_Compras,
  
case when BB.GMVEBILLABLE>0 then
case when RUBRO='ACC' and TGMV_AUTO/BB.GMVEBILLABLE >=0.40 then 'Resale'
when RUBRO='APPAREL_BEAUTY' and TGMV_BEAUTY/BB.GMVEBILLABLE >=0.40 then 'Resale' 
when RUBRO='CE' and TGMV_CE/BB.GMVEBILLABLE >=0.40 then 'Resale' else 'No Resale'end 
else 'No Compras' end as Objetivo_Compras,


case when BB.GMVEBILLABLE IS NULL and d.NB='Nunca Compro' THEN 'Not Buyer'
 when BB.GMVEBILLABLE IS NULL and d.NB='Compro' then 'Recover'
when BB.GMVEBILLABLE IS not NULL and tdc.q_cuenta in ('Menos 1Q','1Q')  and cant_q_compras >=1 then 'Frequent_NB'
when BB.GMVEBILLABLE IS not NULL and tdc.q_cuenta in ('Menos 1Q','1Q')  and cant_q_compras <1 then 'Non Frequent'
when BB.GMVEBILLABLE IS not NULL and tdc.q_cuenta='2Q' and cant_q_compras >=2 then 'Frequent_NB'
when  BB.GMVEBILLABLE IS not NULL and tdc.q_cuenta='2Q' and cant_q_compras <2 then 'Non Frequent'
when  BB.GMVEBILLABLE IS not NULL and tdc.q_cuenta='3Q' and cant_q_compras >=3 then 'Frequent_NB'
when  BB.GMVEBILLABLE IS not NULL and tdc.q_cuenta='3Q' and cant_q_compras <3 then 'Non Frequent'
when  BB.GMVEBILLABLE IS not NULL and tdc.q_cuenta='OK' and cant_q_compras >=4 then 'Frequent'
when  BB.GMVEBILLABLE IS not NULL and tdc.q_cuenta='OK' and cant_q_compras <4 then 'Non Frequent'
ELSE 'TBD'end AS Frequencia,


count(distinct case when  bb.cus_cust_id_buy is null then v.cus_cust_id_sel else bb.cus_cust_id_buy end)  as cust_total,
count(distinct bb.cus_cust_id_buy) cust_buy,
sum (v.VENTAS_USD) VENTAS_USD,
SUM (BB.GMVEBILLABLE)  bgmv_cpras


FROM BASE_CUST D  --- base de clientes
left JOIN TPV_SEL AS V ON v.CUS_CUST_ID_SEL=D.CUS_CUST_ID and v.sit_site_id=d.sit_site_id_cus --AND COALESCE(D.CUS_TAGS, '') <> 'sink'--and bid.sit_site_id=d.sit_site_id
--LEFT JOIN TPV_BUY AS b on d.cus_cust_id=b.cus_cust_id_buy and d.sit_site_id_cus=b.sit_site_id
LEFT JOIN BGMV_BUY AS BB on d.cus_cust_id=bB.cus_cust_id_buy and d.sit_site_id_cus=bB.sit_site_id --and 
left join Seg_Sel  as s on v.sit_site_id=s.sit_site_id and v.cus_cust_id_sel=s.cus_cust_id_sel
left join lastmcc4 y on d.cus_cust_id = y.cus_cust_id
left join vert2 vt on v.sit_site_id=vt.sit_site_id and v.cus_cust_id_sel=vt.cus_cust_id_sel
LEFT JOIN intergrados ing on ing.CUS_CUST_ID=D.CUS_CUST_ID and ing.sit_site_id=d.sit_site_id_cus
 LEFT JOIN WHOWNER.BT_LYL_POINTS_SNAPSHOT lyl on D.cus_cust_id = lyl.cus_cust_id and D.sit_site_id_CUS = lyl.sit_site_id and lyl.tim_month_id = '202012'
LEFT JOIN account_money2 am on am.CUS_CUST_ID=D.CUS_CUST_ID and am.sit_site_id=d.sit_site_id_cus
 LEFT JOIN seller_shipping Ship2 on ship2.CUS_CUST_ID_sel=D.CUS_CUST_ID and ship2.sit_site_id=d.sit_site_id_cus
 LEFT JOIN credits cred on cred.CUS_CUST_ID=D.CUS_CUST_ID and cred.sit_site_id=d.sit_site_id_cus
 LEFT JOIN seguros seg on seg.CUS_CUST_ID_buy=D.CUS_CUST_ID 
left join BGMV_TIPO_COMPRADOR_2 tdc on d.cus_cust_id=tdc.cus_cust_id_buy and d.sit_site_id_cus=tdc.sit_site_id

group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22


having Baseline='ok'

) with data primary index (cus_cust_id,SIT_SITE_ID_cus) on commit preserve rows;








select


  --BID.CUS_CUST_ID_BUY,
ba.SIT_SITE_ID_CUS,ba.Canal,ba.RUBRO,ba.REG_DATA_TYPE_group,ba.CUS_TAX_PAYER_TYPE,ba.TIPO_COMPRADOR_TGMV,ba.RANGO_VTA_PURO,ba.INTEGRACION,ba.ACCOUNT_MONEY,ba.Ratio_AM_VTAS,ba.LOYALTY,ba.ECOSISTEMA,ba.SEGUROS,ba.CREDITOS,ba.shipping,ba.Frequencia,
       --bid.sit_site_id,

    cat.cat_categ_name_l1,
      -- cat.cat_categ_name_l2,
       --BA.RUBRO,

     
     
    -- case when shp.SHP_SHIPPING_MODE_ID='me2' and shp.shp_picking_type_id='fulfillment' then 'fbm' 
     --when shp.SHP_SHIPPING_MODE_ID='me2' and shp.shp_picking_type_id='self_service' then 'flex'
     --when shp.SHP_SHIPPING_MODE_ID='me2' and shp.shp_picking_type_id='drop_off' then 'ds'
    -- when shp.SHP_SHIPPING_MODE_ID='me2' and shp.shp_picking_type_id in ('xd_drop_off','cross_docking') then 'xd'
    -- when shp.SHP_SHIPPING_MODE_ID='me2' and shp.shp_picking_type_id not in ('fulfillment', 'xd_drop_off','cross_docking') then 'other me2'
     --when shp.SHP_SHIPPING_MODE_ID='me1'  then 'me1'
     --when pick.odr_order_id is not null then 'puis'
    --else 'other' end as ENVIO,
     

          
          
       
     sum((Case when  tgmv_flag = 1  then (BID.BID_SITE_CURRENT_PRICE * bid.BID_QUANTITY_OK) else 0.0 end))  TGMVELC,
    sum((Case when  tgmv_flag = 1 then (bid.BID_BASE_CURRENT_PRICE * bid.BID_QUANTITY_OK) else 0.0 end))  TGMVE,
  COUNT((Case when  tgmv_flag = 1 then BID.BID_QUANTITY_OK else 0.0 end))  TORDERS,
   sum((Case when  tgmv_flag = 1 then BID.BID_QUANTITY_OK else 0.0 end))  TSIE
  -- count(Distinct cus_cust_id_buy) buyer,
    --sum((Case when coalesce(bid.BID_FVF_BONIF, 'Y') = 'N' then (BID.BID_SITE_CURRENT_PRICE * bid.BID_QUANTITY_OK) else 0.0 end))  GMVEBILLABLELC,
   -- sum((Case when coalesce(bid.BID_FVF_BONIF, 'Y') = 'N' then (bid.BID_BASE_CURRENT_PRICE * bid.BID_QUANTITY_OK) else 0.0 end))  GMVEBILLABLE,
--COUNT((Case when coalesce(bid.BID_FVF_BONIF, 'Y') = 'N' then BID.BID_QUANTITY_OK else 0.0 end))  ORDERSBILLABLE,
  -- sum((Case when coalesce(bid.BID_FVF_BONIF, 'Y') = 'N' then BID.BID_QUANTITY_OK else 0.0 end))  SIEBILLABLE
--sum (BID.BID_QUANTITY_OK),
--COUNT(distinct (case when bid.crt_purchase_id is null then bid.ord_order_id else bid.crt_purchase_id end)) as TX_BUY

  --count(distinct bid.ord_order_id) as txdirecto  
  --COUNT(distinct bid.crt_purchase_id) as txcarrito,
  --count(distinct bid.ord_order_id) as txdirecto ---- TRANSACCIONES DIRECTO
    
from WHOWNER.BT_BIDS as bid

left join WHOWNER.LK_ITE_ITEMS_PH ite
on 	(bid.ITE_ITEM_ID = ite.ITE_ITEM_ID and     
bid.PHOTO_ID = ite.PHOTO_ID and     
bid.SiT_SITE_ID = ite.SIT_SITE_ID)   
	
  
left join WHOWNER.AG_LK_CAT_CATEGORIES_PH as cat    
on (ite.sit_site_id = cat.sit_site_id and ite.cat_Categ_id = cat.cat_Categ_id_l7 and cat.photo_id = 'TODATE')   
--left join WHOWNER.LK_CAT_CATEGORIES_CPG as cpg on (cat.sit_site_id = cpg.sit_site_id and cat.cat_categ_id_l3 = cpg.cat_categ_id_l3)

--left join whowner.lk_mapp_mobile c
--on (coalesce(bid.MAPP_APP_ID, '-1') = c.MAPP_APP_ID)

--left join WHOWNER.LK_SEGMENTO_SELLERS seg
--on bid.cus_cust_id_sel=seg.cus_cust_id_sel and bid.sit_site_id=seg.sit_site_id

--left join WHOWNER.LK_CUS_CBT_ITEM_ORIGIN cbt
--on bid.cus_cust_id_sel=cbt.cus_cust_id and bid.sit_site_id=cbt.sit_site_id AND cbt.cus_cbt_flag_t2 = 0 AND cbt.cus_cbt_flag_t4 = 0

--left join WHOWNER.LK_CUS_MKPL_SPECIAL pl
--on bid.cus_cust_id_sel=pl.cus_cust_id_sel --and bid.sit_site_id=pl.sit_site_id
--left join act_10 a10
--on bid.ite_item_id=a10.ite_item_id and bid.sit_site_id=a10.sit_site_id

--left join WHOWNER.LK_ORD_ORDER_TAG t on t.ord_order_id = bid.ord_order_id and t.ord_tag = 'mshops'

--left join WHOWNER.BT_SHP_SHIPMENTS as shp 
--on bid.shp_shipment_id = shp.shp_shipment_id and shp.sit_site_id = bid.sit_site_id

--left join WHOWNER.LK_CAT_CATEGORIES_L7_PH as cat    
--on (ite.sit_site_id = cat.sit_site_id and ite.cat_Categ_id = cat.cat_Categ_id_l7 and cat.photo_id = 'TODATE')   

--left join WHOWNER.BT_ODR_PICKUP pick 
--on bid.ord_order_id = pick.odr_order_id and pick.sit_site_id = bid.sit_site_id

JOIN BASE BA on ba.cus_cust_id=bid.cus_cust_id_buy

where bid.sit_site_id IN 'MLB'--- ('MCO', 'MLA', 'MLB', 'MLC', 'MLM', 'MLU', 'MPE')
and bid.photo_id = 'TODATE' 
and (bid.tim_day_winning_date between DATE '2020-01-01' and '2020-12-31') --OR bid.tim_day_winning_date between DATE '2019-01-01' and '2019-10-31')
and bid.ite_gmv_flag = 1
and bid.mkt_marketplace_id = 'TM'
--AND coalesce(BID.AUTO_OFFER_FLAG, 0) <> 1 
and tgmv_flag = 1
--and BA.CANAL='Point'
--and RUBRO IN ('ACC','CE','APPAREL_BEAUTY')
--and ba.REG_DATA_TYPE_group='Company'
--aND seg.segmenTo <>'LONG TAIL'
--AND coalesce(bid.BID_FVF_BONIF, 'Y') = 'N' ---- > Saca esto
--and cat.cat_categ_name_l1 IN ( 'Arte, Papelaria e Armarinho','Eletrônicos, Áudio e Vídeo','Indústria e Comércio','Informática','Ferramentas')
--and ite.ite_dom_domain_id like '%CELLPHONES'

group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17;
