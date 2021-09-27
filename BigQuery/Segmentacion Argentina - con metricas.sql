/* SEGMENTACION ARGENTINA BIGQUERY */

----------------------------- 01. Traigo ventas: Uno datos Mercado Pago y Marketplace  ------------------------------------------

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
AND mp.sit_site_id IN ('MLA')
AND MP.PAY_MOVE_DATE BETWEEN DATE '2020-01-01' AND DATE '2020-12-31'
AND mp.pay_status_code IN ( 'approved')--, 'authorized')
AND tpv_segment <> 'ON'--AND coalesce(i.ITE_TIPO_PROD,0) <> 'U' -----> Saco todo lo que es Marketplace
GROUP BY 1,2,3,4

UNION ALL--- para poder hacer union las dos tablas son iguales

/* Comment Tomas: Traigo todos los datos de Marketplace */

SELECT
  ord.ORD_SELLER.ID cus_cust_id_sel,  
  ord.sit_site_id sit_site_id,
  'Selling Marketplace' tpv_segment,
  'Selling Marketplace' tpv_segment_detail,
  SUM((CASE WHEN coalesce(ord.ORD_FVF_BONIF, True) = False  THEN (ord.ORD_ITEM.BASE_CURRENT_PRICE * ord.ORD_ITEM.QTY) ELSE 0.0 END))  VENTAS_USD,
  SUM (1) Q
FROM WHOWNER.BT_ORD_ORDERS as ord
where ord.sit_site_id IN ('MLA')

AND ord.ORD_CLOSED_DT BETWEEN DATE '2020-01-01' AND DATE '2020-12-31'
AND ord.ORD_GMV_FLG = True
AND ord.ORD_CATEGORY.MARKETPLACE_ID = 'TM'
AND coalesce(ord.ORD_AUTO_OFFER_FLG, False) <> True
AND coalesce(ord.ORD_FVF_BONIF, True) = False
GROUP BY 1,2,3,4
) ,

----------------------------- 02. Agrupo Volumen de ventas por seller.  ------------------------------------------

TPV_SEL as ( --- sumo los volumenes por seller --- volumen total por seller
SELECT
  cus_cust_id_sel,
  sit_site_id,
  SUM(VENTAS_USD) VENTAS_USD,
  SUM(Q) Q,
  COUNT(distinct tpv_segment_id) Q_SEG
FROM TPV_SEL_1
GROUP BY 1,2
) ,

----------------------------- 03. Query para tener el segmento de MP, tomo el mayor puede que haya seller en mas de un segmento -----------------------------------------

SEG_SEL as ( 
SELECT
  sp.cus_cust_id_sel,
  sp.sit_site_id,
  sp.tpv_segment_id,
  sp.tpv_segment_detail,
  sm.SEGMENTO,
  sum(VENTAS_USD) VENTAS_USD
FROM TPV_SEL_1 sp
LEFT JOIN WHOWNER.LK_MKP_SEGMENTO_SELLERS  sm
on cast(sp.cus_cust_id_sel as string)=cast(sm.cus_cust_id_sel as string)
GROUP BY 1,2,3,4,5
qualify row_number () over (partition by sp.cus_cust_id_sel, sp.sit_site_id order by VENTAS_USD DESC) = 1
),

----------------------------- 04. Agrupo volumen de compras por buyer y categoria ------------------------------------------

BGMV_BUY AS ( --- compras en el marketplace por empresa
SELECT
    ord.ORD_BUYER.ID,  
    ord.sit_site_id,
    SUM((CASE WHEN ORD_TGMV_FLG = True and cat.cat_categ_name_l1 in ('Computación','Herramientas y Construcción','Industrias y Oficinas', 'Electrónica','Electrónica, Audio y Video','Arte, Librería y Mercería','Arte y Artesanías','Arte y Antigüedades') 
          THEN (ord.ORD_ITEM.BASE_CURRENT_PRICE * ord.ORD_ITEM.QTY) 
          ELSE 0.0 
        END )) TGMV_COMP,
    SUM((CASE WHEN ORD_TGMV_FLG = True and cat.cat_categ_name_l1 in ('Acessórios para Veículos','Accesorios para Vehículos') 
          THEN (ord.ORD_ITEM.BASE_CURRENT_PRICE * ord.ORD_ITEM.QTY)
          ELSE 0.0 
        END )) TGMV_AUTO,
    SUM((case when ORD_TGMV_FLG = True and cat.cat_categ_name_l1 in ('Belleza y Cuidado Personal') 
    THEN (ord.ORD_ITEM.BASE_CURRENT_PRICE * ord.ORD_ITEM.QTY) else 0.0 end )) TGMV_BEAUTY,
    SUM((CASE WHEN ORD_TGMV_FLG = True and cat.cat_categ_name_l1 in ('Computación','Electrónica','Electrónica, Audio y Video','Electrodomésticos','Electrodomésticos y Aire Acond','Electrodomésticos y Aires Ac.','Celulares y Teléfonos','Celulares y Telefonía','Cámaras Digitales y Foto','Cámaras Digitales y Foto.','Cámaras y Accesorios') 
          THEN (ord.ORD_ITEM.BASE_CURRENT_PRICE * ord.ORD_ITEM.QTY)
          ELSE 0.0 
        END))  TGMV_CE,
    SUM((CASE WHEN ORD_TGMV_FLG = True
          THEN (ord.ORD_ITEM.BASE_CURRENT_PRICE * ord.ORD_ITEM.QTY)
          ELSE 0.0 
        END))  GMVEBILLABLE, -- TGMV
    COUNT((CASE WHEN ORD_TGMV_FLG = True then ord.ORD_ITEM.QTY else 0.0 end))  TORDERS_BUY,
    SUM((CASE WHEN ORD_TGMV_FLG = True then ord.ORD_ITEM.QTY else 0.0 end))  TSIE_BUY, -- TSI
    COUNT(distinct (case when ord.crt_purchase_id is null then ord.ord_order_id else ord.crt_purchase_id end)) as TX_BUY

FROM WHOWNER.BT_ORD_ORDERS AS ord
LEFT JOIN WHOWNER.LK_ITE_ITEMS ite 
on (ord.ITE_ITEM_ID = ite.ITE_ITEM_ID AND      
ord.SiT_SITE_ID = ite.SIT_SITE_ID)  

LEFT JOIN WHOWNER.AG_LK_CAT_CATEGORIES_PH AS cat    
on (ite.sit_site_id = cat.sit_site_id AND
cast(ite.cat_Categ_id as string) = cat.cat_Categ_id_l7 AND
cat.photo_id = 'TODATE')  

WHERE ord.sit_site_id IN ('MLA')
AND ord.ORD_CLOSED_DT BETWEEN DATE '2020-01-01' AND DATE '2020-12-31'
AND ord.ORD_GMV_FLG = True
AND ord.ORD_CATEGORY.MARKETPLACE_ID = 'TM'
AND ORD_TGMV_FLG = True
GROUP BY 1,2
),


----------------------------- 05. Traigo los Q en los que hizo compras. -----------------------------------------

BGMV_TIPO_COMPRADOR AS ( --- compras en el marketplace por usuario 
SELECT
    ord.ORD_BUYER.ID cus_cust_id_buy,  
    ord.sit_site_id,
    cal.TIM_QUARTER_ID_QLY quarter,    
    COUNT(distinct (case when ord.crt_purchase_id is null then ord.ord_order_id else ord.crt_purchase_id end)) as TX_BUY
FROM WHOWNER.BT_ORD_ORDERS as ord
LEFT JOIN WHOWNER.LK_TIM_DAYS cal
on ord.ORD_CLOSED_DT = cal.TIM_DAY
WHERE ord.sit_site_id IN ('MLA')
AND ord.ORD_CLOSED_DT between DATE '2020-01-01' AND DATE '2020-12-31'
AND ord.ORD_GMV_FLG = True
AND ord.ORD_CATEGORY.MARKETPLACE_ID = 'TM'
AND ORD_TGMV_FLG = True
group by 1,2,3
),

----------------------------- 06. Categorizo tipo de comprador -----------------------------------------

BGMV_TIPO_COMPRADOR_2 AS (
SELECT
    tcb.cus_cust_id_buy,  
    tcb.sit_site_id,
    CASE WHEN cdt.cus_first_buy_no_bonif_autoof <= '2020-01-01' then 'OK'
      WHEN cdt.cus_first_buy_no_bonif_autoof <= '2020-03-31' then '3Q'
      WHEN cdt.cus_first_buy_no_bonif_autoof <= '2020-06-30' then '2Q'
      WHEN cdt.cus_first_buy_no_bonif_autoof <= '2020-09-30' then '1Q'
    ELSE 'Menos 1Q'  end as Q_cuenta,
    CASE WHEN cdt.cus_first_buy_no_bonif_autoof IS null THEN 'Nunca Compro' ELSE 'Compro' END AS NB,
    COUNT(distinct quarter) cant_q_compras  
FROM BGMV_TIPO_COMPRADOR tcb
LEFT JOIN WHOWNER.LK_CUS_CUSTOMER_DATES CDT ON CDT.CUS_CUST_ID=tcb.CUS_CUST_ID_BUY AND CDT.sit_site_id=tcb.SIT_SITE_ID
group by 1,2,3,4

),

----------------------------- 07. Traigo el regimen fiscal del cust: Para argentina no son consistentes los datos pero mantengo para mantener las columnas ------------------------------------------

CREATE multiset volatile TABLE br_mx as ( -- tipo de regimen
SELECT
  cus_cust_id,
  sit_site_id,
  cus_tax_payer_type,
  cus_tax_regime   ----Charly: NO TIENE SENTIDO XQ SON TODOS NULL en argentina
FROM LK_TAX_CUST_WRAPPER
WHERE sit_site_id IN ('MLA')
qualify row_number () over (partition by cus_cust_id, sit_site_id ORDER BY  aud_upd_dt DESC) = 1
) with data primary index (sit_site_id,cus_cust_id) on commit preserve rows;

----------------------------- 08. Traigo rubro ------------------------------------------

lastmcc3 as ( --- rubro
SELECT * FROM (
  SELECT
    MCC.cus_cust_id,
    sit_site_id,
    MCC.mcc_last_adding_datetime,
    CASE WHEN (MCC.mcc_last_adding_datetime = MAX(MCC.mcc_last_adding_datetime) OVER (PARTITION BY cus_cust_id)) THEN 'Y' ELSE 'N' END AS MAX_NUM_IND,
     mcc_id
  FROM
    WHOWNER.LK_CUS_CUSTOMER_MCC MCC
    WHERE mcc_source='USER_ASSIGN'
  ) DAT2
  WHERE MAX_NUM_IND = 'Y'
  qualify row_number () over (partition by  cus_cust_id, sit_site_id order by mcc_last_adding_datetime DESC) = 1
) ,

----------------------------- 9. Agrupo el rubro ------------------------------------------

lastmcc4 as (
SELECT
  y.cus_cust_id,
  sit_site_id,
  y.MCC_ID MCC_ID,
  x.mcc_description MCC1,
  CASE WHEN MCC1 in ('Automotive Service Shops (Non-Dealer)','Car Washes','Parking Lots and Garages') then 'ACC'
  WHEN MCC1 in ('Auto Parts and Accessories Stores','Car and Truck Dealers (New and Used) Sales Service Repairs Parts and Leasing','Motor Vehicle Supplies and New Parts','Motorcycle Shops and Dealers','ACC') then 'ACC'
  WHEN MCC1 in ('Family Clothing Stores','Precious Stones and Metals Watches and Jewelry','Sewing Needlework Fabric and Piece Goods Stores','Shoe Stores','APPAREL') then 'APPAREL'
  WHEN MCC1 in ('Beauty and Barber Shops','Cosmetic Stores','Health and Beauty Spas') then 'APPAREL_BEAUTY'
  WHEN MCC1 in ('Book Stores','Books Periodicals and Newspapers','News Dealers and Newsstands','Music Stores - Musical Instruments Pianos and Sheet Music') then 'ENTERTAINMENT'
  WHEN MCC1 in ('Computer Maintenance and Repair Services - Not Elsewhere Classified','Computer Software Stores','Durable Goods - Not Elsewhere Classified','Electrical Parts and Equipment','Electronics Stores','Hardware Equipment and Supplies','Household Appliance Stores','CE') then 'CE'
  WHEN MCC1 in ('Advertising Services','Architectural Engineering and Surveying Services','Business Services -Not Elsewhere Classified','Detective Agencies Protective Agencies and Security Services including Armored Cars and Guard Dogs','Direct Marketing/Direct Marketers - Not Elsewhere Classified','Dry Cleaners','Funeral Services and Crematories','Insurance Sales Underwriting and Premiums','Legal Services and Attorneys','Management Consulting and Public Relations Services','Miscellaneous Personal Services - Not Elsewhere Classified','Miscellaneous Publishing and Printing Services','Motion Picture Theaters','Photographic Studios','Quick Copy Reproduction and Blueprinting Services','Real Estate (e,g, rent)','Theatrical Producers (except Motion Pictures) and Ticket Agencies','Travel Agencies and Tour Operators','Typesetting Plate Making and Related Services','Electrical Contractors','General Contractors - Residential and Commercial','Masonry Stonework Tile-Setting Plastering and Insulation Contractors','Metal Service Centers and Offices','Roofing Siding and Sheet Metal Work Contractors','Courier Services - Air and Ground and Freight Forwarders','Motor Freight Carriers and Trucking - Local and Long Distance Moving and Storage Companies and Local Delivery','Fuel Dealers - Fuel Oil Wood Coal and Liquefied Petroleum','Cleaning Maintenance and Janitorial Services') then 'CONSULTING & SERVICES'
  WHEN MCC1 in ('Grocery Stores and Supermarkets','Miscellaneous Food Stores - Convenience Stores and Specialty Markets','Package Stores - Beer Wine and Liquor','CPG') then 'CPG'
  WHEN MCC1 in ('Construction Materials - Not Elsewhere Classified','Furniture Home Furnishings and Equipment Stores and Manufacturers except Appliances','Lumber and Building Materials Stores','Miscellaneous Home Furnishings Specialty Stores','HOME & INDUSTRIES','Lodging Hotels Motels and Resorts','Stationery Office Supplies Printing and Writing Paper') then 'HOME & INDUSTRIES'
  WHEN MCC1 in ('Dentists and Orthodontists','Doctors and Physicians - Not Elsewhere Classified','Hospitals','Medical Services and Health Practitioners - Not Elsewhere Classified','Dental/Laboratory/Medical/ Ophthalmic Hospital Equipment and Supplies','Drug Stores and Pharmacies','Opticians Optical Goods and Eyeglasses') then 'APPAREL_MEDICAL'
  WHEN MCC1 in ('Civic Social and Fraternal Associations','Membership Clubs (Sports Recreation Athletic) Country Clubs and Private Golf Courses','Religious Organizations','Child Care Services','Political Organizations','Recreation Services - Not Elsewhere Classified','Schools and Educational Services - Not Elsewhere Classified','Tourist Attractions and Exhibits','Charitable and Social Service Organizations') then 'SOCIAL'
  WHEN MCC1 in ('Bicycle Shops - Sales and Service','Sporting Goods Stores') then 'APPAREL_SPORTS'
  WHEN MCC1 in ('Hobby Toy and Game Stores') then 'TOYS & BABIES'
  WHEN MCC1 in ('Bus Lines','Taxicabs and Limousines','Transportation Services - Not Elsewhere Classified') then 'TRAVEL'
  ELSE 'OTROS' END AS MCC,
  x.mcc_category mcc_group
  --COUNT(*)
FROM lastmcc3 y
LEFT JOIN lk_mp_mcc_code x
ON cast(y.MCC_ID as varchar(4))=cast(x.mcc_code as varchar(4))
GROUP BY 1,2,3,4,5,6
--having count(*)>1
) 

----------------------------- 10. Traigo vertical maxima ------------------------------------------

vert as ( -- vertical maxima
SELECT
    ord.sit_site_id,
    ord.ORD_SELLER.ID cus_cust_id_sel,
    cat.vertical,
    sum((Case when coalesce(ord.ORD_FVF_BONIF, True) = False then (ord.ORD_ITEM.BASE_CURRENT_PRICE * ord.ORD_ITEM.QTY) else 0.0 end))  GMVEBILLABLE
FROM WHOWNER.BT_ORD_ORDERS as ord
LEFT JOIN WHOWNER.LK_ITE_ITEMS ite
on (ord.ITE_ITEM_ID = ite.ITE_ITEM_ID AND     
ord.SiT_SITE_ID = ite.SIT_SITE_ID)  

LEFT JOIN WHOWNER.AG_LK_CAT_CATEGORIES_PH as cat    
on (ite.sit_site_id = cat.sit_site_id AND
cast(ite.cat_Categ_id as string) = cat.cat_Categ_id_l7 AND
cat.photo_id = 'TODATE')  

/*
LEFT JOIN WHOWNER.LK_MKP_SEGMENTO_SELLERS seg
on ord.ORD_SELLER.ID =seg.cus_cust_id_sel AND
seg.sit_site_id=ord.sit_site_id

LEFT JOIN WHOWNER.LK_ITE_ATTRIBUTE_VALUES att
on ord.ORD_ITEM.ID = att.ITE_ITEM_ID AND       
ord.SiT_SITE_ID = att.SIT_SITE_ID
*/

WHERE ord.sit_site_id IN ('MLA')

AND ord.ORD_CLOSED_DT BETWEEN DATE '2020-01-01' AND DATE '2020-12-31'
AND ord.ORD_GMV_FLG = True
AND ord.ORD_CATEGORY.MARKETPLACE_ID = 'TM'
AND coalesce(ord.ORD_AUTO_OFFER_FLG, False) <> True
AND coalesce(ord.ORD_FVF_BONIF, True) = False
group by 1,2,3
),

-- 11. Agrupo Vertical

vert2 AS ( -- vertical maxima
SELECT
  cus_cust_id_sel,
  sit_site_id,
  vertical,
  sum(GMVEBILLABLE) GMVEBILLABLE
FROM vert
group by 1,2,3
qualify row_number () over (partition by cus_cust_id_sel, sit_site_id order by  GMVEBILLABLE DESC) = 1
),

--- 12. Cuento la cantidad de verticales que vende

vert3 as ( --- cantidad de verticales de vta
SELECT
    cus_cust_id_sel,
    sit_site_id,
COUNT(distinct VERTICAL) AS CANT_VERT_VTA
FROM vert
GROUP BY 1,2
) ,


/*----------------------------- 13. Traigo cantidad de integrados por seller ------------------------------------------

CREATE multiset volatile TABLE intergrados as ( --- cantidad de integraciones a partir del get
SELECT
cus_cust_id,
sit_site_id,
COUNT(*) CANT_INTEGRADOS
FROM  WHOWNER.lk_op_active_users us
WHERE us.date_end >=  '2020-12-31' --- trae los que no finalizaron en 2020
AND us.sit_site_id IN ('MLA')
group by 1,2
) */

----------------------------- 14. Trae la plata en cuenta de mercadopago ------------------------------------------

account_money as (
SELECT
  cus_cust_id,
  sit_site_id,
  AVG (AVAILABLE_BALANCE) balance
FROM  WHOWNER.BT_MP_SALDOS_SNAPSHOT_DAILY
WHERE TIM_DAY BETWEEN DATE '2021-03-01' AND DATE '2021-04-30'
AND sit_site_id IN ('MLA')
GROUP BY 1,2
),

----------------------------- 15. Categoriza la account money ------------------------------------------

account_money2 as (
SELECT
  CASE WHEN cus_cust_id_sel IS NULL THEN CUS_CUST_ID ELSE cus_cust_id_sel END AS cus_cust_id ,
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

FROM TPV_SEL AS V
FULL OUTER JOIN account_money am
on am.CUS_CUST_ID=V.CUS_CUST_ID_SEL
AND am.sit_site_id=V.sit_site_id
) ,

----------------------------- 16. Trae datos de creditos ------------------------------------------

credits as (
SELECT
  CUS_CUST_ID_BORROWER CUS_CUST_ID,
  SIT_SITE_ID,
  COUNT(*) total
FROM WHOWNER.BT_MP_CREDITS
WHERE CRD_CREDIT_DATE_FINISHED_ID >= DATE '2020-01-01'
AND sit_site_id='MLA'
GROUP BY 1,2
) ,

----------------------------- 17. Trae datos de seguros ------------------------------------------

seguros as (
SELECT
  CUS_CUST_ID_buy,
  COUNT(*) total
FROM WHOWNER.BT_INSURANCE_PRODUCT_PURCHASES
WHERE INSUR_PURCHASE_STATUS = 'confirmed'
and SIT_SITE_ID='MLA'
GROUP BY 1
) ,

----------------------------- 18. Trae datos de shipping ------------------------------------------

seller_shipping AS (
SELECT
  ord.sit_site_id,
  ord.ORD_SELLER.ID cus_cust_id_sel,
  COUNT(*) total
FROM WHOWNER.BT_ORD_ORDERS AS ord
LEFT JOIN WHOWNER.BT_SHP_SHIPMENTS AS shp
on ord.ORD_SHIPPING.ID = shp.shp_shipment_id AND shp.sit_site_id = ord.sit_site_id
WHERE ord.sit_site_id = 'MLA'--- ('MCO', 'MLA', 'MLB', 'MLC', 'MLM', 'MLU', 'MPE')

AND ord.ORD_CLOSED_DTTM BETWEEN DATE '2020-10-01' AND DATE '2020-12-31' --OR bid.tim_day_winning_date between DATE '2019-01-01' and '2019-10-31')
AND ord.ORD_GMV_FLG = True
AND ord.ORD_CATEGORY.MARKETPLACE_ID = 'TM'
AND shp.shp_picking_type_id IN ('xd_drop_off','cross_docking','fulfillment')
AND ord.ORD_TGMV_FLG = True
group by 1,2
) ,