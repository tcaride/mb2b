/* SEGMENTACION ARGENTINA TERADATA */

----------------------------- 01. Traigo ventas: Uno datos Mercado Pago y Marketplace  ------------------------------------------

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
GROUP BY 1,2,3,4

UNION --- para poder hacer union las dos tablas son iguales

/* Comment Tomas: Traigo todos los datos de Marketplace */

SELECT
  bid.cus_cust_id_sel cus_cust_id_sel,  
  bid.sit_site_id sit_site_id,
  'Selling Marketplace' tpv_segment_id,
  'Selling Marketplace' tpv_segment_detail,
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
GROUP BY 1,2,3,4
) WITH data primary index (CUS_CUST_ID_SEL,SIT_SITE_ID) on commit preserve rows;

----------------------------- 02. Agrupo Volumen de ventas por seller.  ------------------------------------------

CREATE multiset volatile TABLE TPV_SEL as ( --- sumo los volumenes por seller --- volumen total por seller
SELECT
  kyc.KYC_IDENTIFICATION_NUMBER,
  sit_site_id,
  SUM(VENTAS_USD) VENTAS_USD,
  SUM(Q) Q,
  COUNT(distinct tpv_segment_id) Q_SEG
FROM TPV_SEL_1
LEFT JOIN LK_KYC_VAULT_USER kyc
ON kyc.cus_cust_id=bid.cus_cust_id_sel 

GROUP BY 1,2, 3
) WITH data primary index (KYC_IDENTIFICATION_NUMBER,SIT_SITE_ID) on commit preserve rows;

----------------------------- 03. Query para tener el segmento de MP, tomo el mayor puede que haya seller en mas de un segmento -----------------------------------------

CREATE multiset volatile TABLE SEG_SEL as ( 
SELECT
  SP.KYC_IDENTIFICATION_NUMBER,
  SP.sit_site_id,

  SP.tpv_segment_id,
  SP.tpv_segment_detail,
  VENTAS_USD
FROM TPV_SEL_1 sp
qualify row_number () over (partition by sp.cus_cust_id_sel, sp.sit_site_id order by VENTAS_USD DESC) = 1
GROUP BY 1,2,3,4,5,6
) with data primary index (CUS_CUST_ID_SEL,SIT_SITE_ID) on commit preserve rows;

----------------------------- 04. Agrupo volumen de compras por buyer y categoria ------------------------------------------

CREATE MULTISET VOLATILE TABLE BGMV_BUY AS ( --- compras en el marketplace por empresa
SELECT
    bid.cus_cust_id_buy,  
    bid.sit_site_id,
    KYC.KYC_IDENTIFICATION_NUMBER,
    SUM((CASE WHEN tgmv_flag = 1 and cat.cat_categ_name_l1 in ('Computación','Herramientas y Construcción','Industrias y Oficinas', 'Electrónica','Electrónica, Audio y Video','Arte, Librería y Mercería','Arte y Artesanías','Arte y Antigüedades') 
          THEN (bid.BID_BASE_CURRENT_PRICE * bid.BID_QUANTITY_OK) 
          ELSE 0.0 
        END )) TGMV_COMP,
    SUM((CASE WHEN tgmv_flag = 1 and cat.cat_categ_name_l1 in ('Acessórios para Veículos','Accesorios para Vehículos') 
          THEN (bid.BID_BASE_CURRENT_PRICE * bid.BID_QUANTITY_OK) 
          ELSE 0.0 
        END )) TGMV_AUTO,
    SUM((case when tgmv_flag = 1 and cat.cat_categ_name_l1 in ('Belleza y Cuidado Personal') 
    THEN (bid.BID_BASE_CURRENT_PRICE * bid.BID_QUANTITY_OK) else 0.0 end )) TGMV_BEAUTY,
    SUM((CASE WHEN tgmv_flag = 1 and cat.cat_categ_name_l1 in ('Computación','Electrónica','Electrónica, Audio y Video','Electrodomésticos','Electrodomésticos y Aire Acond','Electrodomésticos y Aires Ac.','Celulares y Teléfonos','Celulares y Telefonía','Cámaras Digitales y Foto','Cámaras Digitales y Foto.','Cámaras y Accesorios') 
          THEN (bid.BID_BASE_CURRENT_PRICE * bid.BID_QUANTITY_OK)
          ELSE 0.0 
        END))  TGMV_CE,
    SUM((CASE WHEN tgmv_flag = 1 
          THEN (bid.BID_BASE_CURRENT_PRICE * bid.BID_QUANTITY_OK) 
          ELSE 0.0 
        END))  GMVEBILLABLE, -- TGMV
    COUNT((CASE WHEN tgmv_flag = 1 then BID.BID_QUANTITY_OK else 0.0 end))  TORDERS_BUY,
    SUM((CASE WHEN tgmv_flag = 1 then BID.BID_QUANTITY_OK else 0.0 end))  TSIE_BUY, -- TSI
    COUNT(distinct (case when bid.crt_purchase_id is null then bid.ord_order_id else bid.crt_purchase_id end)) as TX_BUY

FROM WHOWNER.BT_BIDS AS bid
LEFT JOIN LK_KYC_VAULT_USER KYC
kyc.cus_cust_id=bid.cus_cust_id_buy

LEFT JOIN WHOWNER.LK_ITE_ITEMS_PH ite 
on (bid.ITE_ITEM_ID = ite.ITE_ITEM_ID AND    
bid.PHOTO_ID = ite.PHOTO_ID AND    
bid.SiT_SITE_ID = ite.SIT_SITE_ID)  

LEFT JOIN WHOWNER.AG_LK_CAT_CATEGORIES_PH AS cat    
on (ite.sit_site_id = cat.sit_site_id AND
ite.cat_Categ_id = cat.cat_Categ_id_l7 AND
cat.photo_id = 'TODATE')  

WHERE bid.sit_site_id IN ('MLA')
AND bid.photo_id = 'TODATE'
AND bid.tim_day_winning_date BETWEEN DATE '2020-01-01' AND DATE '2020-12-31'
AND bid.ite_gmv_flag = 1
AND bid.mkt_marketplace_id = 'TM'
AND tgmv_flag = 1
AND KYC.KYC_ENTITY_TYPE = 'company'
GROUP BY 1,2
)WITH DATA PRIMARY INDEX (sit_site_id,cus_cust_id_buy) ON COMMIT PRESERVE ROWS;

----------------------------- 05. Traigo los Q en los que hizo compras. -----------------------------------------

CREATE MULTISET VOLATILE TABLE BGMV_TIPO_COMPRADOR AS ( --- compras en el marketplace por usuario 
SELECT
    bid.cus_cust_id_buy,  
    bid.sit_site_id,
    KYC.KYC_IDENTIFICATION_NUMBER,
     ((CAST(EXTRACT(MONTH FROM bid.TIM_DAY_WINNING_DATE) AS BYTEINT)-1)/3)+1
  || 'Q' || substring(bid.TIM_DAY_WINNING_DATE,3,2) quarter,    
    COUNT(distinct (case when bid.crt_purchase_id is null then bid.ord_order_id else bid.crt_purchase_id end)) as TX_BUY
FROM WHOWNER.BT_BIDS as bid
LEFT JOIN LK_KYC_VAULT_USER KYC
kyc.cus_cust_id=bid.cus_cust_id_buy
WHERE bid.sit_site_id IN ('MLA')
AND bid.photo_id = 'TODATE' 
AND bid.tim_day_winning_date between DATE '2020-01-01' AND DATE '2020-12-31'
AND bid.ite_gmv_flag = 1
AND bid.mkt_marketplace_id = 'TM'
AND tgmv_flag = 1
AND KYC.KYC_ENTITY_TYPE = 'company'
group by 1,2,3,4
)WITH DATA PRIMARY INDEX (sit_site_id,cus_cust_id_buy) ON COMMIT PRESERVE ROWS;


----------------------------- 06. Categorizo tipo de comprador -----------------------------------------

CREATE MULTISET VOLATILE TABLE BGMV_TIPO_COMPRADOR_2 AS (

)WITH DATA PRIMARY INDEX (sit_site_id,cus_cust_id_buy) ON COMMIT PRESERVE ROWS;

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

CREATE multiset volatile TABLE lastmcc3 as ( --- rubro
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
) with data primary index (cus_cust_id) on commit preserve rows;

----------------------------- 9. Agrupo el rubro ------------------------------------------

CREATE multiset volatile TABLE lastmcc4 as (
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
) with data primary index (cus_cust_id) on commit preserve rows;

-------------------------- 10. Traigo vertical maxima ------------------------------------------

CREATE multiset volatile TABLE vert as ( -- vertical maxima
SELECT
    bid.sit_site_id,
    bid.cus_cust_id_sel,
    cat.vertical,
    sum((Case when coalesce(bid.BID_FVF_BONIF, 'Y') = 'N' then (BID.BID_base_CURRENT_PRICE * bid.BID_QUANTITY_OK) else 0.0 end))  GMVEBILLABLE
FROM WHOWNER.BT_BIDS as bid

LEFT JOIN LK_KYC_VAULT_USER KYC
kyc.cus_cust_id=bid.cus_cust_id_buy

LEFT JOIN WHOWNER.LK_ITE_ITEMS_PH ite
on (bid.ITE_ITEM_ID = ite.ITE_ITEM_ID AND    
bid.PHOTO_ID = ite.PHOTO_ID AND    
bid.SiT_SITE_ID = ite.SIT_SITE_ID)  

LEFT JOIN WHOWNER.AG_LK_CAT_CATEGORIES_PH as cat    
on (ite.sit_site_id = cat.sit_site_id AND
ite.cat_Categ_id = cat.cat_Categ_id_l7 AND
cat.photo_id = 'TODATE')  

LEFT JOIN WHOWNER.LK_SEGMENTO_SELLERS seg
on bid.cus_cust_id_sel=seg.cus_cust_id_sel AND
seg.sit_site_id=bid.sit_site_id

LEFT JOIN WHOWNER.LK_ITE_ATTRIBUTE_VALUES att
on bid.ITE_ITEM_ID = att.ITE_ITEM_ID AND    
bid.PHOTO_ID = ite.PHOTO_ID AND    
bid.SiT_SITE_ID = att.SIT_SITE_ID

WHERE bid.sit_site_id IN ('MLA')
AND bid.photo_id = 'TODATE'
AND bid.tim_day_winning_date BETWEEN DATE '2020-01-01' AND DATE '2020-12-31'
AND bid.ite_gmv_flag = 1
AND bid.mkt_marketplace_id = 'TM'
AND coalesce(BID.AUTO_OFFER_FLAG, 0) <> 1
AND coalesce(bid.BID_FVF_BONIF, 'Y') = 'N'
AND KYC.KYC_ENTITY_TYPE = 'company'
group by 1,2,3
) with data primary index (sit_site_id,cus_cust_id_sel) on commit preserve rows;


--- 11. Agrupo Vertical

CREATE multiset volatile TABLE vert2 AS ( -- vertical maxima
SELECT
  cus_cust_id_sel,
  sit_site_id,
  vertical,
  GMVEBILLABLE
FROM vert
--group by 1,2,3
qualify row_number () over (partition by cus_cust_id_sel, sit_site_id order by  GMVEBILLABLE DESC) = 1
) with data primary index (sit_site_id,cus_cust_id_sel) on commit preserve rows;

--- 12. Cuento la cantidad de verticales que vende

CREATE multiset volatile TABLE vert3 as ( --- cantidad de verticales de vta
SELECT
    cus_cust_id_sel,
    sit_site_id,
COUNT(distinct VERTICAL) AS CANT_VERT_VTA
FROM vert
GROUP BY 1,2
) with data primary index (sit_site_id,cus_cust_id_sel) on commit preserve rows;
