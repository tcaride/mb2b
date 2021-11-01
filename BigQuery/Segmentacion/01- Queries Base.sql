/* ------------------------- Queries de base para correr metricas y segmentacion ---------------------- */


------ SETTINGS ------

DECLARE from_date DATE DEFAULT '2020-11-01';  
DECLARE to_date DATE DEFAULT '2021-10-30';
DECLARE from_date_account_money DATE DEFAULT '2021-04-20';  -- UNAV_AMOUNT esta disponible desde esta fecha en la tabla

/* ------------------------- Corro a nivel CUST ID ---------------------- */

----------------------------- 01 - Traigo rubro ------------------------------------------

  DROP TABLE IF EXISTS meli-bi-data.SBOX_B2B_MKTPLACE.LK_LASTMCC3;
  CREATE TABLE  meli-bi-data.SBOX_B2B_MKTPLACE.LK_LASTMCC3 as ( --- rubro
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
  ) ;

----------------------------- 02- Agrupo el rubro ------------------------------------------

  DROP TABLE IF EXISTS meli-bi-data.SBOX_B2B_MKTPLACE.LK_LASTMCC4;
  CREATE TABLE meli-bi-data.SBOX_B2B_MKTPLACE.LK_LASTMCC4 as (
  SELECT
    y.cus_cust_id,
    sit_site_id,
    y.MCC_ID MCC_ID,
    x.mcc_description MCC1,
    CASE WHEN     x.mcc_description in ('Automotive Service Shops (Non-Dealer)','Car Washes','Parking Lots and Garages') then 'ACC'
    WHEN     x.mcc_description in ('Auto Parts and Accessories Stores','Car and Truck Dealers (New and Used) Sales Service Repairs Parts and Leasing','Motor Vehicle Supplies and New Parts','Motorcycle Shops and Dealers','ACC') then 'ACC'
    WHEN     x.mcc_description in ('Family Clothing Stores','Precious Stones and Metals Watches and Jewelry','Sewing Needlework Fabric and Piece Goods Stores','Shoe Stores','APPAREL') then 'APPAREL'
    WHEN     x.mcc_description in ('Beauty and Barber Shops','Cosmetic Stores','Health and Beauty Spas') then 'APPAREL_BEAUTY'
    WHEN     x.mcc_description in ('Book Stores','Books Periodicals and Newspapers','News Dealers and Newsstands','Music Stores - Musical Instruments Pianos and Sheet Music') then 'ENTERTAINMENT'
    WHEN     x.mcc_description in ('Computer Maintenance and Repair Services - Not Elsewhere Classified','Computer Software Stores','Durable Goods - Not Elsewhere Classified','Electrical Parts and Equipment','Electronics Stores','Hardware Equipment and Supplies','Household Appliance Stores','CE') then 'CE'
    WHEN     x.mcc_description in ('Advertising Services','Architectural Engineering and Surveying Services','Business Services -Not Elsewhere Classified','Detective Agencies Protective Agencies and Security Services including Armored Cars and Guard Dogs','Direct Marketing/Direct Marketers - Not Elsewhere Classified','Dry Cleaners','Funeral Services and Crematories','Insurance Sales Underwriting and Premiums','Legal Services and Attorneys','Management Consulting and Public Relations Services','Miscellaneous Personal Services - Not Elsewhere Classified','Miscellaneous Publishing and Printing Services','Motion Picture Theaters','Photographic Studios','Quick Copy Reproduction and Blueprinting Services','Real Estate (e,g, rent)','Theatrical Producers (except Motion Pictures) and Ticket Agencies','Travel Agencies and Tour Operators','Typesetting Plate Making and Related Services','Electrical Contractors','General Contractors - Residential and Commercial','Masonry Stonework Tile-Setting Plastering and Insulation Contractors','Metal Service Centers and Offices','Roofing Siding and Sheet Metal Work Contractors','Courier Services - Air and Ground and Freight Forwarders','Motor Freight Carriers and Trucking - Local and Long Distance Moving and Storage Companies and Local Delivery','Fuel Dealers - Fuel Oil Wood Coal and Liquefied Petroleum','Cleaning Maintenance and Janitorial Services') then 'CONSULTING & SERVICES'
    WHEN     x.mcc_description in ('Grocery Stores and Supermarkets','Miscellaneous Food Stores - Convenience Stores and Specialty Markets','Package Stores - Beer Wine and Liquor','CPG') then 'CPG'
    WHEN     x.mcc_description in ('Construction Materials - Not Elsewhere Classified','Furniture Home Furnishings and Equipment Stores and Manufacturers except Appliances','Lumber and Building Materials Stores','Miscellaneous Home Furnishings Specialty Stores','HOME & INDUSTRIES','Lodging Hotels Motels and Resorts','Stationery Office Supplies Printing and Writing Paper') then 'HOME & INDUSTRIES'
    WHEN     x.mcc_description in ('Dentists and Orthodontists','Doctors and Physicians - Not Elsewhere Classified','Hospitals','Medical Services and Health Practitioners - Not Elsewhere Classified','Dental/Laboratory/Medical/ Ophthalmic Hospital Equipment and Supplies','Drug Stores and Pharmacies','Opticians Optical Goods and Eyeglasses') then 'APPAREL_MEDICAL'
    WHEN     x.mcc_description in ('Civic Social and Fraternal Associations','Membership Clubs (Sports Recreation Athletic) Country Clubs and Private Golf Courses','Religious Organizations','Child Care Services','Political Organizations','Recreation Services - Not Elsewhere Classified','Schools and Educational Services - Not Elsewhere Classified','Tourist Attractions and Exhibits','Charitable and Social Service Organizations') then 'SOCIAL'
    WHEN x.mcc_description in ('Bicycle Shops - Sales and Service','Sporting Goods Stores') then 'APPAREL_SPORTS'
    WHEN x.mcc_description in ('Hobby Toy and Game Stores') then 'TOYS & BABIES'
    WHEN x.mcc_description in ('Bus Lines','Taxicabs and Limousines','Transportation Services - Not Elsewhere Classified') then 'TRAVEL'
    ELSE 'OTROS' END AS MCC,
    x.mcc_category mcc_group
    --COUNT(*)
  FROM meli-bi-data.SBOX_B2B_MKTPLACE.LK_LASTMCC3 y
  LEFT JOIN  WHOWNER.LK_MP_MCC_CODE x
  ON cast(y.MCC_ID as string)=cast(x.mcc_code as string)
  GROUP BY 1,2,3,4,5,6
  --having count(*)>1
  ) ;


----------------------------- 03- Traigo vertical maxima ------------------------------------------

DROP TABLE IF EXISTS meli-bi-data.SBOX_B2B_MKTPLACE.vert;
CREATE TABLE meli-bi-data.SBOX_B2B_MKTPLACE.vert as ( 
  SELECT
      bid.sit_site_id,
      bid.ORD_SELLER.ID  cus_cust_id_sel,
      cat.vertical,
      sum((CASE WHEN coalesce(bid.ORD_FVF_BONIF, True) = FALSE THEN (BID.ORD_ITEM.BASE_CURRENT_PRICE * bid.ORD_ITEM.QTY) ELSE 0.0 END))  TGMV
  FROM WHOWNER.BT_ORD_ORDERS as bid
  LEFT JOIN WHOWNER.LK_ITE_ITEMS ite
  on (bid.ITE_ITEM_ID = ite.ITE_ITEM_ID AND     
  bid.SiT_SITE_ID = ite.SIT_SITE_ID)  

  LEFT JOIN WHOWNER.AG_LK_CAT_CATEGORIES_PH as cat    
  on (ite.sit_site_id = cat.sit_site_id AND
  cast(ite.cat_Categ_id as string) = cat.cat_Categ_id_l7 AND
  cat.photo_id = 'TODATE')  

/*
  LEFT JOIN WHOWNER.LK_SEGMENTO_SELLERS seg
  on bid.cus_cust_id_sel=seg.cus_cust_id_sel AND
  seg.sit_site_id=bid.sit_site_id

  LEFT JOIN WHOWNER.LK_ITE_ATTRIBUTE_VALUES att
  on bid.ITE_ITEM_ID = att.ITE_ITEM_ID AND    
  bid.PHOTO_ID = ite.PHOTO_ID AND    
  bid.SiT_SITE_ID = att.SIT_SITE_ID
  */

  WHERE bid.ORD_CLOSED_DT BETWEEN from_date AND to_date
  AND bid.ORD_GMV_FLG = True -- bid.ite_gmv_flag = 1
  AND bid.ORD_CATEGORY.MARKETPLACE_ID = 'TM' -- bid.mkt_marketplace_id = 'TM'
  AND coalesce(BID.ORD_AUTO_OFFER_FLG, False) <> True --coalesce(BID.AUTO_OFFER_FLAG, 0) <> 1
  AND coalesce(bid.ORD_FVF_BONIF, True) = False --coalesce(bid.BID_FVF_BONIF, 'Y') = 'N'
  AND bid.sit_site_id IN ('MLA','MLM','MLC','MLB')
  group by 1,2,3
  ) ;

----------------------------- 04- Agrupo vertical ------------------------------------------


  DROP TABLE IF EXISTS meli-bi-data.SBOX_B2B_MKTPLACE.vert2;
  CREATE TABLE  meli-bi-data.SBOX_B2B_MKTPLACE.vert2 AS (
  SELECT
    cus_cust_id_sel,
    sit_site_id,
    vertical,
    TGMV
  FROM  meli-bi-data.SBOX_B2B_MKTPLACE.vert
  group by 1,2,3,4
  qualify row_number () over (partition by cus_cust_id_sel, sit_site_id order by  TGMV DESC) = 1
  );


----------------------------- 05- Traigo la plata en cuenta de mercadopago por cust ------------------------------------------

  DROP TABLE IF EXISTS meli-bi-data.SBOX_B2B_MKTPLACE.LK_ACCOUNT_MONEY_CUST;
  CREATE TABLE meli-bi-data.SBOX_B2B_MKTPLACE.LK_ACCOUNT_MONEY_CUST as (
  SELECT
    cus_cust_id,
    sit_site_id,
    AVG (TOTAL_AMOUNT - UNAV_AMOUNT) balance
  FROM  WHOWNER.BT_MP_SALDOS_SNAPSHOT_DAILY
  WHERE TIM_DAY BETWEEN from_date_account_money AND to_date
  AND sit_site_id IN ('MLA','MLM','MLC','MLB')
  GROUP BY 1,2
  ) ;



/* ------------------------- Corro a nivel B2B ID ---------------------- */

----------------------------- 01- Creo campo customer y datos de kyc por B2B ID ------------------------------------------


  DROP TABLE IF EXISTS meli-bi-data.SBOX_B2B_MKTPLACE.kyc_customer;
  CREATE TABLE meli-bi-data.SBOX_B2B_MKTPLACE.kyc_customer AS (
  WITH 
  temp_base AS (
   SELECT 
    DISTINCT
    coalesce(KYC_COMP_IDNT_NUMBER,cast(a.cus_cust_id as string)) b2b_id,
    a.KYC_COMP_IDNT_NUMBER,
    a.kyc_entity_type,
    a.sit_site_id,
    CASE WHEN CHARACTER_LENGTH(REGEXP_REPLACE(a.KYC_COMP_CORPORATE_NAME, '[^0-9]*', ''))=11 THEN 'MEI' ELSE 'NOT MEI' END AS TIPO_MEI,
    CASE WHEN (CASE WHEN CHARACTER_LENGTH(REGEXP_REPLACE(a.KYC_COMP_CORPORATE_NAME, '[^0-9]*', ''))=11 THEN 'MEI' ELSE 'NOT MEI' END)='NOT MEI' THEN 1 ELSE 0 END TIPO_MEI_ID,
    a.cus_cust_id,
    e.CUS_PARTY_TYPE_ID,
    CASE WHEN e.CUS_PARTY_TYPE_ID LIKE '%1P%' OR e.CUS_PARTY_TYPE_ID LIKE '%PL%' THEN 1
    ELSE 0 END customer_id
    FROM WHOWNER.LK_KYC_VAULT_USER a
    LEFT JOIN WHOWNER.LK_CUS_CUSTOMERS_DATA e
    ON a.sit_site_id=e.sit_site_id_cus AND a.cus_cust_id=e.cus_cust_id
    WHERE a.kyc_entity_type= 'company'
  ),
  
  temp_mei_id AS (
    SELECT b2b_id, sum(tipo_mei_id)sum_tipo_mei_id
    FROM temp_base
    GROUP BY b2b_id
  ), 

  temp_sum_customer_id AS (
    SELECT
    b2b_id, KYC_COMP_IDNT_NUMBER, kyc_entity_type, sit_site_id, 
    -- count(cus_cust_id) over (partition by KYC_IDENTIFICATION_NUMBER) count_cust_kyc,
    SUM(customer_id) AS sum_customer_id
    FROM temp_base
    GROUP BY b2b_id, KYC_COMP_IDNT_NUMBER,kyc_entity_type, sit_site_id
  ),

  temp_corporate_name AS (
    SELECT 
    coalesce(KYC_COMP_IDNT_NUMBER,cast(a.cus_cust_id as string)) b2b_id,
    a.KYC_COMP_IDNT_NUMBER,
    a.kyc_entity_type,
    a.KYC_COMP_CORPORATE_NAME,
    a.sit_site_id,
    a.AUD_UPD_DT,
    row_number () over (partition by KYC_COMP_IDNT_NUMBER order by AUD_UPD_DT DESC) ultimo_update
    FROM WHOWNER.LK_KYC_VAULT_USER a
    WHERE a.kyc_entity_type= 'company'
  )

    SELECT
    a.b2b_id,
    a.kyc_entity_type, 
    a.KYC_COMP_IDNT_NUMBER,
    b.KYC_COMP_CORPORATE_NAME,
    a.sit_site_id,
    b.AUD_UPD_DT,
    Case WHEN sum_customer_id>0 THEN 'MELI'
    ELSE '' END AS customer_final,
    CASE WHEN sum_tipo_mei_id>0 THEN 'NOT MEI' ELSE 'MEI' END TIPO_MEI
    FROM temp_sum_customer_id  a
    LEFT JOIN temp_corporate_name b
    ON a.b2b_id=b.b2b_id
    LEFT JOIN temp_mei_id c
    ON a.b2b_id=c.b2b_id
    WHERE ultimo_update=1

  ) ;

----------------------------- 02- Traigo el regimen fiscal del doc------------------------------------------

----- BUG: REVISARRR!!!!
  DROP TABLE IF EXISTS meli-bi-data.SBOX_B2B_MKTPLACE.LK_br_mx_doc;
  CREATE TABLE meli-bi-data.SBOX_B2B_MKTPLACE.LK_br_mx_doc as (
    SELECT
      distinct 
      coalesce(KYC_COMP_IDNT_NUMBER,cast(a.cus_cust_id as string)) b2b_id, 
      a.sit_site_id,
      a.cus_tax_payer_type,
      a.cus_tax_regime   
    FROM LK_TAX_CUST_WRAPPER a
    LEFT JOIN WHOWNER.LK_KYC_VAULT_USER  b
    on a.sit_site_id=b.sit_site_id AND a.cus_cust_id=b.cus_cust_id
    qualify row_number () over (partition by KYC_COMP_IDNT_NUMBER, a.sit_site_id ORDER BY  a.aud_upd_dt DESC) = 1
  ) ;

----------------------------- 03- Traigo la plata en cuenta de mercadopago por B2B ID ------------------------------------------

  DROP TABLE IF EXISTS meli-bi-data.SBOX_B2B_MKTPLACE.LK_ACCOUNT_MONEY_DOC;
  CREATE TABLE meli-bi-data.SBOX_B2B_MKTPLACE.LK_ACCOUNT_MONEY_DOC as (
  WITH temp_account AS (
    SELECT
      coalesce(KYC_COMP_IDNT_NUMBER,cast(a.cus_cust_id as string)) b2b_id, 
      b.kyc_entity_type,
      b.KYC_COMP_IDNT_NUMBER,
      a.cus_cust_id,
      a.sit_site_id,
      AVG (a.balance) balance
    FROM  SBOX_B2B_MKTPLACE.LK_ACCOUNT_MONEY_CUST a
    LEFT JOIN WHOWNER.LK_KYC_VAULT_USER  b
    on a.sit_site_id=b.sit_site_id AND a.cus_cust_id=b.cus_cust_id
    LEFT JOIN WHOWNER.LK_CUS_CUSTOMERS_DATA e
    ON a.sit_site_id=e.sit_site_id_cus AND a.cus_cust_id=e.cus_cust_id
    WHERE 'sink' not in unnest(e.CUS_TAGS) AND b.kyc_entity_type='company'
    GROUP BY 1,2,3,4,5
  )

    SELECT 
    b2b_id,
    kyc_entity_type,
    KYC_COMP_IDNT_NUMBER,
    sit_site_id,
    sum(balance) balance
    FROM temp_account
    GROUP BY  1,2,3,4
  );

----------------------------- 04- Traigo datos de creditos por B2B ID ------------------------------------------

  DROP TABLE IF EXISTS SBOX_B2B_MKTPLACE.LK_credits;
  CREATE TABLE SBOX_B2B_MKTPLACE.LK_credits as (
  SELECT
    coalesce(KYC_COMP_IDNT_NUMBER,cast(a.CUS_CUST_ID_BORROWER as string)) b2b_id, 
    b.kyc_entity_type,
    b.KYC_COMP_IDNT_NUMBER,
    a.CUS_CUST_ID_BORROWER CUS_CUST_ID,
    a.SIT_SITE_ID,
    COUNT(*) total
  FROM WHOWNER.BT_MP_CREDITS a
  LEFT JOIN WHOWNER.LK_KYC_VAULT_USER  b
  on a.sit_site_id=b.sit_site_id AND a.CUS_CUST_ID_BORROWER=b.cus_cust_id
  LEFT JOIN WHOWNER.LK_CUS_CUSTOMERS_DATA e
  ON a.sit_site_id=e.sit_site_id_cus AND a.CUS_CUST_ID_BORROWER=e.cus_cust_id
  WHERE 'sink' not in unnest(e.CUS_TAGS) AND b.kyc_entity_type='company'

  AND CRD_CREDIT_DATE_FINISHED_ID >= from_date
  AND a.sit_site_id IN ('MLA','MLM','MLC','MLB')
  GROUP BY 1,2,3,4,5

  ) ;

----------------------------- 05- Traigo datos de seguros por B2B ID ------------------------------------------

  DROP TABLE IF EXISTS SBOX_B2B_MKTPLACE.LK_seguros;
  CREATE TABLE SBOX_B2B_MKTPLACE.LK_seguros as (
  SELECT
    coalesce(KYC_COMP_IDNT_NUMBER,cast(a.CUS_CUST_ID_buy as string)) b2b_id, 
    b.kyc_entity_type,
    b.KYC_COMP_IDNT_NUMBER,
    a.CUS_CUST_ID_buy CUS_CUST_ID,
    a.SIT_SITE_ID,
    COUNT(*) total
  FROM WHOWNER.BT_INSURANCE_PRODUCT_PURCHASES a
  LEFT JOIN WHOWNER.LK_KYC_VAULT_USER  b
  on a.sit_site_id=b.sit_site_id AND a.CUS_CUST_ID_buy=b.cus_cust_id
  LEFT JOIN WHOWNER.LK_CUS_CUSTOMERS_DATA e
  ON a.sit_site_id=e.sit_site_id_cus AND a.CUS_CUST_ID_buy=e.cus_cust_id
  WHERE 'sink' not in unnest(e.CUS_TAGS) AND b.kyc_entity_type='company'
  AND  INSUR_PURCHASE_STATUS = 'confirmed'
  AND a.sit_site_id IN ('MLA','MLM','MLC','MLB')
  GROUP BY 1,2,3,4,5
  );

----------------------------- 06- Traigo datos de shipping por B2B ID ------------------------------------------

  DROP TABLE IF EXISTS SBOX_B2B_MKTPLACE.LK_seller_shipping;
  CREATE TABLE SBOX_B2B_MKTPLACE.LK_seller_shipping AS (
  SELECT
    coalesce(KYC_COMP_IDNT_NUMBER,cast(bid.ORD_SELLER.ID  as string)) b2b_id, 
    b.kyc_entity_type,
    b.KYC_COMP_IDNT_NUMBER,
    bid.sit_site_id,
    bid.ORD_SELLER.ID  cus_cust_id_sel,
    COUNT(*) total
  FROM WHOWNER.BT_ORD_ORDERS AS bid
  LEFT JOIN WHOWNER.BT_SHP_SHIPMENTS AS shp
  on bid.ORD_SHIPPING.ID = shp.shp_shipment_id AND shp.sit_site_id = bid.sit_site_id

  LEFT JOIN WHOWNER.LK_KYC_VAULT_USER  b
  on bid.sit_site_id=b.sit_site_id AND bid.ORD_SELLER.ID=b.cus_cust_id
  LEFT JOIN WHOWNER.LK_CUS_CUSTOMERS_DATA e
  ON bid.sit_site_id=e.sit_site_id_cus AND bid.ORD_SELLER.ID=e.cus_cust_id
  WHERE 'sink' not in unnest(e.CUS_TAGS)  AND b.kyc_entity_type='company'

  AND bid.sit_site_id IN ('MLA','MLM','MLC','MLB')--- ('MCO', 'MLA', 'MLB', 'MLC', 'MLM', 'MLU', 'MPE')
  AND bid.ORD_CLOSED_DT BETWEEN from_date AND to_date
  AND bid.ORD_GMV_FLG = True -- bid.ite_gmv_flag = 1
  AND bid.ORD_CATEGORY.MARKETPLACE_ID = 'TM' -- bid.mkt_marketplace_id = 'TM'
  AND shp.shp_picking_type_id IN ('xd_drop_off','cross_docking','fulfillment')
  AND bid.ORD_TGMV_FLG= True --tgmv_flag = 1
  group by 1,2,3,4,5
  ) ;

----------------------------- 07- Traigo datos de Loyalty por B2B ID ------------------------------------------

  DROP TABLE IF EXISTS SBOX_B2B_MKTPLACE.LK_LOYALTY_doc;
  CREATE TABLE SBOX_B2B_MKTPLACE.LK_LOYALTY_doc AS (
    SELECT 
     b2b_id,
    max(LYL_LEVEL_NUMBER) LYL_LEVEL_NUMBER
  FROM SBOX_B2B_MKTPLACE.LK_LOYALTY_cust
  GROUP BY 1
  )  ;


