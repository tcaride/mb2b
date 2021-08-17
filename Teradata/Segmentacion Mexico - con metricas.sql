/* SEGMENTACION MEXICO TERADATA. */

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
AND mp.sit_site_id IN ('MLM')
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
where bid.sit_site_id IN ('MLM')
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
  cus_cust_id_sel,
  sit_site_id,
  SUM(VENTAS_USD) VENTAS_USD,
  SUM(Q) Q,
  COUNT(distinct tpv_segment_id) Q_SEG
FROM TPV_SEL_1
GROUP BY 1,2
) WITH data primary index (CUS_CUST_ID_SEL,SIT_SITE_ID) on commit preserve rows;

----------------------------- 03. Query para tener el segmento de MP, tomo el mayor puede que haya seller en mas de un segmento -----------------------------------------

CREATE multiset volatile TABLE SEG_SEL as ( 
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

----------------------------- 04. Agrupo volumen de compras por buyer y categoria ------------------------------------------

CREATE MULTISET VOLATILE TABLE BGMV_BUY AS ( --- compras en el marketplace por empresa
SELECT
    bid.cus_cust_id_buy,  
    bid.sit_site_id,
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
LEFT JOIN WHOWNER.LK_ITE_ITEMS_PH ite 
on (bid.ITE_ITEM_ID = ite.ITE_ITEM_ID AND    
bid.PHOTO_ID = ite.PHOTO_ID AND    
bid.SiT_SITE_ID = ite.SIT_SITE_ID)  

LEFT JOIN WHOWNER.AG_LK_CAT_CATEGORIES_PH AS cat    
on (ite.sit_site_id = cat.sit_site_id AND
ite.cat_Categ_id = cat.cat_Categ_id_l7 AND
cat.photo_id = 'TODATE')  

WHERE bid.sit_site_id IN ('MLM')
AND bid.photo_id = 'TODATE'
AND bid.tim_day_winning_date BETWEEN DATE '2020-01-01' AND DATE '2020-12-31'
AND bid.ite_gmv_flag = 1
AND bid.mkt_marketplace_id = 'TM'
AND tgmv_flag = 1
GROUP BY 1,2
)WITH DATA PRIMARY INDEX (sit_site_id,cus_cust_id_buy) ON COMMIT PRESERVE ROWS;

----------------------------- 05. Traigo los Q en los que hizo compras. -----------------------------------------

CREATE MULTISET VOLATILE TABLE BGMV_TIPO_COMPRADOR AS ( --- compras en el marketplace por usuario 
SELECT
    bid.cus_cust_id_buy,  
    bid.sit_site_id,
     ((CAST(EXTRACT(MONTH FROM bid.TIM_DAY_WINNING_DATE) AS BYTEINT)-1)/3)+1
  || 'Q' || substring(bid.TIM_DAY_WINNING_DATE,3,2) quarter,    
    COUNT(distinct (case when bid.crt_purchase_id is null then bid.ord_order_id else bid.crt_purchase_id end)) as TX_BUY
FROM WHOWNER.BT_BIDS as bid
WHERE bid.sit_site_id IN ('MLM')
AND bid.photo_id = 'TODATE' 
AND bid.tim_day_winning_date between DATE '2020-01-01' AND DATE '2020-12-31'
AND bid.ite_gmv_flag = 1
AND bid.mkt_marketplace_id = 'TM'
AND tgmv_flag = 1
group by 1,2,3
)WITH DATA PRIMARY INDEX (sit_site_id,cus_cust_id_buy) ON COMMIT PRESERVE ROWS;

----------------------------- 06. Categorizo tipo de comprador -----------------------------------------

CREATE MULTISET VOLATILE TABLE BGMV_TIPO_COMPRADOR_2 AS (
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

)WITH DATA PRIMARY INDEX (sit_site_id,cus_cust_id_buy) ON COMMIT PRESERVE ROWS;

----------------------------- 07. Traigo el regimen fiscal del cust: Para argentina no son consistentes los datos pero mantengo para mantener las columnas ------------------------------------------

CREATE multiset volatile TABLE br_mx as ( -- tipo de regimen
SELECT
  cus_cust_id,
  sit_site_id,
  cus_tax_payer_type,
  cus_tax_regime   
FROM LK_TAX_CUST_WRAPPER
WHERE sit_site_id IN ('MLM')
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

----------------------------- 10. Traigo vertical maxima ------------------------------------------

CREATE multiset volatile TABLE vert as ( -- vertical maxima
SELECT
    bid.sit_site_id,
    bid.cus_cust_id_sel,
    cat.vertical,
    sum((Case when coalesce(bid.BID_FVF_BONIF, 'Y') = 'N' then (BID.BID_base_CURRENT_PRICE * bid.BID_QUANTITY_OK) else 0.0 end))  GMVEBILLABLE
FROM WHOWNER.BT_BIDS as bid
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

WHERE bid.sit_site_id IN ('MLM')
AND bid.photo_id = 'TODATE'
AND bid.tim_day_winning_date BETWEEN DATE '2020-01-01' AND DATE '2020-12-31'
AND bid.ite_gmv_flag = 1
AND bid.mkt_marketplace_id = 'TM'
AND coalesce(BID.AUTO_OFFER_FLAG, 0) <> 1
AND coalesce(bid.BID_FVF_BONIF, 'Y') = 'N'
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


----------------------------- 13. Traigo cantidad de integrados por seller ------------------------------------------

CREATE multiset volatile TABLE intergrados as ( --- cantidad de integraciones a partir del get
SELECT
cus_cust_id,
sit_site_id,
COUNT(*) CANT_INTEGRADOS
FROM  WHOWNER.lk_op_active_users us
WHERE us.date_end >=  '2020-12-31' --- trae los que no finalizaron en 2020
AND us.sit_site_id IN ('MLM')
group by 1,2
) with data primary index (sit_site_id,cus_cust_id) on commit preserve rows;

----------------------------- 14. Trae la plata en cuenta de mercadopago ------------------------------------------

CREATE multiset volatile TABLE account_money as (
SELECT
  cus_cust_id,
  sit_site_id,
  AVG (AVAILABLE_BALANCE) balance
FROM  BT_MP_SALDOS_SITE
WHERE TIM_DAY BETWEEN DATE '2021-03-01' AND DATE '2021-04-30'
AND sit_site_id IN ('MLM')
GROUP BY 1,2
) with data primary index (sit_site_id,cus_cust_id) on commit preserve rows;

----------------------------- 15. Categoriza la account money ------------------------------------------

CREATE multiset volatile TABLE account_money2 as (
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
  WHEN  am.balance<((100*20)/5) THEN 'Menos 400 Pesos'
  WHEN am.balance<=((500*20)/5) THEN '400 a 2000 Pesos'
  WHEN am.balance<=((1500*20)/5) THEN '2000 a 6000 Pesos'
  WHEN am.balance<=((5000*20)/5) THEN '6000 a 20000 Pesos'
  WHEN am.balance<=((15000*20)/5) THEN '20000 a 60000 Pesos'
  WHEN am.balance<=((50000*20)/5) THEN '60000 a 200000 Pesos'
  ELSE 'Mas de 200000 Pesos' END as ACCOUNT_MONEY

FROM TPV_SEL AS V
FULL OUTER JOIN account_money am
on am.CUS_CUST_ID=V.CUS_CUST_ID_SEL
AND am.sit_site_id=V.sit_site_id
) with data primary index (sit_site_id,cus_cust_id) on commit preserve rows;

----------------------------- 16. Trae datos de creditos ------------------------------------------

CREATE multiset volatile TABLE credits as (
SELECT
  CUS_CUST_ID_BORROWER CUS_CUST_ID,
  SIT_SITE_ID,
  COUNT(*) total
FROM WHOWNER.BT_MP_CREDITS
WHERE CRD_CREDIT_FINISH_DATE_ID >= DATE '2020-01-01'
AND sit_site_id='MLM'
GROUP BY 1,2
) with data primary index (sit_site_id,cus_cust_id) on commit preserve rows;

----------------------------- 17. Trae datos de seguros ------------------------------------------

CREATE multiset volatile TABLE seguros as (
SELECT
  CUS_CUST_ID_buy,
  COUNT(*) total
FROM WHOWNER.BT_INSURANCE_PURCHASES
WHERE INSUR_STATUS_ID = 'confirmed'
and SIT_SITE_ID='MLM'
GROUP BY 1
) with data primary index (cus_cust_id_buy) on commit preserve rows;

----------------------------- 18. Trae datos de shipping ------------------------------------------

CREATE multiset volatile TABLE seller_shipping AS (
SELECT
  bid.sit_site_id,
  bid.cus_cust_id_sel cus_cust_id_sel,
  COUNT(*) total
FROM WHOWNER.BT_BIDS AS bid
LEFT JOIN WHOWNER.BT_SHP_SHIPMENTS AS shp
on bid.shp_shipment_id = shp.shp_shipment_id AND shp.sit_site_id = bid.sit_site_id
WHERE bid.sit_site_id = 'MLM'--- ('MCO', 'MLA', 'MLB', 'MLC', 'MLM', 'MLU', 'MPE')
AND bid.photo_id = 'TODATE'
AND bid.tim_day_winning_date BETWEEN DATE '2020-10-01' AND DATE '2020-12-31' --OR bid.tim_day_winning_date between DATE '2019-01-01' and '2019-10-31')
AND bid.ite_gmv_flag = 1
AND bid.mkt_marketplace_id = 'TM'
AND shp.shp_picking_type_id IN ('xd_drop_off','cross_docking','fulfillment')
AND tgmv_flag = 1
group by 1,2
) with data primary index (sit_site_id,cus_cust_id_SEL) on commit preserve rows;

----------------------------- 19. Crea la tabla final ------------------------------------------

----------------------------- Creo la base de cust de empresas  ------------------------------------------
/* Traigo los datos del Vault de KYC marcados como company segun las relglas para el entity type por pais:
https://docs.google.com/presentation/d/1ExEk8mfT-Z9J6pibfZ8WUqegUOJzDYRpObmZLZzGVHs/edit#slide=id.g7735a7c26a_2_3
Y elimino los usuarios sink de la tabla de customers, son usuarios creados para carritos y cosas internas.
*/



SELECT
  a.CUS_CUST_ID,
  a.SIT_SITE_ID,
  a.KYC_IDENTIFICATION_NUMBER, --
  CASE WHEN b.tpv_segment_detail ='Aggregator - Other' THEN 'Online Payments' 
      WHEN b.tpv_segment_detail ='Instore' THEN 'QR'
      WHEN b.tpv_segment_detail ='Selling Marketplace' THEN 'Selling Marketplace'
      WHEN b.tpv_segment_detail ='Point' then 'Point'
      WHEN b.tpv_segment_detail is null then 'No Vende'
      ELSE 'Not Considered'
  END as Canal,
  CASE WHEN Canal='QR' OR Canal='Point' THEN 'OF'
    WHEN Canal='Selling Marketplace' OR Canal='Online Payments' THEN 'ON'
    ELSE Canal 
  END as Agg_Canal,
  b.SEGMENTO SEGMENTO_MKTPLACE,
  CASE WHEN b.tpv_segment_detail ='Selling Marketplace' THEN d.vertical 
      ELSE c.MCC 
  END AS RUBRO,
  CASE WHEN e.cus_internal_tags LIKE '%internal_user%' OR e.cus_internal_tags LIKE '%internal_third_party%' THEN 'MELI-1P/PL'
    WHEN e.cus_internal_tags LIKE '%cancelled_account%' THEN 'Cuenta_ELIMINADA'
    WHEN e.cus_internal_tags LIKE '%operators_root%' THEN 'Operador_Root'
    WHEN e.cus_internal_tags LIKE '%operator%' THEN 'Operador'
    ELSE 'OK' 
  END AS CUSTOMER,
  a.KYC_ENTITY_TYPE AS REG_DATA_TYPE_group,
  f.cus_tax_payer_type,
  CASE WHEN g.GMVEBILLABLE IS NULL or g.GMVEBILLABLE=0 THEN 'No Compra' ELSE 'Compra' END  as TIPO_COMPRADOR_TGMV, 
  CASE WHEN h.VENTAS_USD IS null THEN 'a.No Vende'
    WHEN h.VENTAS_USD= 0 THEN 'a.No Vende'
    WHEN h.VENTAS_USD <= 6000 THEN 'b.Menos 6.000'
    WHEN h.VENTAS_USD <= 40000 THEN 'c.6.000 a 40.000'
    WHEN h.VENTAS_USD<= 200000 THEN 'd.40.000 a 200.000'
    ELSE 'e.Mas de 200.000' 
  END AS RANGO_VTA_PURO, 
    CASE WHEN h.VENTAS_USD IS null THEN 'No Vende'
    WHEN h.VENTAS_USD <= 230000 THEN 'Micro'
    WHEN h.VENTAS_USD <= 4750000 THEN 'Pequeña'
    WHEN h.VENTAS_USD <= 12500000 THEN 'Mediana'
    ELSE 'Grande' 
  END AS Tamaño_Ventas_ML, 
  CASE WHEN REG_DATA_TYPE_group='Company' AND Canal<>'Not Considered' THEN 'ok'
    WHEN REG_DATA_TYPE_group <>'Company' AND Canal<>'Not Considered' AND RANGO_VTA_PURO not IN ('a.No Vende','b.Menos 6.000') THEN 'ok'
    ELSE 'no ok'
  END as Baseline,
  CASE WHEN i.cus_cust_id IS null THEN 'Sin Integrador'
    WHEN i.CANT_INTEGRADOS = 1 THEN 'Integrador Unico'  
    WHEN i.CANT_INTEGRADOS <= 3 THEN '2 a 3 Integradores'
    WHEN i.CANT_INTEGRADOS <= 10 THEN '4 a 10 Integradores'
    ELSE 'Mas de 10 integradores' 
  END AS INTEGRACION, 
  j.ACCOUNT_MONEY,
  CASE WHEN j.ACCOUNT_MONEY ='No tiene AM' THEN 0
    WHEN j.ACCOUNT_MONEY ='Menos 400 Pesos' or  j.ACCOUNT_MONEY = '400 a 2000 Pesos' THEN 1
    WHEN  j.ACCOUNT_MONEY = '2000 a 6000 Pesos' or  j.ACCOUNT_MONEY = '6000 a 20000 Pesos' THEN 2
    ELSE 3 
  END as am_rank,
  j.Ratio_AM_VTAS,
  CASE WHEN k.LYL_LEVEL_NUMBER = 1 or k.LYL_LEVEL_NUMBER =2 THEN 1
    WHEN k.LYL_LEVEL_NUMBER = 3 or k.LYL_LEVEL_NUMBER =4 THEN 2
    WHEN k.LYL_LEVEL_NUMBER = 5 or k.LYL_LEVEL_NUMBER =6 THEN 3
    ELSE NULL
  END AS LOYALTY,
  CASE WHEN h.Q_SEG = 1 or h.Q_SEG =2 THEN 1
    WHEN h.Q_SEG = 3 or h.Q_SEG =4  THEN 2
    WHEN h.Q_SEG = 5 THEN 3
    ELSE NULL
  END AS ECOSISTEMA,
  CASE WHEN l.cus_cust_id_buy IS null THEN 0 ELSE 1 END AS SEGUROS,
  CASE WHEN m.cus_cust_id IS null THEN 0 ELSE 1 END as CREDITOS,
  CASE WHEN n.cus_cust_id_sel IS null THEN 0 ELSE 1 END as shipping,
  CASE WHEN g.GMVEBILLABLE>0 THEN
    CASE WHEN g.TGMV_COMP/g.GMVEBILLABLE >=0.45 THEN 'Compras Perfil Empresa' 
        ELSE 'Compras Perfil No Empresa' 
    END 
    ELSE 'No Compras' 
  END AS Tipo_Compras,
  CASE WHEN g.GMVEBILLABLE>0 THEN
    CASE WHEN RUBRO='ACC' AND g.TGMV_AUTO/g.GMVEBILLABLE >=0.40 THEN 'Resale'
      WHEN RUBRO='BEAUTY' AND g.TGMV_BEAUTY/g.GMVEBILLABLE >=0.40 THEN 'Resale' 
      WHEN RUBRO='CE' AND g.TGMV_CE/g.GMVEBILLABLE >=0.40 THEN 'Resale' 
      ELSE 'No Resale'
    END 
    ELSE 'No Compras' 
  END AS Objetivo_Compras,
  CASE WHEN g.GMVEBILLABLE IS NULL AND o.NB='Nunca Compro' THEN 'Not Buyer'
    WHEN g.GMVEBILLABLE IS NULL AND o.NB='Compro' THEN 'Recover'
    WHEN g.GMVEBILLABLE IS not NULL AND o.q_cuenta in ('Menos 1Q','1Q')  AND o.cant_q_compras >=1 THEN 'Frequent_NB'
    WHEN g.GMVEBILLABLE IS not NULL AND o.q_cuenta in ('Menos 1Q','1Q')  AND o.cant_q_compras <1 THEN 'Non Frequent'
    WHEN g.GMVEBILLABLE IS not NULL AND o.q_cuenta='2Q' AND o.cant_q_compras >=2 THEN 'Frequent_NB'
    WHEN g.GMVEBILLABLE IS not NULL AND o.q_cuenta='2Q' AND o.cant_q_compras <2 THEN 'Non Frequent'
    WHEN g.GMVEBILLABLE IS not NULL AND o.q_cuenta='3Q' AND o.cant_q_compras >=3 THEN 'Frequent_NB'
    WHEN g.GMVEBILLABLE IS not NULL AND o.q_cuenta='3Q' AND o.cant_q_compras <3 THEN 'Non Frequent'
    WHEN g.GMVEBILLABLE IS not NULL AND o.q_cuenta='OK' AND o.cant_q_compras >=4 THEN 'Frequent'
    WHEN g.GMVEBILLABLE IS not NULL AND o.q_cuenta='OK' AND o.cant_q_compras <4 THEN 'Non Frequent'
    ELSE 'TBD'
  END AS Frequencia,
  (3*coalesce(LOYALTY,0))+(4*coalesce(ECOSISTEMA,0))+(3*coalesce(am_rank,0)) engagement,
  h.VENTAS_USD VENTAS_USD,
  g.GMVEBILLABLE  bgmv_cpras, 
  COUNT(DISTINCT CASE WHEN g.cus_cust_id_buy is null THEN h.cus_cust_id_sel ELSE g.cus_cust_id_buy END) AS cust_total,
  COUNT(DISTINCT g.cus_cust_id_buy) cust_buy

FROM LK_KYC_VAULT_USER a
LEFT JOIN Seg_Sel b ON a.cus_cust_id=b.cus_cust_id_sel
LEFT JOIN lastmcc4 c ON a.cus_cust_id = c.cus_cust_id
LEFT JOIN vert2 d ON a.cus_cust_id=d.cus_cust_id_sel
LEFT JOIN LK_CUS_CUSTOMERS_DATA e ON  a.cus_cust_id=e.cus_cust_id
LEFT JOIN br_mx f ON a.cus_cust_id=f.cus_cust_id
LEFT JOIN BGMV_BUY AS g ON a.cus_cust_id=g.cus_cust_id_buy 
LEFT JOIN TPV_SEL AS h ON a.cus_cust_id=h.cus_cust_id_sel 
LEFT JOIN intergrados i ON a.cus_cust_id=i.cus_cust_id
LEFT JOIN account_money2 j ON a.cus_cust_id=j.cus_cust_id
LEFT JOIN BT_LYL_POINTS_SNAPSHOT k ON a.cus_cust_id=k.cus_cust_id AND k.tim_month_id = '202012'
LEFT JOIN seguros l ON a.cus_cust_id=l.CUS_CUST_ID_buy 
LEFT JOIN credits m ON a.CUS_CUST_ID=m.CUS_CUST_ID
LEFT JOIN seller_shipping n ON a.cus_cust_id=n.CUS_CUST_ID_sel
LEFT JOIN BGMV_TIPO_COMPRADOR_2 o ON a.cus_cust_id=o.cus_cust_id_buy 
WHERE COALESCE(e.CUS_TAGS, '') <> 'sink' AND ((a.KYC_ENTITY_TYPE = 'company' AND
 (Tipo_Compras<>'No Compras' AND RANGO_VTA_PURO<> 'a.No Vende') )
  OR (a.KYC_ENTITY_TYPE <> 'company' AND  h.VENTAS_USD >= 6000)) 
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22, 23, 24,25,26, 27,28










