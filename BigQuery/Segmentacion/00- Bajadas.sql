---- BAJADAS  :LOYALTY
  
  SELECT
    coalesce(KYC_COMP_IDNT_NUMBER,h.cus_cust_id) b2b_id, 
    b.KYC_COMP_IDNT_NUMBER, -- 3
    b.KYC_ENTITY_TYPE, -- 4
    h.cus_cust_id, 
    h.sit_site_id,
    h.LYL_LEVEL_NUMBER 
  FROM  LK_KYC_VAULT_USER b
  LEFT JOIN WHOWNER.LK_CUS_CUSTOMERS_DATA e
  ON b.sit_site_id=e.sit_site_id_cus AND b.cus_cust_id=e.cus_cust_id
  LEFT JOIN whowner.BT_LYL_POINTS_SNAPSHOT h 
  ON b.cus_cust_id=h.cus_cust_id AND h.tim_month_id = '202110'
  WHERE COALESCE(e.CUS_TAGS, '') <> 'sink' AND b.kyc_entity_type='company'


---- BAJADAS  :LK_TAX_CUST_WRAPPER

 	SELECT
 		a.cus_cust_id,
	  	a.sit_site_id,
	  	a.cus_tax_payer_type,
	  	a.cus_tax_regime ,
	 	 a.aud_upd_dt
	FROM LK_TAX_CUST_WRAPPER a
	LEFT JOIN LK_KYC_VAULT_USER b
	ON a.cus_cust_id =b.cus_cust_id
	WHERE a.sit_site_id in ('MLB') AND b.kyc_entity_type = 'company'
