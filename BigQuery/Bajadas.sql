---- BAJADAS  :LOYALTY

	SELECT sit_site_id,cus_cust_id,LYL_LEVEL_NUMBER 
	FROM  BT_LYL_POINTS_SNAPSHOT
	WHERE tim_month_id = '202012'
	AND sit_site_id = 'MLA'



---- BAJADAS  :LK_TAX_CUST_WRAPPER

	SELECT
	  cus_cust_id,
	  sit_site_id,
	  cus_tax_payer_type,
	  cus_tax_regime   ----Charly: NO TIENE SENTIDO XQ SON TODOS NULL en argentina
	FROM LK_TAX_CUST_WRAPPER
	WHERE sit_site_id IN ('MLA')
	qualify row_number () over (partition by cus_cust_id, sit_site_id ORDER BY  aud_upd_dt DESC) = 1


	--- 
	CREATE TABLE meli-bi-data.SBOX_B2B_MKTPLACE.LK_TAX_CUST_WRAPPER AS (
	SELECT * FROM meli-bi-data.SBOX_B2B_MKTPLACE.LK_TAX_CUST_WRAPPER_BR
	UNION ALL 
	SELECT * FROM meli-bi-data.SBOX_B2B_MKTPLACE.LK_TAX_CUST_WRAPPER_MX
	);


--- KYC VAULT 
	select  cus_cust_id,sit_site_id,kyc_entity_type, KYC_IDENTIFICATION_NUMBER

	from LK_KYC_VAULT_USER

	WHERE kyc_entity_type =  'company'
