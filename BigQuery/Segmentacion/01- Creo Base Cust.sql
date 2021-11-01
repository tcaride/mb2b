
/* 00- Me traigo una tabla de Finanzas que tiene la responsabilidad fiscal. Ej: Monostributista, IVA Responsable Inscripto, etc.
Como esta tabla tiene valores que se actualizaron, me traigo la ultima actualizacion. */

CREATE multiset volatile TABLE B2B_TAX_CUST as (
SELECT
cus_cust_id,
sit_site_id,
cus_doc_type, 
cus_doc_number,
cus_business_doc, 
cus_business_name
cus_tax_payer_type,
cus_tax_regime
FROM LK_TAX_CUST_WRAPPER
qualify row_number () over (partition by cus_cust_id, sit_site_id ORDER BY aud_upd_dt DESC) = 1
) WITH data primary index (sit_site_id,cus_cust_id) on COMMIT preserve rows;


/* 01- Creo la tabla a nivel cust. Uso el Vault de KYC que tiene informacion chequeada con los Bureau y completan la info para 
clientes nuevos. Agrego por las dudas la base de Customers para completar usando la misma logica que KYC:
https://docs.google.com/presentation/d/1ExEk8mfT-Z9J6pibfZ8WUqegUOJzDYRpObmZLZzGVHs/edit#slide=id.g8a0e826e31_0_19
Tambien uso unas tablas de Regulations.
. */

CREATE multiset volatile TABLE B2B_BASE_CUST_2 as (
SELECT
d.cus_cust_id,
d.sit_site_id_CUS,
CASE WHEN d.cus_internal_tags LIKE '%internal_user%' OR d.cus_internal_tags LIKE '%internal_third_party%' THEN 'MELI-1P/PL'
    WHEN d.cus_internal_tags LIKE '%cancelled_account%' THEN 'Cuenta_ELIMINADA'
    WHEN d.cus_internal_tags LIKE '%operators_root%' THEN 'Operador_Root'
    WHEN d.cus_internal_tags LIKE '%operator%' THEN 'Operador'
    ELSE 'OK' END AS customer,
(CASE WHEN  (d.sit_site_id_cus='MLA') THEN
        CASE WHEN kycv.entity_type IN('company') THEN 'Company'
            WHEN D.cus_cust_doc_type IN ('CUIL','CUIT') AND (left(D.cus_cust_doc_number,1)='3' OR left(D.cus_cust_doc_number,1)='55') THEN 'Company'
            ELSE 'Not Company' END
    WHEN (d.sit_site_id_cus='MLB') THEN
        CASE WHEN kycv.entity_type IN('company') THEN 'Company'
            WHEN D.cus_cust_doc_type In ('CNPJ') THEN 'Company'
            ELSE 'Not Company' END
    WHEN  (d.sit_site_id_cus='MLM') THEN
        CASE WHEN D.cus_cust_doc_type In ('RFC') and length(d.cus_cust_doc_number) = 12  THEN 'Company'
            WHEN kycv.entity_type IN('company') THEN 'Company'
            ELSE 'Not Company' END             
    WHEN  (d.sit_site_id_cus='MLC') THEN
        CASE WHEN kycv.entity_type IN('company') THEN 'Company'
            WHEN cast(regexp_substr(regexp_replace(D.cus_cust_doc_number,'[.$+*/&¿?! ]'),'^[0-9]+') AS bigint) between 50000000 and 9999999 THEN 'Company'
            ELSE 'Not Company' END
    ELSE 'ERROR_SITE'
END) AS reg_data_type_group, -- tipo documento
tax.cus_tax_payer_type,
--tax.cus_tax_regime,

COUNT(*) cuenta

FROM meli-bi-data.WHOWNER.LK_KYC_VAULT_USERS kycv
LEFT JOIN meli-bi-data.WHOWNER.LK_CUS_CUSTOMERS_DATA d ON kycv.cus_cust_id=d.cus_cust_id

LEFT JOIN B2B_TAX_CUST tax on tax.CUS_CUST_ID=kycv.CUS_CUST_ID and tax.sit_site_id=d.sit_site_id_cus -- Revisar el tax_site
WHERE COALESCE(d.cus_tags, '') <> 'sink'
--AND kycv.sit_site_id_cus IN (${vars})
GROUP BY 1,2,3,4,5
--having  CUSTOMER not in ('Operador','MELI-1P/PL')
) WITH data primary index (cus_cust_id,SIT_SITE_ID_cus) on commit preserve rows;




create multiset volatile table BASE_CUST as (
select
cus_cust_id,
sit_site_id_CUS,
cus_tax_payer_type,
CUSTOMER,
REG_DATA_TYPE_group

from B2B_BASE_CUST_2

qualify row_number () over (partition by cus_cust_id, sit_site_id_cus order by  REG_DATA_TYPE_group asc) = 1

) with data primary index (cus_cust_id,SIT_SITE_ID_cus) on commit preserve rows;