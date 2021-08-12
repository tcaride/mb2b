
/* 00- Me traigo una tabla de Finanzas que tiene la responsabilidad fiscal. Ej: Monostributista, IVA Responsable Inscripto, etc.
Como esta tabla tiene valores que se actualizaron, me traigo la ultima actualizacion. */

CREATE multiset volatile TABLE br_mx as (
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




create multiset volatile table BASE_CUST_2 as (
select
D.cus_cust_id,
d.sit_site_id_CUS,




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
                  WHEN D.cus_cust_doc_type IN ('CUIL','CUIT') and left(D.cus_cust_doc_number,1)='3' THEN 'Company'
                  WHEN KYC.cus_doc_type IN ('CUIL','CUIT') and left(KYC.cus_doc_number,1)='3' THEN 'Company'
                  WHEN KYC2.CUS_KYC_ENTITY_TYPE IN('company') THEN 'Company'
                  --WHEN KYC.cus_doc_type IN ('CUIL') and left(KYC.cus_doc_number,1)='2' THEN 'person'
                 --WHEN KYC.cus_doc_type IN ('DNI','CI','LC','LE') THEN 'person'
                 --WHEN D.cus_cust_doc_type IN ('CUIT') and left(D.cus_cust_doc_number,1)='2' THEN 'Not Company'
                  --WHEN KYC.cus_doc_type IN ('CUIT') and left(KYC.cus_doc_number,1)='2' THEN 'Not Company'
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

tax.cus_tax_payer_type,
--tax.cus_tax_regime,

COUNT(*) cuenta

FROM WHOWNER.LK_CUS_CUSTOMERS_DATA D  -- base de clientes
LEFT JOIN WHOWNER.LK_REG_CUSTOMERS R ON d.CUS_CUST_ID=R.CUS_CUST_ID and d.sit_site_id_cus=r.sit_site_id
LEFT JOIN WHOWNER.LK_REG_PERSON R1 ON R.REG_CUST_DOC_TYPE = R1.REG_CUST_DOC_TYPE and R.REG_CUST_DOC_NUMBER = R1.REG_CUST_DOC_NUMBER and r1.sit_site_id=r.sit_site_id
LEFT JOIN WHOWNER.LK_KYC_CUSTOMERS KYC ON KYC.CUS_CUST_ID=D.CUS_CUST_ID
LEFT JOIN WHOWNER.BT_MP_KYC_LEVEL KYC2 ON  KYC2.CUS_CUST_ID=D.CUS_CUST_ID
 LEFT JOIN br_mx tax on tax.CUS_CUST_ID=D.CUS_CUST_ID and tax.sit_site_id=d.sit_site_id_cus

where COALESCE(D.CUS_TAGS, '') <> 'sink'
AND D.sit_site_id_cus IN (${vars})
group by 1,2,3,4,5
--having  CUSTOMER not in ('Operador','MELI-1P/PL')
) with data primary index (cus_cust_id,SIT_SITE_ID_cus) on commit preserve rows;




create multiset volatile table BASE_CUST as (
select
cus_cust_id,
sit_site_id_CUS,
cus_tax_payer_type,
CUSTOMER,
REG_DATA_TYPE_group

from BASE_CUST_2

qualify row_number () over (partition by cus_cust_id, sit_site_id_cus order by  REG_DATA_TYPE_group asc) = 1

) with data primary index (cus_cust_id,SIT_SITE_ID_cus) on commit preserve rows;