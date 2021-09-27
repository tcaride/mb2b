
CREATE multiset volatile TABLE br_mx as (
SELECT
  cus_cust_id,
  sit_site_id,
  cus_tax_payer_type,
  cus_tax_regime
FROM LK_TAX_CUST_WRAPPER
WHERE SIT_SITE_ID IN ('MLB','MLA','MLM','MLC')
qualify row_number () over (partition by cus_cust_id, sit_site_id order by  aud_upd_dt DESC) = 1
) with data primary index (sit_site_id,cus_cust_id) on commit preserve rows;


create multiset volatile table BASE_CUST as (
select
D.cus_cust_id,
d.sit_site_id_CUS,
BID.ord_order_id,


case  when d.cus_internal_tags like '%internal_user%' or d.cus_internal_tags like '%internal_third_party%' then 'MELI-1P/PL'
    when d.cus_internal_tags like '%cancelled_account%' then 'Cuenta_ELIMINADA'
     when d.cus_internal_tags like '%operators_root%' then 'Operador_Root'
     when d.cus_internal_tags like '%operator%' then 'Operador'
      ELSE 'OK' end as CUSTOMER,
    KYC.KYC_ENTITY_TYPE cust_type, -- tipo documento
    tax.cus_tax_payer_type tax_payer, 
    COUNT(*) cuenta

from WHOWNER.BT_BIDS bid
join WHOWNER.LK_CUS_CUSTOMERS_DATA D  ON bid.cus_cust_id_buy = d.cus_cust_id and bid.sit_site_id=d.sit_site_id_cus-- base de clientes
LEFT JOIN WHOWNER.LK_KYC_VAULT_USER KYC ON KYC.CUS_CUST_ID=D.CUS_CUST_ID
LEFT JOIN br_mx tax on tax.CUS_CUST_ID=D.CUS_CUST_ID and tax.sit_site_id=d.sit_site_id_cus

where COALESCE(D.CUS_TAGS, '') <> 'sink'
and bid.sit_site_id IN (${vars})
and bid.photo_id = 'TODATE' 
and bid.tim_day_winning_date between date ${start_date} and date ${end_date} 
and bid.ite_gmv_flag = 1
and bid.mkt_marketplace_id = 'TM'


group by 1,2,3,4,5,6
--having  CUSTOMER not in ('Operador','MELI-1P/PL')
) with data primary index (cus_cust_id,SIT_SITE_ID_cus) on commit preserve rows;






--create multiset volatile table BASE_FINAL_ORDEN AS (
--SELECT 
--BID.ord_order_id,
--d.CUST_TYPE
--FROM WHOWNER.BT_BIDS bid
--JOIN BASE_CUST d ON bid.cus_cust_id_buy = d.cus_cust_id and bid.sit_site_id=d.sit_site_id_cus
--where bid.sit_site_id IN (${vars})
--and bid.photo_id = 'TODATE' 
--and bid.tim_day_winning_date between date ${start_date} and date ${end_date} 
--and bid.ite_gmv_flag = 1
--and bid.mkt_marketplace_id = 'TM'
-- and tgmv_flag = 1
--)WITH DATA PRIMARY INDEX (ord_order_id, CUST_TYPE) ON COMMIT PRESERVE ROWS;








--drop table input_1;
--drop table input_2;

CREATE MULTISET VOLATILE TABLE input_1 AS (
SELECT
SIT_SITE_ID,
ORD_ORDER_ID,
--SHP_SHIPMENT_ID,
--CUS_CUST_ID_SEL,
UE_VERTICAL,
ITE_SUPERMARKET_FLAG,
UE_FS_TYPE,
UE_CATEG_L1,
UE_COMBO,
UE_ME_PICKING_TYPE,
UE_SELLER_SEGMENT,
DIA,
UE_MONTH_ID,
ue_MONTH_BPP,
UE_MONTH_ID AS MONTH_FINAL,
sum(cast(coalesce(UE_GMVE_BILLABLE_LC,0) as decimal(20,6))) UE_GMVE_BILLABLE_LC,
count(distinct shp_shipment_id) q_envios,
sum(cast(coalesce(UE_REBATE,0) as decimal(20,6))) UE_REBATE,
sum(cast(coalesce(UE_FVF_REAL_LC,0) as decimal(20,6))) UE_FVF_REAL_LC,
sum(cast(coalesce(UE_FLAT_FEE_LC,0) as decimal(20,6))) UE_FLAT_FEE_LC,
sum(cast(coalesce(UE_DISCOUNT_LC,0) as decimal(20,6))) UE_DISCOUNT_LC,
sum(cast(coalesce(UE_OTHER_REVS_ACCOUNT_LC,0) as decimal(20,6))) UE_OTHER_REVS_ACCOUNT_LC,
sum(cast(coalesce(UE_FINANCIAL_FEE_LC,0) as decimal(20,6))) UE_FINANCIAL_FEE_LC,
sum(cast(coalesce(UE_REVS_ME,0) as decimal(20,6))) UE_REVS_ME,
sum(cast(coalesce(UE_OTHER_PAYMENTS_REVS_ACCOUNT_LC,0) as decimal(20,6))) UE_OTHER_PAYMENTS_REVS_ACCOUNT_LC,
sum(cast(coalesce(UE_REVS_PADS,0) as decimal(20,6))) UE_REVS_PADS,
sum(cast(coalesce(UE_DISPLAY_ADS_LC,0) as decimal(20,6))) UE_DISPLAY_ADS_LC,
sum(cast(coalesce(UE_COST_ME,0) as decimal(20,6))) UE_COST_ME,
sum(cast(coalesce(UE_COST_ME_Flex,0) as decimal(20,6))) UE_COST_ME_Flex,
sum(cast(coalesce(UE_COSTOS_FINANCIEROS_NO_PSJ_ALT,0) as decimal(20,6))) UE_COSTOS_FINANCIEROS_NO_PSJ_ALT,
sum(cast(coalesce(UE_COSTOS_FINANCIEROS_PSJ_ALT,0) as decimal(20,6))) UE_COSTOS_FINANCIEROS_PSJ_ALT,
sum(cast(coalesce(UE_OPERATING_COST_LC_ALT,0) as decimal(20,6))) UE_OPERATING_COST_LC_ALT,
sum(cast(coalesce(UE_SALES_TAXES_ACCOUNT_LC,0) as decimal(20,6))) UE_SALES_TAXES_ACCOUNT_LC,
sum(cast(coalesce(UE_ME_ICMS_CREDITOS,0) as decimal(20,6))) UE_ME_ICMS_CREDITOS,
sum(cast(coalesce(UE_ME_ICMS_DEBITOS,0) as decimal(20,6))) UE_ME_ICMS_DEBITOS,
sum(cast(coalesce(UE_DEBITO_PIS_CONFINS_MKTPLACE,0) as decimal(20,6))) UE_DEBITO_PIS_CONFINS_MKTPLACE,
sum(cast(coalesce(UE_CREDITO_PIS_CONFINS_MKTING,0) as decimal(20,6))) UE_CREDITO_PIS_CONFINS_MKTING,
sum(cast(coalesce(UE_CREDITO_PIS_CONFINS_COLLFEES,0) as decimal(20,6))) UE_CREDITO_PIS_CONFINS_COLLFEES,
sum(cast(coalesce(UE_DEBITO_PIS_CONFINS_ME,0) as decimal(20,6))) UE_DEBITO_PIS_CONFINS_ME,
sum(cast(coalesce(UE_DEBITO_PIS_CONFINS_MP,0) as decimal(20,6))) UE_DEBITO_PIS_CONFINS_MP,
sum(cast(coalesce(UE_DEBITO_PIS_CONFINS_ADS,0) as decimal(20,6))) UE_DEBITO_PIS_CONFINS_ADS,
sum(cast(coalesce(UE_ISS_MKTPLACE,0) as decimal(20,6))) UE_ISS_MKTPLACE,
sum(cast(coalesce(UE_ISS_ME,0) as decimal(20,6))) UE_ISS_ME,
sum(cast(coalesce(UE_ISS_MP,0) as decimal(20,6))) UE_ISS_MP,
sum(cast(coalesce(UE_ISS_ADS,0) as decimal(20,6))) UE_ISS_ADS,
sum(cast(coalesce(UE_CONSOLIDADO_SALES_TAXES,0) as decimal(20,6))) UE_CONSOLIDADO_SALES_TAXES,
sum(cast(coalesce(UE_CE_FP_ACCOUNT_LC,0) as decimal(20,6))) UE_CE_FP_ACCOUNT_LC,
sum(cast(coalesce(UE_OTHER_COGS_ACCOUNT_LC,0) as decimal(20,6))) UE_OTHER_COGS_ACCOUNT_LC,
sum(cast(coalesce(UE_SHIPPING_COSTS_XD_LC,0) as decimal(20,6))) UE_SHIPPING_COSTS_XD_LC,
sum(cast(coalesce(UE_SHIPPING_COSTS_DS_LC,0) as decimal(20,6))) UE_SHIPPING_COSTS_DS_LC,
sum(cast(coalesce(UE_SHIPPING_COSTS_FBM_LC,0) as decimal(20,6))) UE_SHIPPING_COSTS_FBM_LC,
cast(0 as decimal(20,6)) UE_Devoluciones_CX_BPP_LC,
cast(0 as decimal(20,6)) UE_Devoluciones_CX_EX_BPP_LC,
cast(0 as decimal(20,6)) UE_BPP_CASHOUT_LC_ADJ,
sum(cast(coalesce(UE_CHARGEBACKS_LC,0) as decimal(20,6))) UE_CHARGEBACKS_LC,
sum(cast(coalesce(UE_BAD_DEBT_ACCOUNT_LC,0) as decimal(20,6))) UE_BAD_DEBT_ACCOUNT_LC,
sum(cast(coalesce(UE_MARKETING_EXPENSES_ACCOUNT_LC,0) as decimal(20,6))) UE_MARKETING_EXPENSES_ACCOUNT_LC,
sum(cast(coalesce(UE_MKT_EXP_CON_GMVE_LC,0) as decimal(20,6))) UE_MKT_EXP_CON_GMVE_LC,
sum(cast(coalesce(UE_MKT_EXP_SIN_GMVE_LC,0) as decimal(20,6))) UE_MKT_EXP_SIN_GMVE_LC,
sum(cast(coalesce(UE_MKT_EXP_DIF_LC,0) as decimal(20,6))) UE_MKT_EXP_DIF_LC,
sum(cast(coalesce(UE_storage_fees_Lc,0) as decimal(20,6))) UE_storage_fees_Lc,
sum(cast(coalesce(UE_INVERSION_NETA_CTRAL_DESC,0) as decimal(20,6))) UE_INVERSION_NETA_CTRAL_DESC,
sum(cast(coalesce(UE_DIF_LOYALTY_ACCOUNT_LC,0) as decimal(20,6))) UE_DIF_LOYALTY_ACCOUNT_LC,
sum(cast(coalesce(UE_DEV_REVS_DIF_LOYALTY_ACCOUNT_LC,0) as decimal(20,6))) UE_DEV_REVS_DIF_LOYALTY_ACCOUNT_LC,
sum(cast(coalesce(UE_DEPRECIATION_AMORTIZATION_ACCOUNT_LC,0) as decimal(20,6))) UE_DEPRECIATION_AMORTIZATION_ACCOUNT_LC,
sum(cast(coalesce(UE_MONEY_IN_ACCOUNT_LC,0) as decimal(20,6))) UE_MONEY_IN_ACCOUNT_LC,
sum(cast(coalesce(UE_OTHER_LOCAL_EXPENSES_ACCOUNT_LC,0) as decimal(20,6))) UE_OTHER_LOCAL_EXPENSES_ACCOUNT_LC,
sum(cast(coalesce(UE_Product_Development_ACCOUNT_LC,0) as decimal(20,6))) UE_Product_Development_ACCOUNT_LC,
sum(cast(coalesce(UE_BU_Corporate_Exp_ACCOUNT_LC,0) as decimal(20,6))) UE_BU_Corporate_Exp_ACCOUNT_LC,
sum(cast(coalesce(UE_General_Corporate_Allocation_ACCOUNT_LC,0) as decimal(20,6))) UE_General_Corporate_Allocation_ACCOUNT_LC,
sum(cast(coalesce(UE_General_Local_Expenses_ACCOUNT_LC,0) as decimal(20,6))) UE_General_Local_Expenses_ACCOUNT_LC,
sum(cast(coalesce(Core_Marketplace,0) as decimal(20,6))) Core_Marketplace,
sum(cast(coalesce(Flat_Fee,0) as decimal(20,6))) Flat_Fee,
sum(cast(coalesce(Rebates_Discounts,0) as decimal(20,6))) Rebates_Discounts,
sum(cast(coalesce(Payments_Revenues,0) as decimal(20,6))) Payments_Revenues,
sum(cast(coalesce(Financing_Cost,0) as decimal(20,6))) Financing_Cost,
sum(cast(coalesce(Payments_Net,0) as decimal(20,6))) Payments_Net,
sum(cast(coalesce(Shipping_Revenues,0) as decimal(20,6))) Shipping_Revenues,
sum(cast(coalesce(Shipping_Cost,0) as decimal(20,6))) Shipping_Cost,
sum(cast(coalesce(Shipping_Net,0) as decimal(20,6))) Shipping_Net,
sum(cast(coalesce(Ads_Others,0) as decimal(20,6))) Ads_Others,
sum(cast(coalesce(Net_Revenues,0) as decimal(20,6))) Net_Revenues,
sum(cast(coalesce(CX_PF_Hosting,0) as decimal(20,6))) CX_PF_Hosting,
sum(cast(coalesce(Collection_Fees,0) as decimal(20,6))) Collection_Fees,
sum(cast(coalesce(Sales_Taxes,0) as decimal(20,6))) Sales_Taxes,
sum(cast(coalesce(Shipping_Operations_Cost,0) as decimal(20,6))) Shipping_Operations_Cost,
sum(cast(coalesce(Gross_Profit,0) as decimal(20,6))) Gross_Profit,
sum(cast(coalesce(BPP,0) as decimal(20,6))) BPP,
sum(cast(coalesce(Chargebacks_Bad_Debt,0) as decimal(20,6))) Chargebacks_Bad_Debt,
sum(cast(coalesce(Marketing_Expenses,0) as decimal(20,6))) Marketing_Expenses,
sum(cast(coalesce(BU_Local_Expenses,0) as decimal(20,6))) BU_Local_Expenses,
sum(cast(coalesce(Direct_Expenses,0) as decimal(20,6))) Direct_Expenses,
sum(cast(coalesce(ue_dc,0) as decimal(20,6))) ue_dc,
sum(cast(coalesce(ue_dc_sinmarketing,0) as decimal(20,6))) ue_dc_sinmarketing,
sum(cast(coalesce(BU_Corporate_Expenses,0) as decimal(20,6))) BU_Corporate_Expenses,
sum(cast(coalesce(Product_Development,0) as decimal(20,6))) Product_Development,
sum(cast(coalesce(Allocated_Expenses,0) as decimal(20,6))) Allocated_Expenses,
sum(cast(coalesce(PD_Corporate_Expenses,0) as decimal(20,6))) PD_Corporate_Expenses,
sum(cast(coalesce(UE_EBIT,0) as decimal(20,6))) UE_EBIT

FROM WHOWNER.BT_UE_DIARIO_3P
where UE_MONTH_BPP is null
and MONTH_FINAL in (202104,202105,202106,202107)
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12

UNION

SELECT
SIT_SITE_ID,
ORD_ORDER_ID,
UE_VERTICAL,
ITE_SUPERMARKET_FLAG,
UE_FS_TYPE,
--SHP_SHIPMENT_ID,
--CUS_CUST_ID_SEL,
UE_CATEG_L1,
UE_COMBO,
UE_ME_PICKING_TYPE,
UE_SELLER_SEGMENT,
DIA,
UE_MONTH_ID,
UE_MONTH_BPP,
UE_MONTH_BPP AS MONTH_FINAL,
cast(0 as decimal(20,6)) UE_GMVE_BILLABLE_LC,
cast(0 as decimal(20,6)) q_envios,
--cast(0 as decimal(20,6)) UE_FVF_NETO,
--cast(0 as decimal(20,6)) UE_FLAT_FEE,
--cast(0 as decimal(20,6)) UE_FVF_GROSS,
--cast(0 as decimal(20,6)) UE_FVF_BONIF,
cast(0 as decimal(20,6)) UE_REBATE,
cast(0 as decimal(20,6)) UE_FVF_REAL_LC,
cast(0 as decimal(20,6)) UE_FLAT_FEE_LC,
cast(0 as decimal(20,6)) UE_DISCOUNT_LC,
cast(0 as decimal(20,6)) UE_OTHER_REVS_ACCOUNT_LC,
cast(0 as decimal(20,6)) UE_FINANCIAL_FEE_LC,
cast(0 as decimal(20,6)) UE_REVS_ME,
cast(0 as decimal(20,6)) UE_OTHER_PAYMENTS_REVS_ACCOUNT_LC,
cast(0 as decimal(20,6)) UE_REVS_PADS,
cast(0 as decimal(20,6)) UE_DISPLAY_ADS_LC,
cast(0 as decimal(20,6)) UE_COST_ME,
cast(0 as decimal(20,6)) UE_COST_ME_Flex,
cast(0 as decimal(20,6)) UE_COSTOS_FINANCIEROS_NO_PSJ_ALT,
cast(0 as decimal(20,6)) UE_COSTOS_FINANCIEROS_PSJ_ALT,
cast(0 as decimal(20,6)) UE_OPERATING_COST_LC_ALT,
cast(0 as decimal(20,6)) UE_SALES_TAXES_ACCOUNT_LC,
cast(0 as decimal(20,6)) UE_ME_ICMS_CREDITOS,
cast(0 as decimal(20,6)) UE_ME_ICMS_DEBITOS,
cast(0 as decimal(20,6)) UE_DEBITO_PIS_CONFINS_MKTPLACE,
cast(0 as decimal(20,6)) UE_CREDITO_PIS_CONFINS_MKTING,
cast(0 as decimal(20,6)) UE_CREDITO_PIS_CONFINS_COLLFEES,
cast(0 as decimal(20,6)) UE_DEBITO_PIS_CONFINS_ME,
cast(0 as decimal(20,6)) UE_DEBITO_PIS_CONFINS_MP,
cast(0 as decimal(20,6)) UE_DEBITO_PIS_CONFINS_ADS,
cast(0 as decimal(20,6)) UE_ISS_MKTPLACE,
cast(0 as decimal(20,6)) UE_ISS_ME,
cast(0 as decimal(20,6)) UE_ISS_MP,
cast(0 as decimal(20,6)) UE_ISS_ADS,
cast(0 as decimal(20,6)) UE_CONSOLIDADO_SALES_TAXES,
cast(0 as decimal(20,6)) UE_CE_FP_ACCOUNT_LC,
cast(0 as decimal(20,6)) UE_OTHER_COGS_ACCOUNT_LC,
cast(0 as decimal(20,6)) UE_SHIPPING_COSTS_XD_LC,
cast(0 as decimal(20,6)) UE_SHIPPING_COSTS_DS_LC,
cast(0 as decimal(20,6)) UE_SHIPPING_COSTS_FBM_LC,
--sum(cast(coalesce(UE_BPP_CASHOUT_LC,0) as decimal(20,6))) UE_BPP_CASHOUT_LC,
sum(cast(coalesce(UE_Devoluciones_CX_BPP_LC,0) as decimal(20,6))) UE_Devoluciones_CX_BPP_LC,
sum(cast(coalesce(UE_Devoluciones_CX_EX_BPP_LC,0) as decimal(20,6))) UE_Devoluciones_CX_EX_BPP_LC,
--sum(cast(coalesce(UE_BPP_TOTAL_LC_ADJ,0) as decimal(20,6))) UE_BPP_TOTAL_LC_ADJ,
sum(cast(coalesce(UE_BPP_CASHOUT_LC_ADJ,0) as decimal(20,6))) UE_BPP_CASHOUT_LC_ADJ,
cast(0 as decimal(20,6)) UE_CHARGEBACKS_LC,
cast(0 as decimal(20,6)) UE_BAD_DEBT_ACCOUNT_LC,
cast(0 as decimal(20,6)) UE_MARKETING_EXPENSES_ACCOUNT_LC,
cast(0 as decimal(20,6)) UE_MKT_EXP_CON_GMVE_LC,
cast(0 as decimal(20,6)) UE_MKT_EXP_SIN_GMVE_LC,
cast(0 as decimal(20,6)) UE_MKT_EXP_DIF_LC,
cast(0 as decimal(20,6)) UE_storage_fees_Lc,
cast(0 as decimal(20,6)) UE_INVERSION_NETA_CTRAL_DESC,
cast(0 as decimal(20,6)) UE_DIF_LOYALTY_ACCOUNT_LC,
cast(0 as decimal(20,6)) UE_DEV_REVS_DIF_LOYALTY_ACCOUNT_LC,
cast(0 as decimal(20,6)) UE_DEPRECIATION_AMORTIZATION_ACCOUNT_LC,
cast(0 as decimal(20,6)) UE_MONEY_IN_ACCOUNT_LC,
cast(0 as decimal(20,6)) UE_OTHER_LOCAL_EXPENSES_ACCOUNT_LC,
cast(0 as decimal(20,6)) UE_Product_Development_ACCOUNT_LC,
cast(0 as decimal(20,6)) UE_BU_Corporate_Exp_ACCOUNT_LC,
cast(0 as decimal(20,6)) UE_General_Corporate_Allocation_ACCOUNT_LC,
cast(0 as decimal(20,6)) UE_General_Local_Expenses_ACCOUNT_LC,
sum(cast(coalesce(Core_Marketplace,0) as decimal(20,6))) Core_Marketplace,
sum(cast(coalesce(Flat_Fee,0) as decimal(20,6))) Flat_Fee,
sum(cast(coalesce(Rebates_Discounts,0) as decimal(20,6))) Rebates_Discounts,
sum(cast(coalesce(Payments_Revenues,0) as decimal(20,6))) Payments_Revenues,
sum(cast(coalesce(Financing_Cost,0) as decimal(20,6))) Financing_Cost,
sum(cast(coalesce(Payments_Net,0) as decimal(20,6))) Payments_Net,
sum(cast(coalesce(Shipping_Revenues,0) as decimal(20,6))) Shipping_Revenues,
sum(cast(coalesce(Shipping_Cost,0) as decimal(20,6))) Shipping_Cost,
sum(cast(coalesce(Shipping_Net,0) as decimal(20,6))) Shipping_Net,
sum(cast(coalesce(Ads_Others,0) as decimal(20,6))) Ads_Others,
sum(cast(coalesce(Net_Revenues,0) as decimal(20,6))) Net_Revenues,
sum(cast(coalesce(CX_PF_Hosting,0) as decimal(20,6))) CX_PF_Hosting,
sum(cast(coalesce(Collection_Fees,0) as decimal(20,6))) Collection_Fees,
sum(cast(coalesce(Sales_Taxes,0) as decimal(20,6))) Sales_Taxes,
sum(cast(coalesce(Shipping_Operations_Cost,0) as decimal(20,6))) Shipping_Operations_Cost,
sum(cast(coalesce(Gross_Profit,0) as decimal(20,6))) Gross_Profit,
sum(cast(coalesce(BPP,0) as decimal(20,6))) BPP,
sum(cast(coalesce(Chargebacks_Bad_Debt,0) as decimal(20,6))) Chargebacks_Bad_Debt,
sum(cast(coalesce(Marketing_Expenses,0) as decimal(20,6))) Marketing_Expenses,
sum(cast(coalesce(BU_Local_Expenses,0) as decimal(20,6))) BU_Local_Expenses,
sum(cast(coalesce(Direct_Expenses,0) as decimal(20,6))) Direct_Expenses,
sum(cast(coalesce(ue_dc,0) as decimal(20,6))) ue_dc,
sum(cast(coalesce(ue_dc_sinmarketing,0) as decimal(20,6))) ue_dc_sinmarketing,
sum(cast(coalesce(BU_Corporate_Expenses,0) as decimal(20,6))) BU_Corporate_Expenses,
sum(cast(coalesce(Product_Development,0) as decimal(20,6))) Product_Development,
sum(cast(coalesce(Allocated_Expenses,0) as decimal(20,6))) Allocated_Expenses,
sum(cast(coalesce(PD_Corporate_Expenses,0) as decimal(20,6))) PD_Corporate_Expenses,
sum(cast(coalesce(UE_EBIT,0) as decimal(20,6))) UE_EBIT

FROM WHOWNER.BT_UE_DIARIO_3P
where UE_MONTH_BPP is not null
and MONTH_FINAL in (202104,202105,202106,202107)
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
) WITH DATA primary index (ord_order_id)ON COMMIT PRESERVE ROWS;


--drop table input_2;
CREATE MULTISET VOLATILE TABLE input_2 AS (

SELECT
SIT_SITE_ID,
ORD_ORDER_ID,
--SHP_SHIPMENT_ID,
--CUS_CUST_ID_SEL,
UE_VERTICAL,
ITE_SUPERMARKET_FLAG,
UE_FS_TYPE,
UE_CATEG_L1,
UE_COMBO,
UE_ME_PICKING_TYPE,
UE_SELLER_SEGMENT,
DIA,
UE_MONTH_ID,
ue_MONTH_BPP,
UE_MONTH_ID AS MONTH_FINAL,
sum(cast(coalesce(UE_GMVE_BILLABLE_LC,0) as decimal(20,6))) UE_GMVE_BILLABLE_LC,
count(cast(coalesce(q_envios,0) as decimal(20,6))) q_envios,
sum(cast(coalesce(UE_REBATE,0) as decimal(20,6))) UE_REBATE,
sum(cast(coalesce(UE_FVF_REAL_LC,0) as decimal(20,6))) UE_FVF_REAL_LC,
sum(cast(coalesce(UE_FLAT_FEE_LC,0) as decimal(20,6))) UE_FLAT_FEE_LC,
sum(cast(coalesce(UE_DISCOUNT_LC,0) as decimal(20,6))) UE_DISCOUNT_LC,
sum(cast(coalesce(UE_OTHER_REVS_ACCOUNT_LC,0) as decimal(20,6))) UE_OTHER_REVS_ACCOUNT_LC,
sum(cast(coalesce(UE_FINANCIAL_FEE_LC,0) as decimal(20,6))) UE_FINANCIAL_FEE_LC,
sum(cast(coalesce(UE_REVS_ME,0) as decimal(20,6))) UE_REVS_ME,
sum(cast(coalesce(UE_OTHER_PAYMENTS_REVS_ACCOUNT_LC,0) as decimal(20,6))) UE_OTHER_PAYMENTS_REVS_ACCOUNT_LC,
sum(cast(coalesce(UE_REVS_PADS,0) as decimal(20,6))) UE_REVS_PADS,
sum(cast(coalesce(UE_DISPLAY_ADS_LC,0) as decimal(20,6))) UE_DISPLAY_ADS_LC,
sum(cast(coalesce(UE_COST_ME,0) as decimal(20,6))) UE_COST_ME,
sum(cast(coalesce(UE_COST_ME_Flex,0) as decimal(20,6))) UE_COST_ME_Flex,
sum(cast(coalesce(UE_COSTOS_FINANCIEROS_NO_PSJ_ALT,0) as decimal(20,6))) UE_COSTOS_FINANCIEROS_NO_PSJ,
sum(cast(coalesce(UE_COSTOS_FINANCIEROS_PSJ_ALT,0) as decimal(20,6))) UE_COSTOS_FINANCIEROS_PSJ,
sum(cast(coalesce(UE_OPERATING_COST_LC_ALT,0) as decimal(20,6))) UE_OPERATING_COST_LC,
sum(cast(coalesce(UE_SALES_TAXES_ACCOUNT_LC,0) as decimal(20,6))) UE_SALES_TAXES_ACCOUNT_LC,
sum(cast(coalesce(UE_ME_ICMS_CREDITOS,0) as decimal(20,6))) UE_ME_ICMS_CREDITOS,
sum(cast(coalesce(UE_ME_ICMS_DEBITOS,0) as decimal(20,6))) UE_ME_ICMS_DEBITOS,
sum(cast(coalesce(UE_DEBITO_PIS_CONFINS_MKTPLACE,0) as decimal(20,6))) UE_DEBITO_PIS_CONFINS_MKTPLACE,
sum(cast(coalesce(UE_CREDITO_PIS_CONFINS_MKTING,0) as decimal(20,6))) UE_CREDITO_PIS_CONFINS_MKTING,
sum(cast(coalesce(UE_CREDITO_PIS_CONFINS_COLLFEES,0) as decimal(20,6))) UE_CREDITO_PIS_CONFINS_COLLFEES,
sum(cast(coalesce(UE_DEBITO_PIS_CONFINS_ME,0) as decimal(20,6))) UE_DEBITO_PIS_CONFINS_ME,
sum(cast(coalesce(UE_DEBITO_PIS_CONFINS_MP,0) as decimal(20,6))) UE_DEBITO_PIS_CONFINS_MP,
sum(cast(coalesce(UE_DEBITO_PIS_CONFINS_ADS,0) as decimal(20,6))) UE_DEBITO_PIS_CONFINS_ADS,
sum(cast(coalesce(UE_ISS_MKTPLACE,0) as decimal(20,6))) UE_ISS_MKTPLACE,
sum(cast(coalesce(UE_ISS_ME,0) as decimal(20,6))) UE_ISS_ME,
sum(cast(coalesce(UE_ISS_MP,0) as decimal(20,6))) UE_ISS_MP,
sum(cast(coalesce(UE_ISS_ADS,0) as decimal(20,6))) UE_ISS_ADS,
sum(cast(coalesce(UE_CONSOLIDADO_SALES_TAXES,0) as decimal(20,6))) UE_CONSOLIDADO_SALES_TAXES,
sum(cast(coalesce(UE_CE_FP_ACCOUNT_LC,0) as decimal(20,6))) UE_CE_FP_ACCOUNT_LC,
sum(cast(coalesce(UE_OTHER_COGS_ACCOUNT_LC,0) as decimal(20,6))) UE_OTHER_COGS_ACCOUNT_LC,
sum(cast(coalesce(UE_SHIPPING_COSTS_XD_LC,0) as decimal(20,6))) UE_SHIPPING_COSTS_XD_LC,
sum(cast(coalesce(UE_SHIPPING_COSTS_DS_LC,0) as decimal(20,6))) UE_SHIPPING_COSTS_DS_LC,
sum(cast(coalesce(UE_SHIPPING_COSTS_FBM_LC,0) as decimal(20,6))) UE_SHIPPING_COSTS_FBM_LC,
--sum(cast(coalesce(UE_BPP_CASHOUT_LC,0) as decimal(20,6))) UE_BPP_CASHOUT_LC,
sum(cast(coalesce(UE_Devoluciones_CX_BPP_LC,0) as decimal(20,6))) UE_Devoluciones_CX_BPP_LC,
sum(cast(coalesce(UE_Devoluciones_CX_EX_BPP_LC,0) as decimal(20,6))) UE_Devoluciones_CX_EX_BPP_LC,
--sum(cast(coalesce(UE_BPP_TOTAL_LC_ADJ,0) as decimal(20,6))) UE_BPP_TOTAL_LC_ADJ,
sum(cast(coalesce(UE_BPP_CASHOUT_LC_ADJ,0) as decimal(20,6))) UE_BPP_CASHOUT_LC_ADJ,
sum(cast(coalesce(UE_CHARGEBACKS_LC,0) as decimal(20,6))) UE_CHARGEBACKS_LC,
sum(cast(coalesce(UE_BAD_DEBT_ACCOUNT_LC,0) as decimal(20,6))) UE_BAD_DEBT_ACCOUNT_LC,
sum(cast(coalesce(UE_MARKETING_EXPENSES_ACCOUNT_LC,0) as decimal(20,6))) UE_MARKETING_EXPENSES_ACCOUNT_LC,
sum(cast(coalesce(UE_MKT_EXP_CON_GMVE_LC,0) as decimal(20,6))) UE_MKT_EXP_CON_GMVE_LC,
sum(cast(coalesce(UE_MKT_EXP_SIN_GMVE_LC,0) as decimal(20,6))) UE_MKT_EXP_SIN_GMVE_LC,
sum(cast(coalesce(UE_MKT_EXP_DIF_LC,0) as decimal(20,6))) UE_MKT_EXP_DIF_LC,
sum(cast(coalesce(UE_storage_fees_Lc,0) as decimal(20,6))) UE_storage_fees_Lc,
sum(cast(coalesce(UE_INVERSION_NETA_CTRAL_DESC,0) as decimal(20,6))) UE_INVERSION_NETA_CTRAL_DESC,
sum(cast(coalesce(UE_DIF_LOYALTY_ACCOUNT_LC,0) as decimal(20,6))) UE_DIF_LOYALTY_ACCOUNT_LC,
sum(cast(coalesce(UE_DEV_REVS_DIF_LOYALTY_ACCOUNT_LC,0) as decimal(20,6))) UE_DEV_REVS_DIF_LOYALTY_ACCOUNT_LC,
sum(cast(coalesce(UE_DEPRECIATION_AMORTIZATION_ACCOUNT_LC,0) as decimal(20,6))) UE_DEPRECIATION_AMORTIZATION_ACCOUNT_LC,
sum(cast(coalesce(UE_MONEY_IN_ACCOUNT_LC,0) as decimal(20,6))) UE_MONEY_IN_ACCOUNT_LC,
sum(cast(coalesce(UE_OTHER_LOCAL_EXPENSES_ACCOUNT_LC,0) as decimal(20,6))) UE_OTHER_LOCAL_EXPENSES_ACCOUNT_LC,
sum(cast(coalesce(UE_Product_Development_ACCOUNT_LC,0) as decimal(20,6))) UE_Product_Development_ACCOUNT_LC,
sum(cast(coalesce(UE_BU_Corporate_Exp_ACCOUNT_LC,0) as decimal(20,6))) UE_BU_Corporate_Exp_ACCOUNT_LC,
sum(cast(coalesce(UE_General_Corporate_Allocation_ACCOUNT_LC,0) as decimal(20,6))) UE_General_Corporate_Allocation_ACCOUNT_LC,
sum(cast(coalesce(UE_General_Local_Expenses_ACCOUNT_LC,0) as decimal(20,6))) UE_General_Local_Expenses_ACCOUNT_LC,
sum(cast(coalesce(Core_Marketplace,0) as decimal(20,6))) Core_Marketplace,
sum(cast(coalesce(Flat_Fee,0) as decimal(20,6))) Flat_Fee,
sum(cast(coalesce(Rebates_Discounts,0) as decimal(20,6))) Rebates_Discounts,
sum(cast(coalesce(Payments_Revenues,0) as decimal(20,6))) Payments_Revenues,
sum(cast(coalesce(Financing_Cost,0) as decimal(20,6))) Financing_Cost,
sum(cast(coalesce(Payments_Net,0) as decimal(20,6))) Payments_Net,
sum(cast(coalesce(Shipping_Revenues,0) as decimal(20,6))) Shipping_Revenues,
sum(cast(coalesce(Shipping_Cost,0) as decimal(20,6))) Shipping_Cost,
sum(cast(coalesce(Shipping_Net,0) as decimal(20,6))) Shipping_Net,
sum(cast(coalesce(Ads_Others,0) as decimal(20,6))) Ads_Others,
sum(cast(coalesce(Net_Revenues,0) as decimal(20,6))) Net_Revenues,
sum(cast(coalesce(CX_PF_Hosting,0) as decimal(20,6))) CX_PF_Hosting,
sum(cast(coalesce(Collection_Fees,0) as decimal(20,6))) Collection_Fees,
sum(cast(coalesce(Sales_Taxes,0) as decimal(20,6))) Sales_Taxes,
sum(cast(coalesce(Shipping_Operations_Cost,0) as decimal(20,6))) Shipping_Operations_Cost,
sum(cast(coalesce(Gross_Profit,0) as decimal(20,6))) Gross_Profit,
sum(cast(coalesce(BPP,0) as decimal(20,6))) BPP,
sum(cast(coalesce(Chargebacks_Bad_Debt,0) as decimal(20,6))) Chargebacks_Bad_Debt,
sum(cast(coalesce(Marketing_Expenses,0) as decimal(20,6))) Marketing_Expenses,
sum(cast(coalesce(BU_Local_Expenses,0) as decimal(20,6))) BU_Local_Expenses,
sum(cast(coalesce(Direct_Expenses,0) as decimal(20,6))) Direct_Expenses,
sum(cast(coalesce(ue_dc,0) as decimal(20,6))) ue_dc,
sum(cast(coalesce(ue_dc_sinmarketing,0) as decimal(20,6))) ue_dc_sinmarketing,
sum(cast(coalesce(BU_Corporate_Expenses,0) as decimal(20,6))) BU_Corporate_Expenses,
sum(cast(coalesce(Product_Development,0) as decimal(20,6))) Product_Development,
sum(cast(coalesce(Allocated_Expenses,0) as decimal(20,6))) Allocated_Expenses,
sum(cast(coalesce(PD_Corporate_Expenses,0) as decimal(20,6))) PD_Corporate_Expenses,
sum(cast(coalesce(UE_EBIT,0) as decimal(20,6))) UE_EBIT

FROM input_1

GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
) WITH DATA primary index (ord_order_id)ON COMMIT PRESERVE ROWS;


select
a.SIT_SITE_ID,
--ORD_ORDER_ID,
--SHP_SHIPMENT_ID,
--CUS_CUST_ID_SEL,
--(CASE WHEN COALESCE(b.BID_QUANTITY_OK, 0) > 0 THEN
--CASE  WHEN b.BID_SITE_CURRENT_PRICE < 299 THEN '<2500'
--    WHEN b.BID_SITE_CURRENT_PRICE <499 THEN '<3500'
--    WHEN b.BID_SITE_CURRENT_PRICE <599 THEN '<4000'
--else '>= 599' end END )as ASP_Range,
--CASE  WHEN a.DIA >='2020-12-09' then '2020-12-09'
--else '<2020-12-09' end as RANGO_FECHA,--
--case when x.ord_order_id is not null then 'B2B' else 'others' end as GRUPO_B2B,
a.UE_VERTICAL,
--a.UE_CATEG_L1,
x.CUST_TYPE,
x.tax_payer,

a.UE_FS_TYPE,
a.UE_COMBO,
a.UE_ME_PICKING_TYPE,
a.UE_SELLER_SEGMENT,
--a.DIA,
a.UE_MONTH_ID,
a.ue_MONTH_BPP,
a.UE_MONTH_ID AS MONTH_FINAL,
sum(cast(coalesce(a.UE_GMVE_BILLABLE_LC,0) as decimal(20,6))) UE_GMVE_BILLABLE_LC,
count(distinct a.q_envios) q_envios,
sum(cast(coalesce(a.Core_Marketplace,0) as decimal(20,6))) Core_Marketplace,
sum(cast(coalesce(a.Flat_Fee,0) as decimal(20,6))) Flat_Fee,
sum(cast(coalesce(a.Rebates_Discounts,0) as decimal(20,6))) Rebates_Discounts,
sum(cast(coalesce(a.Payments_Revenues,0) as decimal(20,6))) Payments_Revenues,
sum(cast(coalesce(a.Financing_Cost,0) as decimal(20,6))) Financing_Cost,
sum(cast(coalesce(a.Payments_Net,0) as decimal(20,6))) Payments_Net,
sum(cast(coalesce(a.Shipping_Revenues,0) as decimal(20,6))) Shipping_Revenues,
sum(cast(coalesce(a.Shipping_Cost,0) as decimal(20,6))) Shipping_Cost,
sum(cast(coalesce(a.Shipping_Net,0) as decimal(20,6))) Shipping_Net,
sum(cast(coalesce(a.Ads_Others,0) as decimal(20,6))) Ads_Others,
sum(cast(coalesce(a.Net_Revenues,0) as decimal(20,6))) Net_Revenues,
sum(cast(coalesce(a.CX_PF_Hosting,0) as decimal(20,6))) CX_PF_Hosting,
sum(cast(coalesce(a.Collection_Fees,0) as decimal(20,6))) Collection_Fees,
sum(cast(coalesce(a.Sales_Taxes,0) as decimal(20,6))) Sales_Taxes,
sum(cast(coalesce(a.Shipping_Operations_Cost,0) as decimal(20,6))) Shipping_Operations_Cost,
sum(cast(coalesce(a.Gross_Profit,0) as decimal(20,6))) Gross_Profit,
sum(cast(coalesce(a.BPP,0) as decimal(20,6))) BPP,
sum(cast(coalesce(a.Chargebacks_Bad_Debt,0) as decimal(20,6))) Chargebacks_Bad_Debt,
sum(cast(coalesce(a.Marketing_Expenses,0) as decimal(20,6))) Marketing_Expenses,
sum(cast(coalesce(a.UE_MKT_EXP_CON_GMVE_LC,0) as decimal(20,6))) UE_MKT_EXP_CON_GMVE_LC,
sum(cast(coalesce(a.UE_MKT_EXP_SIN_GMVE_LC,0) as decimal(20,6))) UE_MKT_EXP_SIN_GMVE_LC,
sum(cast(coalesce(a.UE_MKT_EXP_DIF_LC,0) as decimal(20,6))) UE_MKT_EXP_DIF_LC,
sum(cast(coalesce(a.BU_Local_Expenses,0) as decimal(20,6))) BU_Local_Expenses,
sum(cast(coalesce(a.Direct_Expenses,0) as decimal(20,6))) Direct_Expenses,
sum(cast(coalesce(a.ue_dc,0) as decimal(20,6))) ue_dc,
sum(cast(coalesce(a.ue_dc_sinmarketing,0) as decimal(20,6))) ue_dc_sinmarketing,
sum(cast(coalesce(a.BU_Corporate_Expenses,0) as decimal(20,6))) BU_Corporate_Expenses,
sum(cast(coalesce(a.Product_Development,0) as decimal(20,6))) Product_Development,
sum(cast(coalesce(a.Allocated_Expenses,0) as decimal(20,6))) Allocated_Expenses,
sum(cast(coalesce(a.PD_Corporate_Expenses,0) as decimal(20,6))) PD_Corporate_Expenses,
sum(cast(coalesce(a.UE_EBIT,0) as decimal(20,6))) UE_EBIT

from input_2 a
left join WHOWNER.BT_BIDS b on a.ord_order_id=b.ord_order_id
left join BASE_CUST x on a.ord_order_id=x.ord_order_id
--left join BASE_FINAL_ORDEN x on a.ord_order_id=x.ord_order_id
where b.PHOTO_ID = 'TODATE'
and b.ITE_GMV_FLAG = 1
and b.MKT_MARKETPLACE_ID = 'TM'
And a.SIT_SITE_ID IN (${vars})
AND b.tim_day_winning_date between date ${start_date} and date ${end_date} 
and b.CUS_PARTY_TYPE_ID = '3P'
--b.cus_cust_id_sel not in (select distinct cus_cust_id_sel from LK_CUS_MKPL_SPECIAL)

group by 1,2,3,4,5,6,7,8,9,10,11