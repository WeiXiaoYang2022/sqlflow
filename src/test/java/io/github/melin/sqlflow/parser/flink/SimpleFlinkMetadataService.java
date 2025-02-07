package io.github.melin.sqlflow.parser.flink;

import io.github.melin.sqlflow.metadata.MetadataService;
import io.github.melin.sqlflow.metadata.QualifiedObjectName;
import io.github.melin.sqlflow.metadata.SchemaTable;
import io.github.melin.sqlflow.metadata.ViewDefinition;
import io.github.melin.sqlflow.tree.QualifiedName;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Optional;

/**
 * huaixin 2021/12/25 6:13 PM
 */
public class SimpleFlinkMetadataService implements MetadataService {
    @Override
    public Optional<String> getSchema() {
        return Optional.of("default");
    }

    @Override
    public Optional<String> getCatalog() {
        return Optional.empty();
    }

    @Override
    public boolean isAggregationFunction(QualifiedName name) {
        return false;
    }

    @Override
    public Optional<SchemaTable> getTableSchema(QualifiedObjectName table) {
        if (table.getObjectName().equalsIgnoreCase("retek_xx_item_attr_translate_product_enrichment")) {
            List<String> columns = Lists.newArrayList();
            columns.add("ITEM");
            columns.add("ENRICHMENT_ID");
            columns.add("UDA_VALUE_ID");
            columns.add("LANG");
            columns.add("TRANSLATED_VALUE");
            columns.add("LAST_UPDATE_ID");
            columns.add("CREATE_DATETIME");
            columns.add("LAST_UPDATE_DATETIME");
            columns.add("KAFKA_PROCESS_TIME");

            return Optional.of(new SchemaTable("retek_xx_item_attr_translate_product_enrichment", columns));
        } else if (table.getObjectName().equalsIgnoreCase("MDM_DIM_LANG_LOOKUPMAP_ORACLE")) {
            List<String> columns = Lists.newArrayList();
            columns.add("LANG");
            columns.add("ISO_CODE");

            return Optional.of(new SchemaTable("MDM_DIM_LANG_LOOKUPMAP_ORACLE", columns));
        } else if (table.getObjectName().equalsIgnoreCase("mdm_dim_uda_item_ff_lookupmap_oracle")) {
            List<String> columns = Lists.newArrayList();
            columns.add("ITEM");
            columns.add("UDA_ID");
            columns.add("UDA_TEXT");
            columns.add("LAST_UPDATE_ID");
            columns.add("CREATE_DATETIME");
            columns.add("LAST_UPDATE_DATETIME");

            return Optional.of(new SchemaTable("mdm_dim_uda_item_ff_lookupmap_oracle", columns));
        } else if (table.getObjectName().equalsIgnoreCase("mdm_dim_product_attrib_type_lookupmap_mysql")) {
            List<String> columns = Lists.newArrayList();
            columns.add("BU_CODE");
            columns.add("ATTRIB_ID");
            columns.add("ATTRIB_TYPE");
            columns.add("CONTROL_TYPE");
            return Optional.of(new SchemaTable("mdm_dim_product_attrib_type_lookupmap_mysql", columns));
        } else if (table.getObjectName().equalsIgnoreCase("retek_uda_item_ff_product_enrichment_dim")) {
            List<String> columns = Lists.newArrayList();
            columns.add("ITEM");
            columns.add("UDA_ID");
            columns.add("UDA_TEXT");
            columns.add("LAST_UPDATE_ID");
            columns.add("CREATE_DATETIME");
            columns.add("LAST_UPDATE_DATETIME");
            columns.add("KAFKA_PROCESS_TIME");
            return Optional.of(new SchemaTable("retek_uda_item_ff_product_enrichment_dim", columns));
        } else if (table.getObjectName().equalsIgnoreCase("mdm_dim_xx_item_attr_translate_lookupmap_oracle_dim")) {
            List<String> columns = Lists.newArrayList();
            columns.add("ITEM");
            columns.add("UDA_ID");
            columns.add("UDA_VALUE_ID");
            columns.add("LANG");
            columns.add("TRANSLATED_VALUE");
            columns.add("LAST_UPDATE_ID");
            columns.add("CREATE_DATETIME");
            columns.add("LAST_UPDATE_DATETIME");
            return Optional.of(new SchemaTable("mdm_dim_xx_item_attr_translate_lookupmap_oracle_dim", columns));
        } else if (table.getObjectName().equalsIgnoreCase("processed_mdm_product_enrichment")) {
            List<String> columns = Lists.newArrayList();
            columns.add("PROD_ID");
            columns.add("ENRICHMENT_ID");
            columns.add("LANG");
            columns.add("ENRICHMENT_VALUE");
            columns.add("LAST_UPDATED");
            return Optional.of(new SchemaTable("processed_mdm_product_enrichment", columns));
        } else if (table.getObjectName().equalsIgnoreCase("dws_eltct_pro_w")) {  // c
            List<String> columns = Lists.newArrayList();
            columns.add("year");
            columns.add("serial_num_w");
            columns.add("week_start");
            columns.add("week_end");
            columns.add("biz_date");
            columns.add("prov");
            columns.add("prov_spare");
            columns.add("region_cmpny");
            columns.add("region_cmpny_spare");
            columns.add("belong_province_area");
            columns.add("put_state");
            columns.add("adm_power_plant");
            columns.add("std_power_sttn");
            columns.add("pro_id");
            columns.add("fill_pro_name");
            columns.add("revert_pro_name");
            columns.add("secndry_unit_name");
            columns.add("pro_type");
            columns.add("data_store_time");
            columns.add("serial_num");
            columns.add("pro_cmpny");
            columns.add("regional_company_org_id");
            columns.add("province_org_id");
            columns.add("administrative_power_plant_org_id");
            columns.add("standard_power_plant_org_id");
            columns.add("load_capacity");
            columns.add("anlyzbl_on_grid_eltct");
            columns.add("plan_on_grid_eltct_m");
            columns.add("plan_on_grid_eltct_y");
            columns.add("res");
            columns.add("eltct_prdt");
            columns.add("on_grid_eltct");
            columns.add("acmltd_eltct_prdt_y");
            columns.add("acmltd_on_grid_eltct_m");
            columns.add("acmltd_on_grid_eltct_y");
            columns.add("acmltd_eqvl_utlzt_hours_y");
            columns.add("plan_on_grid_eltct_ratio_y");
            columns.add("fault_loss_eltct");
            columns.add("brownts_loss_eltct");
            columns.add("plan_repair_loss_eltct");
            columns.add("offsite_loss_eltct");
            columns.add("other_loss_eltct");
            columns.add("sum_loss_eltct");
            return Optional.of(new SchemaTable("dws_eltct_pro_w", columns));
        } else if (table.getObjectName().equalsIgnoreCase("dwd_pub_fill_w")) {  // c
            List<String> columns = Lists.newArrayList();
            columns.add("id");
            columns.add("system");
            columns.add("data_store_time");
            columns.add("serial_num");
            columns.add("revert_pro_name");
            columns.add("prov");
            columns.add("prov_spare");
            columns.add("region_cmpny_spare");
            columns.add("belong_province_area");
            columns.add("put_state");
            columns.add("secndry_unit_name");
            columns.add("anlyzbl_on_grid_eltct");
            columns.add("pro_id");
            columns.add("region_cmpny");
            columns.add("adm_power_plant");
            columns.add("std_power_sttn");
            columns.add("fill_pro_name");
            columns.add("report_type");
            columns.add("pro_type");
            columns.add("pro_cmpny");
            columns.add("fill_date");
            columns.add("regional_company_org_id");
            columns.add("province_org_id");
            columns.add("administrative_power_plant_org_id");
            columns.add("standard_power_plant_org_id");
            columns.add("start_time");
            columns.add("end_time");
            columns.add("eltct_desc");
            columns.add("eltct_ques");
            columns.add("ticket_work_desc");
            columns.add("load_amount");
            columns.add("forml_incrp_amount");
            columns.add("eltct_prdt_amount");
            columns.add("stand_amount");
            columns.add("brownts_amount");
            columns.add("failure_amount");
            columns.add("repair_amount");
            columns.add("off_amount");
            columns.add("fault_24h");
            columns.add("fault_over_24h");
            columns.add("brownts_24h");
            columns.add("brownts_over_24h");
            columns.add("repair_24h");
            columns.add("repair_over_24h");
            columns.add("off_24h");
            columns.add("off_over_24h");
            return Optional.of(new SchemaTable("dwd_pub_fill_w", columns));
        } else if (table.getObjectName().equalsIgnoreCase("ads_trans_elecric_w")) {
            List<String> columns = Lists.newArrayList();
            columns.add("prov");
            columns.add("prov_spare");
            columns.add("region_cmpny");
            columns.add("region_cmpny_spare");
            columns.add("belong_province_area");
            columns.add("put_state");
            columns.add("std_power_sttn");
            columns.add("fill_pro_name");
            columns.add("revert_pro_name");
            columns.add("secndry_unit_name");
            columns.add("pro_type");
            columns.add("year_month");
            columns.add("serial_num_w");
            columns.add("week_start_end");
            columns.add("week_end");
            columns.add("data_store_time");
            columns.add("serial_num");
            columns.add("regional_company_org_id");
            columns.add("province_org_id");
            columns.add("administrative_power_plant_org_id");
            columns.add("standard_power_plant_org_id");
            columns.add("load_capacity");
            columns.add("anlyzbl_on_grid_eltct");
            columns.add("plan_on_grid_eltct_m");
            columns.add("plan_on_grid_eltct_y");
            columns.add("res");
            columns.add("eltct_prdt");
            columns.add("on_grid_eltct");
            columns.add("acmltd_eltct_prdt_y");
            columns.add("acmltd_on_grid_eltct_m");
            columns.add("acmltd_on_grid_eltct_y");
            columns.add("acmltd_eqvl_utlzt_hours_y");
            columns.add("plan_on_grid_eltct_ratio_y");
            columns.add("fault_loss_eltct");
            columns.add("brownts_loss_eltct");
            columns.add("plan_repair_loss_eltct");
            columns.add("offsite_loss_eltct");
            columns.add("other_loss_eltct");
            columns.add("sum_loss_eltct");
            columns.add("prd_milstn");
            return Optional.of(new SchemaTable("ads_trans_elecric_w", columns));
        }

        return Optional.empty();
    }

    @Override
    public Optional<ViewDefinition> getView(QualifiedObjectName viewName) {
        return Optional.empty();
    }
}
