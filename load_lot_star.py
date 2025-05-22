'''Function loading the lot_star table
'''

import pyspark.sql.functions as f
import utils
from pyspark.sql import Window
from get_src_data import get_transfix as tvb
from load_lot import expr_lot as expr


def get_lot_star(logging, spark_session, target_db_name, staging_location, debug_mode_ind, debug_postfix):

    logging.info("Get child shipment.")

    lot_child_shpmt_df = \
        tvb.get_on_time_data_hub_star(logging, spark_session, target_db_name, target_db_name, staging_location,
                                      debug_mode_ind, debug_postfix)\
        .select("child_shpmt_num").withColumn("load_child_id", f.regexp_replace("child_shpmt_num", '^0', ''))\
        .drop("child_shpmt_num")

    logging.info("Get child shipment completed.")

    lot_otd_dh_df = \
        tvb.get_on_time_data_hub_star(logging, spark_session, target_db_name, target_db_name, staging_location,
                                      debug_mode_ind, debug_postfix)\
        .drop("pg_order_num").drop("cust_po_num").drop("request_dlvry_from_date")\
        .drop("request_dlvry_from_datetm").drop("request_dlvry_to_date").drop("request_dlvry_to_datetm")\
        .drop("orig_request_dlvry_from_tmstp").drop("orig_request_dlvry_to_tmstp").drop("actual_arrival_datetm")\
        .drop("load_method_num").drop("transit_mode_name").drop("order_create_date").drop("order_create_datetm")\
        .drop("schedule_date").drop("schedule_datetm").drop("tender_date").drop("tender_datetm")\
        .drop("first_dlvry_appt_date").drop("first_dlvry_appt_datetm").drop("last_dlvry_appt_date")\
        .drop("last_dlvry_appt_datetm").drop("actual_ship_datetm").drop("csot_failure_reason_bucket_name")\
        .drop("csot_failure_reason_bucket_updated_name").drop("measrbl_flag").drop("profl_method_code")\
        .drop("dest_city_name").drop("dest_state_code").drop("dest_postal_code").drop("cause_code")\
        .drop("on_time_src_code").drop("on_time_src_desc").drop("sales_org_code")\
        .drop("sd_doc_item_overall_process_status_val").drop("multi_stop_num")\
        .drop("lot_exception_categ_val").drop("primary_carr_flag").drop("actual_shpmt_end_date")\
        .drop("trnsp_stage_num").drop("actual_dlvry_tmstp").drop("first_appt_dlvry_tmstp")\
        .filter('trnsp_stage_num = "1"')\
        .filter('LENGTH(COALESCE(actual_load_end_date, "")) > 1')\
        .filter('LENGTH(COALESCE(final_lrdt_date, "")) > 1')\
        .filter('LENGTH(COALESCE(lot_delay_reason_code_desc, "")) > 1')\
        .filter('change_type_code in ("PICKCI", "PICKCO")') \
        .filter('LENGTH(COALESCE(event_datetm, "")) > 1') \
        .filter('event_datetm <> "99991231235959"') \

    lot_otd_dh_filter_df = \
        lot_otd_dh_df.join(lot_child_shpmt_df, lot_otd_dh_df.load_id == lot_child_shpmt_df.load_child_id, how='left') \
        .filter('load_child_id is null')

    logging.info("Calculating new columns for on time data hub.")

    lot_cols_otd_dh_df = lot_otd_dh_filter_df.withColumnRenamed("dest_zone_code", "dest_zone_val")\
        .withColumnRenamed("frt_type_desc", "freight_type_val") \
        .withColumn("ship_month", f.expr(expr.ship_month_expr))\
        .withColumn("ship_year", f.expr(expr.ship_year_expr)) \
        .withColumn("ship_3lettermonth", f.expr(expr.ship_3letter_expr)) \
        .withColumn("lot_otd_cnt", f.expr(expr.lot_otd_count_expr)) \
        .withColumn("lot_tat_late_counter_val", f.expr(expr.lot_tat_late_counter_expr)) \
        .withColumn("country_to_desc", f.expr(expr.cntry_to_desc_expr)) \
        .withColumn("actual_ship_month_val", f.concat(f.col('ship_3lettermonth'), f.lit(' '), f.col('ship_year')))

    utils.manageOutput(logging, spark_session, lot_cols_otd_dh_df, 1, "lot_cols_otd_dh_df", target_db_name,
                       staging_location, debug_mode_ind, "_{}{}".format(target_db_name, debug_postfix))

    logging.info("Calculating new columns for on time data hub has finished.")

    logging.info("Group by load_id to calculate no of loads.")

    lot_cnt_otd_dh_df = lot_cols_otd_dh_df.select("load_id").groupBy("load_id")\
        .agg(f.countDistinct("load_id").alias("load_num_per_load_id_cnt"))

    logging.info("Group by load_id to calculate no of loads has finished.")

    logging.info("Get Max values for load_id.")

    lot_final_otd_dh_df = lot_cols_otd_dh_df.join(lot_cnt_otd_dh_df, "load_id", how='left')\
        .withColumn("rn",
                    f.row_number().over(
                        Window.partitionBy("load_id", "lot_delay_reason_code", "change_type_code")
                        .orderBy(f.col("event_datetm").desc()))) \
        .filter("rn = 1")\
        .withColumn("rn2",
                    f.row_number().over(
                        Window.partitionBy("load_id", "change_type_code").orderBy(f.col("event_datetm").desc()))) \
        .filter("rn2 = 1")\
        .withColumn("last_update_utc_tmstp", f.to_utc_timestamp(f.from_unixtime(f.unix_timestamp()), 'PRT'))

    utils.manageOutput(logging, spark_session, lot_final_otd_dh_df, 0, "lot_final_otd_dh_df", target_db_name,
                       staging_location, debug_mode_ind, "_{}{}".format(target_db_name, debug_postfix))

    logging.info("Get Max values for load_id has finished.")

    return lot_final_otd_dh_df


def load_lot_star(logging, config_module, debug_mode_ind, debug_postfix):
    ''' Load the lot_star table'''

    # Create a spark session
    params = utils.ConfParams.build_from_module(logging, config_module, debug_mode_ind, debug_postfix)
    #spark_params = utils.ConfParams.build_from_module(logging, config_spark_module, debug_mode_ind, debug_postfix)
    spark_conf = list(set().union(
        params.SPARK_GLOBAL_PARAMS,
        params.SPARK_TFX_PARAMS)
        )
    spark_session = utils.get_spark_session(logging, 'tfx_lot', params.SPARK_MASTER, spark_conf)

    #Remove debug tables (if they are)
    utils.removeDebugTables(logging, spark_session, params.TARGET_DB_NAME, debug_mode_ind, debug_postfix)

    logging.info("Started loading {}.lot_star table.".format(params.TARGET_DB_NAME))

    # Get target table column list
    target_table_cols = spark_session.table('{}.lot_star'.format(params.TARGET_DB_NAME)).schema.fieldNames()

    lot_star_df = get_lot_star(logging, spark_session, params.TARGET_DB_NAME, params.STAGING_LOCATION,
                               debug_mode_ind, debug_postfix).select(target_table_cols)

    logging.info("Inserting data into a table (overwriting old data)")

    lot_star_df.write.insertInto(
        tableName='{}.{}'.format(params.TARGET_DB_NAME, 'lot_star'),
        overwrite=True)
    logging.info("Loading {}.lot_star table has finished".format(params.TARGET_DB_NAME))
