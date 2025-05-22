'''Function loading the carrier dashboard tables
'''

import pyspark.sql.functions as f
from pyspark.sql import Window
import utils
from get_src_data import get_transfix as tvb


def get_tac_lane_detail_star(
        logging, spark_session, trans_vsblt_db_name, target_db_name, staging_location, debug_mode_ind, debug_postfix):

    tac_tender_pg_summary_df = \
        tvb.get_tac_tender_pg_summary_new_star(logging, spark_session, target_db_name, target_db_name, staging_location,
                                      debug_mode_ind, debug_postfix).drop("actual_carr_trans_cost_amt")\
        .drop("linehaul_cost_amt").drop("incrmtl_freight_auction_cost_amt").drop("cnc_carr_mix_cost_amt")\
        .drop("unsource_cost_amt").drop("fuel_cost_amt").drop("acsrl_cost_amt").drop("forward_agent_id")\
        .drop("service_tms_code").drop("sold_to_party_id").drop("ship_cond_val").drop("primary_carr_flag")\
        .drop("month_type_val").drop("cal_year_num").drop("month_date").drop("week_num").drop("dest_postal_code") \
        .withColumnRenamed("region_code", "state_province_code").distinct()

    #shipping_location_na_dim_df = \
    #    tvb.get_shipping_location_na_dim(logging, spark_session, trans_vsblt_db_name, target_db_name, staging_location,
    #                                     debug_mode_ind, debug_postfix).drop("origin_zone_ship_from_code").distinct()

    logging.info("Calculating columns in TAC tender pg summary data.")

    tac_tender_pg_summary_calc_df = \
        tac_tender_pg_summary_df.withColumn("calendar_week_num", f.weekofyear("week_begin_date"))\
        .withColumn("calendar_year_num", f.year("week_begin_date")) \
        .withColumn("str_calendar_week_num", f.lpad("calendar_week_num", 2, '0')) \
        .withColumn("concat_week_year", f.concat(f.col('calendar_year_num'), f.col('str_calendar_week_num')))\
        .withColumn("drank_week_year", f.dense_rank().over(Window.orderBy(f.col("concat_week_year").desc())))\
        .withColumn("last_update_utc_tmstp", f.to_utc_timestamp(f.from_unixtime(f.unix_timestamp()), 'PRT'))

    utils.manageOutput(logging, spark_session, tac_tender_pg_summary_calc_df, 0, "tac_tender_pg_summary_calc_df",
                       target_db_name, staging_location, debug_mode_ind, "_{}{}".format(target_db_name, debug_postfix))
    logging.info("Calculating columns in TAC tender pg summary data has finished.")

    #logging.info("Joining tables for final data.")
    #
    #carrier_taclane_join_df = \
    #    tac_tender_pg_summary_calc_df.join(shipping_location_na_dim_df,
    #                                       (tac_tender_pg_summary_calc_df.customer_desc ==
    #                                        shipping_location_na_dim_df.loc_name), how='left')
    #
    #utils.manageOutput(logging, spark_session, carrier_taclane_join_df, 0, "carrier_taclane_join_df", target_db_name,
    #                   staging_location, debug_mode_ind, "_{}{}".format(target_db_name, debug_postfix))
    #logging.info("Joining tables for final data has finished.")

    return tac_tender_pg_summary_calc_df.filter('drank_week_year < 14')


def get_tac_shpmt_detail_star(
        logging, spark_session, trans_vsblt_db_name, target_db_name, staging_location, debug_mode_ind, debug_postfix):

    tac_df = \
        tvb.get_tac(logging, spark_session, target_db_name, target_db_name, staging_location, debug_mode_ind,
                    debug_postfix) \
        .withColumn("load_id", f.regexp_replace("load_id", '^0', '')).drop("actual_carr_trans_cost_amt")\
        .drop("linehaul_cost_amt").drop("incrmtl_freight_auction_cost_amt").drop("cnc_carr_mix_cost_amt")\
        .drop("unsource_cost_amt").drop("fuel_cost_amt").drop("acsrl_cost_amt").drop("applnc_subsector_step_cnt")\
        .drop("baby_care_subsector_step_cnt").drop("chemical_subsector_step_cnt").drop("fabric_subsector_step_cnt")\
        .drop("family_subsector_step_cnt").drop("fem_subsector_step_cnt").drop("hair_subsector_step_cnt")\
        .drop("home_subsector_step_cnt").drop("oral_subsector_step_cnt").drop("phc_subsector_step_cnt")\
        .drop("shave_subsector_step_cnt").drop("skin_subsector_cnt").drop("other_subsector_cnt")\
        .drop("customer_lvl1_code").drop("customer_lvl1_desc").drop("customer_lvl2_code").drop("customer_lvl2_desc")\
        .drop("customer_lvl3_code").drop("customer_lvl3_desc").drop("customer_lvl4_code").drop("customer_lvl4_desc")\
        .drop("customer_lvl5_code").drop("customer_lvl5_desc").drop("customer_lvl6_code").drop("customer_lvl6_desc")\
        .drop("customer_lvl7_code").drop("customer_lvl7_desc").drop("customer_lvl8_code").drop("customer_lvl8_desc")\
        .drop("customer_lvl9_code").drop("customer_lvl9_desc").drop("customer_lvl10_code").drop("customer_lvl10_desc")\
        .drop("customer_lvl11_code").drop("customer_lvl11_desc").drop("customer_lvl12_code")\
        .drop("customer_lvl12_desc").drop("origin_zone_code").drop("daily_award_qty").distinct()

    tac_lane_detail_star_df = \
        tvb.get_tac_lane_detail_star(logging, spark_session, target_db_name, target_db_name, staging_location,
                                     debug_mode_ind, debug_postfix).distinct()

    shipping_location_na_dim_df = \
        tvb.get_shipping_location_na_dim(logging, spark_session, trans_vsblt_db_name, target_db_name, staging_location,
                                         debug_mode_ind, debug_postfix).drop("origin_zone_ship_from_code")\
        .drop("loc_id").drop("loc_name").withColumnRenamed("postal_code", "final_stop_postal_code").distinct()

    logging.info("Calculating columns in TAC data.")

    cd_shpmt_tac_calc_df = \
        tac_df.withColumn("calendar_week_num", f.weekofyear("actual_goods_issue_date"))\
        .withColumn("calendar_year_num", f.year("actual_goods_issue_date")) \
        .withColumn("str_calendar_week_num", f.lpad("calendar_week_num", 2, '0')) \
        .withColumn("concat_week_year", f.concat(f.col('calendar_year_num'), f.col('str_calendar_week_num')))\
        .drop("calendar_week_num").drop("calendar_year_num")\
        .drop("str_calendar_week_num")

    logging.info("Calculating columns in TAC data has finished.")

    logging.info("Calculating columns in TAC Lane Detail data.")

    cd_shpmt_tac_lane_detail_star_calc_df = \
        tac_lane_detail_star_df.withColumn("calendar_week_num", f.weekofyear("week_begin_date"))\
        .withColumn("calendar_year_num", f.year("week_begin_date")) \
        .withColumn("str_calendar_week_num", f.lpad("calendar_week_num", 2, '0')) \
        .withColumn("concat_week_year", f.concat(f.col('calendar_year_num'), f.col('str_calendar_week_num')))\
        .drop("calendar_week_num").drop("calendar_year_num")\
        .drop("str_calendar_week_num")

    logging.info("Calculating columns in TAC Lane Detail data has finished.")

    logging.info("Joining tables for final data.")

    cd_shpmt_join_df = \
        cd_shpmt_tac_calc_df.join(cd_shpmt_tac_lane_detail_star_calc_df, "concat_week_year", how='inner')\
        .join(shipping_location_na_dim_df, "final_stop_postal_code", how='left')\
        .withColumn("last_update_utc_tmstp", f.to_utc_timestamp(f.from_unixtime(f.unix_timestamp()), 'PRT'))

    utils.manageOutput(logging, spark_session, cd_shpmt_join_df, 0, "cd_shpmt_join_df", target_db_name,
                       staging_location, debug_mode_ind, "_{}{}".format(target_db_name, debug_postfix))

    logging.info("Joining tables for final data has finished.")

    return cd_shpmt_join_df


def load_carrier_dashboard(logging, config_module, debug_mode_ind, debug_postfix):
    ''' Load the carrier dashboard tables'''

    params = utils.ConfParams.build_from_module(logging, config_module, debug_mode_ind, debug_postfix)
    #spark_params = utils.ConfParams.build_from_module(logging, config_spark_module, debug_mode_ind, debug_postfix)
    spark_conf = list(set().union(
        params.SPARK_GLOBAL_PARAMS,
        params.SPARK_TFX_PARAMS)
        )
    spark_session = utils.get_spark_session(logging, 'tfx_carrier', params.SPARK_MASTER, spark_conf)

    #Remove debug tables (if they are)
    utils.removeDebugTables(logging, spark_session, params.TARGET_DB_NAME, debug_mode_ind, debug_postfix)

    logging.info("Started loading {}.tac_lane_detail_star table.".format(params.TARGET_DB_NAME))

    # Get target table column list
    target_table_cols = spark_session.table('{}.tac_lane_detail_star'.format(params.TARGET_DB_NAME)).schema.fieldNames()

    tac_lane_detail_star_df = \
        get_tac_lane_detail_star(logging, spark_session, params.TRANS_VB_DB_NAME, params.TARGET_DB_NAME,
                                 params.STAGING_LOCATION, debug_mode_ind, debug_postfix).select(target_table_cols)

    logging.info("Inserting data into a table (overwriting old data)")

    tac_lane_detail_star_df.write.insertInto(
        tableName='{}.{}'.format(params.TARGET_DB_NAME, 'tac_lane_detail_star'),
        overwrite=True)
    logging.info("Loading {}.tac_lane_detail_star table has finished".format(params.TARGET_DB_NAME))

    logging.info("Started loading {}.tac_shpmt_detail_star table.".format(params.TARGET_DB_NAME))

    # Get target table column list
    target_table_cols = spark_session.table('{}.tac_shpmt_detail_star'.format(params.TARGET_DB_NAME)).schema.fieldNames()

    tac_shpmt_detail_star_df = \
        get_tac_shpmt_detail_star(logging, spark_session, params.TRANS_VB_DB_NAME, params.TARGET_DB_NAME,
                                  params.STAGING_LOCATION, debug_mode_ind, debug_postfix).select(target_table_cols)

    logging.info("Inserting data into a table (overwriting old data)")

    tac_shpmt_detail_star_df.write.insertInto(
        tableName='{}.{}'.format(params.TARGET_DB_NAME, 'tac_shpmt_detail_star'),
        overwrite=True)
    logging.info("Loading {}.tac_shpmt_detail_star table has finished".format(params.TARGET_DB_NAME))
