'''Function loading the carrier dashboard tables
'''

import pyspark.sql.functions as f
from pyspark.sql import Window
import utils
from get_src_data import get_transfix as tvb
from load_netw_scorec import expr_network_scorecard as expr


def get_weekly_network_sccrd_star(
        logging, spark_session, trans_vsblt_db_name, target_db_name, staging_location, debug_mode_ind, debug_postfix):

    tac_tender_pg_summary_df = \
        tvb.get_tac_tender_pg_summary_new(logging, spark_session, 'ap_transfix_tv_na', target_db_name, staging_location,
                                      debug_mode_ind, debug_postfix) \
        .withColumn("carr_num", f.expr(expr.tendigit_carrier_id_expr))\
        .withColumnRenamed("carr_num", "long_forward_agent_id") \
        .withColumnRenamed("calendar_year_week_tac", "week_year_val") \
        .withColumnRenamed("carrier_id", "forward_agent_id") \
        .withColumn("lane_key", f.concat(f.col("origin_zone_ship_from_code"), f.lit("-"), f.col("dest_ship_from_code")))\
        .withColumnRenamed("tms_service_code", "service_tms_code")\
        .withColumnRenamed("origin_location_id", "origin_code") \
        .drop("carr_desc") \
        # .filter("week_year_val = '25/2019'") \
        # .filter("forward_agent_id = '15319746'").filter("dest_ship_from_code = '2002546843'") \


    monster_data_df = \
        tvb.get_monster_view_data(logging, spark_session, target_db_name, target_db_name, staging_location, debug_mode_ind,
                          debug_postfix)\
        .withColumn("lane_name", f.concat(f.col("parent_loc_code"), f.lit("-"), f.col("dest_ship_from_code")))\
        .withColumnRenamed("ship_week_num", "week_year_val") \
        .groupBy("parent_loc_code", "dest_ship_from_code", "ship_to_party_code", "carr_num", "service_tms_code",
                 "origin_code", "origin_zone_ship_from_code", "week_year_val") \
        .agg(f.max("ship_to_party_desc").alias("ship_to_party_desc"),
             f.max("dest_sold_to_name").alias("dest_sold_to_name"),
             f.max("carr_desc").alias("carr_desc"),
             f.max("actual_ship_date").alias("actual_ship_date"),
             f.max("freight_type_val").alias("freight_type_val"),
             f.max("cdot_ontime_cnt").alias("cdot_ontime_cnt"),
             f.sum("shpmt_cnt").alias("shpmt_cnt"),
             f.sum("shpmt_on_time_cnt").alias("shpmt_on_time_cnt"),
             f.sum("measrbl_shpmt_cnt").alias("measrbl_shpmt_cnt"),
             f.max("parent_carr_name").alias("parent_carr_name"),
             f.max("lane_name").alias("lane_name")
             )\
        .withColumnRenamed("carr_num", "forward_agent_id") \
        .drop("origin_zone_ship_from_code") \
        .drop("ship_to_party_code")#.drop("carr_desc")
        #.withColumnRenamed("lane_name", "lane_key") \
        #.drop("dest_ship_from_code")

    monster_data_2_df = \
        tvb.get_monster_view_data(logging, spark_session, target_db_name, target_db_name, staging_location, debug_mode_ind,
                          debug_postfix)\
        .withColumn("lane_name", f.concat(f.col("parent_loc_code"), f.lit("-"), f.col("dest_ship_from_code")))\
        .withColumnRenamed("ship_week_num", "week_year_val") \
        .groupBy("dest_ship_from_code", "carr_num", "service_tms_code",
                 "origin_code", "week_year_val") \
        .agg(f.max("ship_to_party_desc").alias("ship_to_party_desc"),
             f.max("dest_sold_to_name").alias("dest_sold_to_name"),
             f.max("carr_desc").alias("carr_desc"),
             f.max("actual_ship_date").alias("actual_ship_date"),
             f.max("freight_type_val").alias("freight_type_val"),
             f.max("cdot_ontime_cnt").alias("cdot_ontime_cnt"),
             f.sum("shpmt_cnt").alias("shpmt_cnt"),
             f.sum("shpmt_on_time_cnt").alias("shpmt_on_time_cnt"),
             f.sum("measrbl_shpmt_cnt").alias("measrbl_shpmt_cnt"),
             f.max("parent_carr_name").alias("parent_carr_name"),
             f.max("ship_to_party_code").alias("ship_to_party_code"),
             f.max("origin_zone_ship_from_code").alias("origin_zone_ship_from_code"),
             f.max("parent_loc_code").alias("parent_loc_code"),
             f.max("lane_name").alias("lane_name")
             )\
        .withColumnRenamed("carr_num", "forward_agent_id") \
        .drop("origin_zone_ship_from_code") \
        .drop("ship_to_party_code")\
        .withColumn("monster_origin_code", f.col("origin_code")) \
        .withColumnRenamed("week_year_val", "week_year_val_mon") \
        .withColumnRenamed("forward_agent_id", "forward_agent_id_mon") \
        .withColumnRenamed("service_tms_code", "service_tms_code_mon") \
        .withColumnRenamed("dest_ship_from_code", "dest_ship_from_code_mon") \
        .withColumnRenamed("origin_code", "origin_code_mon") \
        #.withColumnRenamed("lane_name", "lane_key") \
        #.drop("dest_ship_from_code")

    lot_data_df = \
        tvb.get_lot_view_data(logging, spark_session, target_db_name, target_db_name, staging_location, debug_mode_ind,
                          debug_postfix)\
        .withColumn("lane_name", f.concat(f.col("origin_zone_code"), f.lit("-"), f.col("dest_ship_from_code")))\
        .withColumnRenamed("ship_week_num", "week_year_val")


    vfr_data_df = \
        tvb.get_vfr_agg_data(logging, spark_session, 'ap_transfix_tv_na', target_db_name, staging_location,
                              debug_mode_ind, debug_postfix) \
        .withColumn("lane_name", f.concat(f.col("origin_zone_ship_from_code"), f.lit("-"), f.col("dest_loc_code"))) \
        .withColumn("carrier_id", f.regexp_replace(f.col('carr_id'), '^0*', ''))\
        .withColumn("formatted_actl_ship_date", f.concat(f.substring(f.col("shpmt_start_date"), 7, 4), f.lit("-"),
                                                        f.substring(f.col("shpmt_start_date"), 4, 2), f.lit("-"),
                                                        f.substring(f.col("shpmt_start_date"), 1, 2))) \
        .withColumn("actl_ship_weekofyear", f.weekofyear(f.col("formatted_actl_ship_date")))\
        .withColumn("actl_ship_week", f.concat(f.col("actl_ship_weekofyear"), f.lit("/"),
                                               f.substring(f.col("shpmt_start_date"), 7, 4)))\
        .withColumn("calendar_week_num", f.lpad(f.weekofyear("formatted_actl_ship_date"), 2, '0')) \
        .withColumn("calendar_year_num", f.year("formatted_actl_ship_date")) \
        .withColumn("week_year_val", f.concat(f.col("calendar_week_num"), f.lit("/"), f.col("calendar_year_num")))

    utils.manageOutput(logging, spark_session, vfr_data_df, 0, "vfr_data_df", target_db_name,
                       staging_location, debug_mode_ind, "_{}{}".format(target_db_name, debug_postfix))



    logging.info("Joining data (2) - origin id.")

    netw_scorec2_1_df = monster_data_df.withColumn("monster_origin_code", f.col("origin_code"))\
        .join(tac_tender_pg_summary_df,
              ["week_year_val", "forward_agent_id", "service_tms_code", "origin_code", "dest_ship_from_code"], # remove: lane_key, add: destination_sf
              'left_outer')\

    logging.info("Joining data (2) - origin id has finished.")
    utils.manageOutput(logging, spark_session, netw_scorec2_1_df, 0, "netw_scorec2_1_df_new", target_db_name,
                       staging_location, debug_mode_ind, "_{}{}".format(target_db_name, debug_postfix))


    netw_scorec2_2_df = tac_tender_pg_summary_df\
        .join(monster_data_2_df,
              (monster_data_2_df.week_year_val_mon == tac_tender_pg_summary_df.week_year_val ) &
              (monster_data_2_df.forward_agent_id_mon == tac_tender_pg_summary_df.forward_agent_id) &
              (monster_data_2_df.service_tms_code_mon == tac_tender_pg_summary_df.service_tms_code) &
              (monster_data_2_df.dest_ship_from_code_mon == tac_tender_pg_summary_df.dest_ship_from_code) &
              (monster_data_2_df.origin_code_mon == tac_tender_pg_summary_df.origin_code) , how='right')\
         .filter((tac_tender_pg_summary_df.week_year_val.isNull()) & (tac_tender_pg_summary_df.forward_agent_id.isNull()) & 
         (tac_tender_pg_summary_df.service_tms_code.isNull()) & (tac_tender_pg_summary_df.origin_code.isNull()) & 
         (tac_tender_pg_summary_df.dest_ship_from_code.isNull()))
         # .filter("week_year_val = NULL").filter("forward_agent_id = NULL") \
         # .filter("service_tms_code = NULL").filter("origin_code = NULL").filter("dest_ship_from_code = NULL") \

    logging.info("Joining data (2) - origin id has finished.")
    utils.manageOutput(logging, spark_session, netw_scorec2_2_df, 0, "netw_scorec2_2_df", target_db_name,
                       staging_location, debug_mode_ind, "_{}{}".format(target_db_name, debug_postfix))

 
    netw_scorec_rename_df = netw_scorec2_2_df\
        .drop("week_year_val").drop("forward_agent_id") \
        .drop("service_tms_code").drop("dest_ship_from_code") \
        .drop("origin_code") \
        .withColumnRenamed("week_year_val_mon", "week_year_val") \
        .withColumnRenamed("forward_agent_id_mon", "forward_agent_id") \
        .withColumnRenamed("service_tms_code_mon", "service_tms_code") \
        .withColumnRenamed("dest_ship_from_code_mon", "dest_ship_from_code") \
        .withColumnRenamed("origin_code_mon", "origin_code") \

    logging.info("Joining data (2) - lane.")

    netw_scorec2_3_df = netw_scorec_rename_df\
        .groupBy("parent_loc_code", "dest_ship_from_code", "forward_agent_id", 
                 "service_tms_code", "week_year_val") \
        .agg(f.max("origin_zone_ship_from_code").alias("origin_zone_ship_from_code"),
             f.max("ship_to_party_desc").alias("ship_to_party_desc"), 
             f.max("dest_sold_to_name").alias("dest_sold_to_name"),
             f.max("actual_ship_date").alias("actual_ship_date"), 
             f.max("freight_type_val").alias("freight_type_val"), 
             f.max("cdot_ontime_cnt").alias("cdot_ontime_cnt"),
             f.max("shpmt_cnt").alias("shpmt_cnt"),
             f.max("shpmt_on_time_cnt").alias("shpmt_on_time_cnt"),
             f.max("measrbl_shpmt_cnt").alias("measrbl_shpmt_cnt"),
             f.max("parent_carr_name").alias("parent_carr_name"),
             f.max("lane_name").alias("lane_name"),
             f.max("monster_origin_code").alias("monster_origin_code"),
             f.max("carr_desc").alias("carr_desc")) \
        .drop("origin_zone_ship_from_code") \
        .withColumnRenamed("parent_loc_code", "origin_zone_ship_from_code")

    logging.info("Joining data (2) - origin id has finished.")
    utils.manageOutput(logging, spark_session, netw_scorec2_3_df, 0, "netw_scorec2_3_df", target_db_name,
                       staging_location, debug_mode_ind, "_{}{}".format(target_db_name, debug_postfix))

    netw_scorec2_4_df = tac_tender_pg_summary_df\
        .join(netw_scorec2_3_df.withColumn("parent_loc_code", f.col("origin_zone_ship_from_code")),
              ["week_year_val", "forward_agent_id", "service_tms_code", "origin_zone_ship_from_code", "dest_ship_from_code"], # remove: lane_key, add: destination_sf
              'right_outer')\

    logging.info("Joining data (2) - origin id has finished.")
    utils.manageOutput(logging, spark_session, netw_scorec2_4_df, 0, "netw_scorec2_4_df", target_db_name,
                       staging_location, debug_mode_ind, "_{}{}".format(target_db_name, debug_postfix))


    netw_scorec2_df = \
        netw_scorec2_1_df.select("week_year_val", "lane_key", "campus_lane_name", "forward_agent_id", "service_tms_code",
                                 "origin_code", "monster_origin_code", "actual_carr_trans_cost_amt", "linehaul_cost_amt",
                                 "incrmtl_freight_auction_cost_amt", "cnc_carr_mix_cost_amt",
                                 "unsource_cost_amt", "fuel_cost_amt", "acsrl_cost_amt", "lane",
                                 "origin_zone_ship_from_code", "dest_ship_from_code", "carr_desc",
                                 "sold_to_party_id", "avg_award_weekly_vol_qty", "ship_cond_val",
                                 "country_from_code", "country_to_code", "freight_auction_flag",
                                 "primary_carr_flag", "week_begin_date", "month_type_val", "cal_year_num",
                                 "month_date", "week_num", "region_code", "accept_cnt", "total_cnt",
                                 "dest_postal_code", "reject_cnt", "accept_pct", "reject_pct",
                                 "expct_vol_val", "reject_below_award_val", "weekly_carr_rate",
                                 "customer_desc", "customer_code", "customer_lvl3_desc",
                                 "customer_lvl5_desc", "customer_lvl6_desc", "customer_lvl12_desc",
                                 "customer_specific_lane_name", "long_forward_agent_id",
                                 "parent_loc_code", "ship_to_party_desc", "dest_sold_to_name",
                                 "actual_ship_date", "freight_type_val", "cdot_ontime_cnt", "shpmt_cnt",
                                 "shpmt_on_time_cnt", "measrbl_shpmt_cnt", "parent_carr_name") \
        .union(
        netw_scorec2_4_df.select("week_year_val", "lane_key", "campus_lane_name", "forward_agent_id", "service_tms_code",
                                 "origin_code", "monster_origin_code", "actual_carr_trans_cost_amt", "linehaul_cost_amt",
                                 "incrmtl_freight_auction_cost_amt", "cnc_carr_mix_cost_amt", "unsource_cost_amt",
                                 "fuel_cost_amt", "acsrl_cost_amt",
                                 "lane", "origin_zone_ship_from_code", "dest_ship_from_code", "carr_desc",
                                 "sold_to_party_id", "avg_award_weekly_vol_qty", "ship_cond_val", "country_from_code",
                                 "country_to_code", "freight_auction_flag", "primary_carr_flag", "week_begin_date",
                                 "month_type_val", "cal_year_num", "month_date", "week_num", "region_code",
                                 "accept_cnt", "total_cnt", "dest_postal_code", "reject_cnt", "accept_pct",
                                 "reject_pct", "expct_vol_val", "reject_below_award_val", "weekly_carr_rate",
                                 "customer_desc", "customer_code", "customer_lvl3_desc", "customer_lvl5_desc",
                                 "customer_lvl6_desc", "customer_lvl12_desc", "customer_specific_lane_name",
                                 "long_forward_agent_id", "parent_loc_code", "ship_to_party_desc", "dest_sold_to_name",
                                 "actual_ship_date", "freight_type_val", "cdot_ontime_cnt", "shpmt_cnt",
                                 "shpmt_on_time_cnt", "measrbl_shpmt_cnt", "parent_carr_name"))

    logging.info("Joining data (2) - final has finished.")
    utils.manageOutput(logging, spark_session, netw_scorec2_df, 0, "netw_scorec2_df", target_db_name,
                       staging_location, debug_mode_ind, "_{}{}".format(target_db_name, debug_postfix))

    netw_scorec_filter_df = netw_scorec2_df\
        .where(netw_scorec2_df.monster_origin_code != 'NULL')

    netw_scorec3_df = lot_data_df.drop("load_id").drop("actual_ship_date").drop("freight_type_val")\
        .groupBy("carr_num", "dest_ship_from_code", "ship_point_code", 
                 "actual_service_tms_code", "week_year_val") \
        .agg(f.max("carr_desc").alias("carr_desc_lot"),
             # f.max("lane_name").alias("lane_name"),
             f.max("dest_zone_val").alias("dest_zone_val_lot"),
             f.max("origin_zone_code").alias("origin_zone_code_lot"),
             f.max("origin_zone_ship_from_code").alias("origin_zone_ship_from_code_lot"),
             f.max("ship_to_party_code").alias("ship_to_party_code_lot"),
             f.max("ship_to_party_desc").alias("ship_to_party_desc_lot"),
             f.sum("lot_otd_cnt").alias("lot_otd_count"),
             f.sum("lot_tat_late_counter_val").alias("lot_tat_late_counter_val"),
             f.sum("lot_cust_failure_cnt").alias("lot_customer_failure_cnt"),
             f.sum("pg_failure_cnt").alias("pg_failure_cnt"),
             f.sum("carr_failure_cnt").alias("carr_failure_cnt"),
             f.sum("others_failure_cnt").alias("others_failure_cnt"),
             f.max("tolrnc_sot_val").alias("tolrnc_sot_val")) \
        .withColumnRenamed("ship_point_code", "monster_origin_code") \
        .withColumnRenamed("actual_service_tms_code", "service_tms_code") \
        .withColumnRenamed("carr_num", "forward_agent_id") \
        .drop("carr_desc_lot").drop("dest_zone_val_lot") \
        .drop("origin_zone_code_lot").drop("origin_zone_ship_from_code_lot") \
        .drop("ship_to_party_code_lot").drop("ship_to_party_desc_lot") \
        # .drop("dest_ship_from_code").drop("ship_point_code")

    logging.info("Joining data (3) has finished.")
    utils.manageOutput(logging, spark_session, netw_scorec3_df, 0, "netw_scorec3_df", target_db_name,
                       staging_location, debug_mode_ind, "_{}{}".format(target_db_name, debug_postfix))


    logging.info("Joining data (4).")

    netw_scorec4_df = netw_scorec_filter_df\
        .join(netw_scorec3_df, ["monster_origin_code", "dest_ship_from_code", "forward_agent_id",
                                "service_tms_code", "week_year_val" ],
              'left_outer')

    logging.info("Joining data (4) has finished.")
    utils.manageOutput(logging, spark_session, netw_scorec4_df, 0, "netw_scorec4_df", target_db_name,
                       staging_location, debug_mode_ind, "_{}{}".format(target_db_name, debug_postfix))

    logging.info("Joining data (5).")

    netw_scorec5_df = vfr_data_df.drop("load_id").drop("shpmt_start_date")\
        .groupBy("ship_point_code", "dest_ship_from_code", "carrier_id", "tms_service_code", "actl_ship_week")\
        .agg(f.max("carr_desc").alias("carr_desc_vfr"),
             f.max("origin_loc_id").alias("origin_loc_id_vfr"),
             f.max("lane_name").alias("lane_name"),
             #f.max("week_year_val").alias("week_year_val"),
             #f.max("dest_ship_from_code").alias("dest_ship_from_code_vfr"),
             f.sum("su_per_load_cnt").alias("su_per_load_cnt"),
             f.sum("plan_gross_weight_qty").alias("plan_gross_weight_qty"),
             f.sum("plan_net_weight_qty").alias("plan_net_weight_qty"),
             f.sum("shipped_gross_weight_qty").alias("shipped_gross_weight_qty"),
             f.sum("shipped_net_weight_qty").alias("shipped_net_weight_qty"),
             f.sum("plan_gross_vol_qty").alias("plan_gross_vol_qty"),
             f.sum("plan_net_vol_qty").alias("plan_net_vol_qty"),
             f.sum("shipped_gross_vol_qty").alias("shipped_gross_vol_qty"),
             f.sum("shipped_net_vol_qty").alias("shipped_net_vol_qty"),
             f.sum("max_weight_qty").alias("max_weight_qty"),
             f.sum("max_vol_trans_mgmt_sys_qty").alias("max_vol_trans_mgmt_sys_qty"),
             f.avg("plan_gross_weight_qty").alias("avg_plan_gross_weight_qty"),
             f.avg("plan_net_weight_qty").alias("avg_plan_net_weight_qty"),
             f.avg("shipped_gross_weight_qty").alias("avg_shipped_gross_weight_qty"),
             f.avg("shipped_net_weight_qty").alias("avg_shipped_net_weight_qty"),
             f.avg("plan_gross_vol_qty").alias("avg_plan_gross_vol_qty"),
             f.avg("plan_net_vol_qty").alias("avg_plan_net_vol_qty"),
             f.avg("shipped_gross_vol_qty").alias("avg_shipped_gross_vol_qty"),
             f.avg("shipped_net_vol_qty").alias("avg_shipped_net_vol_qty"),
             f.avg("max_weight_qty").alias("avg_max_weight_qty"),
             f.avg("max_vol_trans_mgmt_sys_qty").alias("avg_max_vol_trans_mgmt_sys_qty"),
             f.avg("floor_position_qty").alias("floor_position_qty"),
             f.avg("max_pallet_tms_trans_type_qty").alias("max_pallet_tms_trans_type_qty"),
             f.sum("cut_impact_rate").alias("cut_impact_rate"),
             f.sum("drf_last_truck_amt").alias("drf_last_truck_amt"),
             f.sum("glb_segment_impact_cat_ld_amt").alias("glb_segment_impact_cat_ld_amt"),
             f.sum("hopade_amt").alias("hopade_amt"),
             f.sum("max_orders_incrmtl_amt").alias("max_orders_incrmtl_amt"),
             f.sum("max_orders_non_drp_amt").alias("max_orders_non_drp_amt"),
             f.sum("total_vf_optny_amt").alias("total_vf_optny_amt")) \
        .withColumnRenamed("lane_name", "lane_key") \
        .withColumnRenamed("tms_service_code", "service_tms_code") \
        .withColumnRenamed("carrier_id", "forward_agent_id") \
        .withColumnRenamed("ship_point_code", "monster_origin_code") \
        .withColumnRenamed("actl_ship_week", "week_year_val")\
        .drop("dest_loc_code") \
        .drop("carr_desc_vfr").drop("origin_loc_id_vfr") \
        .drop("dest_ship_from_code_vfr") \
        .drop("origin_zone_ship_from_code")
        #.withColumnRenamed("origin_zone_ship_from_code", "monster_origin_code")\

    logging.info("Joining data (5) has finished.")
    utils.manageOutput(logging, spark_session, netw_scorec5_df, 0, "netw_scorec5_df", target_db_name,
                       staging_location, debug_mode_ind, "_{}{}".format(target_db_name, debug_postfix))

    logging.info("Joining data (6).")

    netw_scorec6_df = netw_scorec4_df\
        .join(netw_scorec5_df,
              ["monster_origin_code", "dest_ship_from_code", "forward_agent_id", "service_tms_code", "week_year_val"],
              'left_outer')\
        .withColumnRenamed("lane", "lane_name") \
        .drop("lane_key").drop("origin_loc_id_vfr") \

    logging.info("Joining data (6) has finished.")
    utils.manageOutput(logging, spark_session, netw_scorec6_df, 0, "netw_scorec6_df", target_db_name,
                       staging_location, debug_mode_ind, "_{}{}".format(target_db_name, debug_postfix))

    return netw_scorec6_df



def load_network_scorecard(logging, config_module, debug_mode_ind, debug_postfix):
    ''' Load the weekly_network_sccrd_agg_star tables'''

    # Create a spark session
    params = utils.ConfParams.build_from_module(logging, config_module, debug_mode_ind, debug_postfix)
    #spark_params = utils.ConfParams.build_from_module(logging, config_spark_module, debug_mode_ind, debug_postfix)
    spark_conf = list(set().union(
        params.SPARK_GLOBAL_PARAMS,
        params.SPARK_TFX_PARAMS)
        )
    spark_session = utils.get_spark_session(logging, 'tfx_wns', params.SPARK_MASTER, spark_conf)

    #Remove debug tables (if they are)
    utils.removeDebugTables(logging, spark_session, params.TARGET_DB_NAME, debug_mode_ind, debug_postfix)

    logging.info("Started loading {}.weekly_network_sccrd_agg_star table.".format(params.TARGET_DB_NAME))

    # Get target table column list
    target_table_cols = spark_session.table('{}.weekly_network_sccrd_agg_star'.format(params.TARGET_DB_NAME))\
        .schema.fieldNames()

    weekly_network_sccrd_star_df = \
        get_weekly_network_sccrd_star(logging, spark_session, params.TRANS_VB_DB_NAME, params.TARGET_DB_NAME,
                                   params.STAGING_LOCATION, debug_mode_ind, debug_postfix).select(target_table_cols)

    logging.info("Inserting data into a table (overwriting old data)")

    weekly_network_sccrd_star_df.write.insertInto(
        tableName='{}.{}'.format(params.TARGET_DB_NAME, 'weekly_network_sccrd_agg_star'),
        overwrite=True)
    logging.info("Loading {}.weekly_network_sccrd_agg_star table has finished".format(params.TARGET_DB_NAME))
