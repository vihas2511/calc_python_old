'''Function loading the carrier dashboard tables
'''

import pyspark.sql.functions as f
from pyspark.sql import Window
import utils
from get_src_data import get_rds as rds, get_transfix as tvb
from load_vfr import expr_vfr as expr


def insert_df_to_table(logging, df_to_insert, target_db_name, target_table_name):
    logging.info("Inserting data into {}.{} (overwriting old data)".format(target_db_name, target_table_name))

    df_to_insert.write.insertInto(
        tableName='{}.{}'.format(target_db_name, target_table_name),
        overwrite=True)
    logging.info("Loading {}.{} table has finished".format(target_db_name, target_table_name))


def df_transpose(src_df, by_key):
    '''Transpose data_frame columns to rows by a given key
       src_df - source data frame
       by_key - key for transpose
       example:
           sample_df:
           id   |  col1  | col2
            1   |  0.1   | 4.0
            2   |  0.6   | 10.0

           df_transpose(sample_df, ["id"]):
           id   | key    | val
            1   | col1   | 0.1
            1   | col2   | 4.0
            2   | col1   | 0.6
            2   | col2   | 10.0
    '''

    # Filter dtypes and split into column names and type description
    cols, dtypes = zip(*((c, t) for (c, t) in src_df.dtypes if c not in by_key))
    # Check if columns are the same type
    assert len(set(dtypes)) == 1, "All columns have to be of the same type"

    # Create array of (column_name, column_value) for given key
    kvs = f.explode(f.array([f.struct(f.lit(c).alias("key"), f.col(c).alias("val")) for c in cols])).alias("kvs")

    return src_df.select(by_key + [kvs]).select(by_key + ["kvs.key", "kvs.val"])


def get_vfr_data_hub_star(logging, spark_session, transvb_db_name, rds_db_name, target_db_name, staging_location,
                          debug_mode_ind, debug_postfix):

    cust_dim_df = rds.get_cust_dim(logging, spark_session, rds_db_name, target_db_name, staging_location,
                                   debug_mode_ind, debug_postfix)\
        .distinct()

    trade_chanl_hier_dim_df = rds.get_trade_chanl_hier_dim(logging, spark_session, rds_db_name, target_db_name,
                                                           staging_location, debug_mode_ind, debug_postfix) \
        .distinct()

    logging.info("Creating trade channel hierarchy data.")

    trade_chanl_df = cust_dim_df.join(trade_chanl_hier_dim_df, "trade_chanl_id", how='inner')\
        .withColumnRenamed("cust_id", "customer_lvl12_code")

    utils.manageOutput(logging, spark_session, trade_chanl_df, 1, "trade_chanl_df", target_db_name,
                       staging_location, debug_mode_ind, "_{}{}".format(target_db_name, debug_postfix))

    logging.info("Creating trade channel hierarchy data has finished.")

    origin_sf_dict_df = tvb.get_origin_sf_dict(logging, spark_session)

    otd_dh_df = \
        tvb.get_on_time_data_hub_star(logging, spark_session, target_db_name, target_db_name, staging_location,
                                      debug_mode_ind, debug_postfix)\
        .select("load_id", "load_builder_prty_val").distinct()\
        .withColumn("load_builder_prty_val", f.expr(expr.load_builder_prty_val_expr)) \
        .filter("load_builder_prty_val > 0")

    logging.info("Getting Truck data.")

    leo_truck_report_df = \
        tvb.get_leo_truck_report_lkp(logging, spark_session, transvb_db_name, target_db_name, staging_location,
                                     debug_mode_ind, debug_postfix)\
        .drop("load_tmstp").drop("load_from_file_name").drop("last_update_utc_tmstp").drop("weight_uom")\
        .drop("volume_uom").drop("predecessor_doc_category_name").drop("predecessor_doc_num")\
        .drop("successor_doc_category_name")\
        .withColumn("release_dttm", f.concat(f.col("release_date"), f.col("release_datetm"))) \
        .withColumn("pallet_num_qty", f.col("pallet_num_qty").cast("double"))\
        .withColumn("pallet_spot_qty", f.col("pallet_spot_qty").cast("double"))\
        .withColumn("total_gross_weight_qty", f.col("total_gross_weight_qty").cast("double"))\
        .withColumn("total_gross_vol_qty", f.col("total_gross_vol_qty").cast("double"))\
        .groupBy("follow_on_doc_num", "leo_tour_id", "release_dttm") \
        .agg(
            f.sum("pallet_num_qty").alias("pallet_num_qty"),
            f.sum("pallet_spot_qty").alias("pallet_spot_qty"),
            f.sum("total_gross_weight_qty").alias("total_gross_weight_qty"),
            f.sum("total_gross_vol_qty").alias("total_gross_vol_qty"),
            f.max("release_date").alias("release_date"),
            f.max("release_datetm").alias("release_datetm"),
            f.max("truck_type_code").alias("truck_type_code"),
            f.max("truck_ship_to_num").alias("truck_ship_to_num"),
            f.max("truck_ship_to_desc").alias("truck_ship_to_desc"),
            f.max("truck_sold_to_num").alias("truck_sold_to_num"),
            f.max("truck_sold_to_desc").alias("truck_sold_to_desc"),
            f.max("truck_ship_point_code").alias("truck_ship_point_code")
        ) \
        .withColumn("rn",
                    f.row_number().over(
                        Window.partitionBy("follow_on_doc_num")
                            .orderBy(f.col("release_dttm").desc()))) \
        .filter("rn=1").drop("rn")

    leo_vehicle_maintenance_df = \
        tvb.get_leo_vehicle_maintenance_lkp(logging, spark_session, transvb_db_name, target_db_name, staging_location,
                                            debug_mode_ind, debug_postfix)\
        .drop("load_tmstp").drop("load_from_file_name").drop("last_update_utc_tmstp").drop("floorspot_uom")\
        .drop("weight_uom").drop("table_uom")\
        .withColumn("vehicle_type2_code", f.col("vehicle_type_code"))\
        .withColumnRenamed("vehicle_type_code", "truck_vehicle_type_code")\
        .withColumn("vehicle_true_max_weight_qty", f.col("total_weight_qty"))\
        .withColumn("vehicle_true_max_vol_qty", f.expr(expr.vehicle_true_max_vol_qty_expr))

    order_shipment_linkage_zvdf_df = \
        tvb.get_order_shipment_linkage_zvdf_lkp(logging, spark_session, transvb_db_name, target_db_name, staging_location,
                                                debug_mode_ind, debug_postfix)\
        .withColumn("rn",
                    f.row_number().over(
                        Window.partitionBy("sap_order_num", "transport_num").orderBy(f.col("load_tmstp").desc()))) \
        .filter("rn=1").drop("rn")\
        .drop("load_tmstp").drop("load_from_file_name").drop("last_update_utc_tmstp")\
        .withColumn("doc_flow_order_num", f.col("sap_order_num")) \
        .withColumnRenamed("sap_order_num", "order_num") \
        .withColumn("load_id", f.expr('SUBSTR(transport_num, -9)'))\
        .withColumn("doc_flow_load_id", f.col('transport_num'))

    leo_join1_df = \
        order_shipment_linkage_zvdf_df\
        .join(leo_truck_report_df, leo_truck_report_df.follow_on_doc_num == order_shipment_linkage_zvdf_df.order_num)\
        .join(leo_vehicle_maintenance_df,
              leo_truck_report_df.truck_type_code == leo_vehicle_maintenance_df.vehicle_trans_medium_code)

    utils.manageOutput(logging, spark_session, leo_join1_df, 1, "leo_join1_df", target_db_name,
                       staging_location, debug_mode_ind, "_{}{}".format(target_db_name, debug_postfix))

    logging.info("Getting Truck data has finished.")

    otd_vfr_df = tvb.get_otd_vfr_na_star(logging, spark_session, transvb_db_name, target_db_name, staging_location,
                                         debug_mode_ind, debug_postfix)\
        .withColumn("gi_month_num", f.expr(expr.gi_month_expr))\
        .withColumn("load_id", f.expr(expr.load_id_expr))\
        .withColumn("load_gbu_id", f.expr(expr.load_gbu_id_expr))\
        .withColumn("material_doc_num", f.regexp_replace("material_doc_num", '^0+', ''))\
        .distinct()
        ##.filter('load_id in ("307536886", "307538413", "307555027", "307641568", "307679981", "307701260", "307767592")')

    utils.manageOutput(logging, spark_session, otd_vfr_df, 1, "otd_vfr_df", target_db_name,
                       staging_location, debug_mode_ind, "_{}{}".format(target_db_name, debug_postfix))

    logging.info("Calculating Shipped data from VFR.")

    otd_vfr_shipped_df = otd_vfr_df\
        .select("load_id", "dlvry_item_num", "material_doc_num", "net_vol_qty", "net_weight_qty", "gross_weight_qty",
                "gross_vol_qty")\
        .groupBy("load_id", "dlvry_item_num", "material_doc_num")\
        .agg(
             f.max("net_vol_qty").alias("net_vol_qty"),
             f.max("net_weight_qty").alias("net_weight_qty"),
             f.max("gross_vol_qty").alias("gross_vol_qty"),
             f.max("gross_weight_qty").alias("gross_weight_qty")) \
        .groupBy("load_id") \
        .agg(
             f.sum("net_vol_qty").alias("shipped_net_vol_qty"),
             f.sum("net_weight_qty").alias("shipped_net_weight_qty"),
             f.sum("gross_vol_qty").alias("shipped_gross_vol_qty"),
             f.sum("gross_weight_qty").alias("shipped_gross_weight_qty"))

    utils.manageOutput(logging, spark_session, otd_vfr_shipped_df, 0, "otd_vfr_shipped_df", target_db_name,
                       staging_location, debug_mode_ind, "_{}{}".format(target_db_name, debug_postfix))

    logging.info("Calculating Shipped data from VFR has finished.")

    logging.info("Calculating Prepared data from VFR.")

    #        .withColumn("dlvry_item_num_cnt",
    #                    f.count("*").over(Window.partitionBy(
    #                        f.col("load_id"),
    #                        f.col("load_gbu_id"),
    #                        f.col("tdcval_code"),
    #                        f.col("material_doc_num"),
    #                        f.col("gross_weight_qty")
    #                    )))
    otd_vfr_prepare_df = otd_vfr_df \
        .select("load_id", "load_gbu_id", "carr_id", "tdcval_code", "material_doc_num", "dlvry_item_num", "tdcval_desc",
                "ship_to_party_id", "ship_to_party_desc", "dlvry_id",
                "vfr_freight_type_code", "gbu_desc", "trans_plan_point_code", "gbu_code", "order_num",
                "oblb_gross_weight_plan_qty", "oblb_gross_weight_shipped_qty", "oblb_net_weight_plan_qty",
                "oblb_gross_vol_shipped_qty", "oblb_gross_vol_plan_qty", "oblb_net_vol_plan_qty", "ship_cond_desc",
                "net_vol_qty", "net_weight_qty", "gross_weight_qty", "gross_vol_qty",
                "max_weight_qty", "max_vol_trans_mgmt_sys_qty", "max_pallet_tms_trans_type_qty", "floor_position_qty",
                "actual_cost_amt", "trans_stage_num", "ship_cond_val")\
        .distinct() \
        .join(otd_vfr_shipped_df, ["load_id"], how='left') \
        .join(leo_join1_df, ["load_id", "order_num"], how='left') \
        .withColumn("oblb_gross_weight_plan_qty", f.expr(expr.oblb_gross_weight_plan_qty_expr)) \
        .withColumn("oblb_gross_vol_plan_qty", f.expr(expr.oblb_gross_vol_plan_qty_expr)) \
        .withColumn("ordered_shipped_flag", f.expr(expr.ordered_shipped_flag_expr))\
        .withColumn("palletsshipped", f.expr(expr.palletsshipped_expr))

    utils.manageOutput(logging, spark_session, otd_vfr_prepare_df, 1, "otd_vfr_prepare_df", target_db_name,
                       staging_location, debug_mode_ind, "_{}{}".format(target_db_name, debug_postfix))

    logging.info("Calculating Prepared data from VFR has finished.")

    logging.info("Calculating AVG data from VFR.")

    otd_vfr_grp2_df = otd_vfr_prepare_df\
        .select("load_id", "load_gbu_id", "tdcval_code", "material_doc_num", "dlvry_item_num", "floor_position_qty",
                "palletsshipped", "net_vol_qty", "net_weight_qty", "gross_weight_qty", "gross_vol_qty",
                "trans_stage_num")\
        .distinct()\
        .groupBy("load_id")\
        .agg(
             #f.avg("floor_position_qty").alias("avg_floor_position_qty_"),
             f.max("palletsshipped").alias("avg_palletsshipped_"),
             f.sum("net_weight_qty").alias("sum_material_weight_qty"),
             f.sum("net_vol_qty").alias("sum_material_vol_qty"),
             f.sum("net_vol_qty").alias("avg_net_vol_qty_"),
             f.sum("gross_weight_qty").alias("sum_gross_material_weight_qty"),
             f.sum("gross_vol_qty").alias("sum_gross_material_vol_qty"),
             f.min("trans_stage_num").alias("min_trans_stage_num"),
             f.min("trans_stage_num").alias("trans_stage_num"),
             f.count("*").alias("shipped_load_cnt"))

    utils.manageOutput(logging, spark_session, otd_vfr_grp2_df, 1, "otd_vfr_grp2_df", target_db_name,
                       staging_location, debug_mode_ind, "_{}{}".format(target_db_name, debug_postfix))

    logging.info("Calculating AVG data from VFR has finished.")

    logging.info("Calculating Planned by Load data from VFR.")

    otd_vfr_plan_df = otd_vfr_prepare_df\
        .select("load_id", "dlvry_id", "oblb_gross_weight_plan_qty", "oblb_gross_vol_plan_qty",
                "oblb_net_weight_plan_qty", "oblb_net_vol_plan_qty")\
        .groupBy("load_id", "dlvry_id")\
        .agg(
             f.max("oblb_gross_weight_plan_qty").alias("max_oblb_gross_weight_plan_qty"),
             f.max("oblb_gross_vol_plan_qty").alias("max_oblb_gross_vol_plan_qty"),
             f.max("oblb_net_weight_plan_qty").alias("max_oblb_net_weight_plan_qty"),
             f.max("oblb_net_vol_plan_qty").alias("max_oblb_net_vol_plan_qty"))\
        .groupBy("load_id")\
        .agg(
             f.sum("max_oblb_gross_weight_plan_qty").alias("plan_gross_weight_qty"),
             f.sum("max_oblb_gross_vol_plan_qty").alias("plan_gross_vol_qty"),
             f.sum("max_oblb_net_weight_plan_qty").alias("plan_net_weight_qty"),
             f.sum("max_oblb_net_vol_plan_qty").alias("plan_net_vol_qty"))

    utils.manageOutput(logging, spark_session, otd_vfr_plan_df, 1, "otd_vfr_plan_df", target_db_name,
                       staging_location, debug_mode_ind, "_{}{}".format(target_db_name, debug_postfix))

    logging.info("Calculating Planned by Load data from VFR has finished.")

    tac_df = tvb.get_tac(logging, spark_session, target_db_name, target_db_name, staging_location, debug_mode_ind,
                         debug_postfix)\
        .withColumn("load_id", f.regexp_replace("load_id", '^0', ''))\
        .withColumnRenamed("forward_agent_id", "carr_id").withColumnRenamed("service_tms_code", "tms_service_code") \
        .drop("daily_award_qty") \
        ##.filter('load_id in ("307536886", "307538413", "307555027", "307641568", "307679981", "307701260", "307767592")')

    utils.manageOutput(logging, spark_session, tac_df, 1, "tac_df", target_db_name,
                       staging_location, debug_mode_ind, "_{}{}".format(target_db_name, debug_postfix))

    logging.info("Calculating customer hierarchy.")

    tac_cust_hier_df = \
        tac_df.select("ship_to_party_id",
                      "customer_lvl1_code", "customer_lvl1_desc",
                      "customer_lvl2_code", "customer_lvl2_desc",
                      "customer_lvl3_code", "customer_lvl3_desc",
                      "customer_lvl4_code", "customer_lvl4_desc",
                      "customer_lvl5_code", "customer_lvl5_desc",
                      "customer_lvl6_code", "customer_lvl6_desc",
                      "customer_lvl7_code", "customer_lvl7_desc",
                      "customer_lvl8_code", "customer_lvl8_desc",
                      "customer_lvl9_code", "customer_lvl9_desc",
                      "customer_lvl10_code", "customer_lvl10_desc",
                      "customer_lvl11_code", "customer_lvl11_desc",
                      "customer_lvl12_code", "customer_lvl12_desc").distinct()\
        .groupBy("ship_to_party_id")\
        .agg(
           f.max("customer_lvl1_code").alias("customer_lvl1_code"),
           f.max("customer_lvl1_desc").alias("customer_lvl1_desc"),
           f.max("customer_lvl2_code").alias("customer_lvl2_code"),
           f.max("customer_lvl2_desc").alias("customer_lvl2_desc"),
           f.max("customer_lvl3_code").alias("customer_lvl3_code"),
           f.max("customer_lvl3_desc").alias("customer_lvl3_desc"),
           f.max("customer_lvl4_code").alias("customer_lvl4_code"),
           f.max("customer_lvl4_desc").alias("customer_lvl4_desc"),
           f.max("customer_lvl5_code").alias("customer_lvl5_code"),
           f.max("customer_lvl5_desc").alias("customer_lvl5_desc"),
           f.max("customer_lvl6_code").alias("customer_lvl6_code"),
           f.max("customer_lvl6_desc").alias("customer_lvl6_desc"),
           f.max("customer_lvl7_code").alias("customer_lvl7_code"),
           f.max("customer_lvl7_desc").alias("customer_lvl7_desc"),
           f.max("customer_lvl8_code").alias("customer_lvl8_code"),
           f.max("customer_lvl8_desc").alias("customer_lvl8_desc"),
           f.max("customer_lvl9_code").alias("customer_lvl9_code"),
           f.max("customer_lvl9_desc").alias("customer_lvl9_desc"),
           f.max("customer_lvl10_code").alias("customer_lvl10_code"),
           f.max("customer_lvl10_desc").alias("customer_lvl10_desc"),
           f.max("customer_lvl11_code").alias("customer_lvl11_code"),
           f.max("customer_lvl11_desc").alias("customer_lvl11_desc"),
           f.max("customer_lvl12_code").alias("customer_lvl12_code"),
           f.max("customer_lvl12_desc").alias("customer_lvl12_desc")
           # f.max("customer_desc").alias("customer_desc")
        )

    utils.manageOutput(logging, spark_session, tac_cust_hier_df, 1, "tac_cust_hier_df", target_db_name,
                       staging_location, debug_mode_ind, "_{}{}".format(target_db_name, debug_postfix))

    logging.info("Calculating customer hierarchy has finished.")

    logging.info("Calculating data from TAC.")

    tac_calcs_df = \
        tac_df.groupBy("load_id")\
        .agg(
           f.max("actual_carr_trans_cost_amt").alias("actual_carr_trans_cost_amt"),
           f.max("linehaul_cost_amt").alias("linehaul_cost_amt"),
           f.max("incrmtl_freight_auction_cost_amt").alias("incrmtl_freight_auction_cost_amt"),
           f.max("cnc_carr_mix_cost_amt").alias("cnc_carr_mix_cost_amt"),
           f.max("unsource_cost_amt").alias("unsource_cost_amt"),
           f.max("fuel_cost_amt").alias("fuel_cost_amt"),
           f.max("acsrl_cost_amt").alias("acsrl_cost_amt"),
           f.max("applnc_subsector_step_cnt").alias("applnc_subsector_step_cnt"),
           f.max("baby_care_subsector_step_cnt").alias("baby_care_subsector_step_cnt"),
           f.max("chemical_subsector_step_cnt").alias("chemical_subsector_step_cnt"),
           f.max("fabric_subsector_step_cnt").alias("fabric_subsector_step_cnt"),
           f.max("family_subsector_step_cnt").alias("family_subsector_step_cnt"),
           f.max("fem_subsector_step_cnt").alias("fem_subsector_step_cnt"),
           f.max("hair_subsector_step_cnt").alias("hair_subsector_step_cnt"),
           f.max("home_subsector_step_cnt").alias("home_subsector_step_cnt"),
           f.max("oral_subsector_step_cnt").alias("oral_subsector_step_cnt"),
           f.max("phc_subsector_step_cnt").alias("phc_subsector_step_cnt"),
           f.max("shave_subsector_step_cnt").alias("shave_subsector_step_cnt"),
           f.max("skin_subsector_cnt").alias("skin_subsector_cnt"),
           f.max("other_subsector_cnt").alias("other_subsector_cnt"),
           f.max("origin_zone_ship_from_code").alias("origin_zone_ship_from_code"),
           f.max("origin_loc_id").alias("origin_loc_id"),
           f.max("origin_zone_code").alias("origin_zone_code"),
           f.max("dest_ship_from_code").alias("dest_ship_from_code"),
           f.max("dest_zone_code").alias("dest_zone_code"),
           f.max("ship_to_party_id").alias("dest_loc_code"),
           f.max("tms_service_code").alias("tms_service_code"),
           f.avg("avg_award_weekly_vol_qty").alias("avg_award_weekly_vol_qty"),
           f.max("freight_type_code").alias("tac_freight_type_code"),
           f.max("freight_auction_flag").alias("freight_auction_flag")) \
        .withColumn("actual_carr_total_trans_cost_usd_amt", f.col("actual_carr_trans_cost_amt"))

    logging.info("Calculating data from TAC has finished.")

    logging.info("Calculating MAX data from TAC.")

    tac_awv_df = tac_df\
        .select("load_id", "carr_id", "tms_service_code", "avg_award_weekly_vol_qty")\
        .groupBy("load_id", "carr_id", "tms_service_code")\
        .agg(f.max("avg_award_weekly_vol_qty").alias("avg_average_awarded_weekly_volume_"))

    logging.info("Calculating MAX data from TAC has finished.")

    ##.filter('load_id in ("307536886", "307538413", "307555027", "307641568", "307679981", "307701260", "307767592")') \
    tfs_sel_df = tvb.get_tfs(logging, spark_session, target_db_name, target_db_name, staging_location, debug_mode_ind,
                         debug_postfix) \
        .withColumn("load_id", f.regexp_replace("shpmt_id", '^0', ''))

    logging.info("Calculating data from TFS.")

    tfs_df = tfs_sel_df.select("load_id", "step_per_load_rate", "freight_cost_charge_code") \
        .distinct().groupBy("load_id") \
        .agg(f.max("step_per_load_rate").alias("step_factor"),
             f.max("freight_cost_charge_code").alias("max_freight_cost_charge_code_")
             )

    utils.manageOutput(logging, spark_session, tfs_df, 1, "tfs_df", target_db_name,
                       staging_location, debug_mode_ind, "_{}{}".format(target_db_name, debug_postfix))

    logging.info("Calculating data from TFS has finished.")

    logging.info("Calculating SU from TFS.")

    tfs_su_df = tfs_sel_df.select("load_id", "su_per_load_qty") \
        .distinct().withColumn("su_per_load_qty_double", f.expr(expr.cast_su_expr))\
        .groupBy("load_id") \
        .agg(f.sum("su_per_load_qty_double").alias("su_per_load_cnt"))

    logging.info("CCalculating SU from TFS has finished.")

    tdc_df = tvb.get_tdcval_md(logging, spark_session, transvb_db_name, target_db_name, staging_location,
                               debug_mode_ind, debug_postfix)

    utils.manageOutput(logging, spark_session, tdc_df, 1, "tdc_df", target_db_name,
                       staging_location, debug_mode_ind, "_{}{}".format(target_db_name, debug_postfix))

    sambc_df = \
        tvb.get_sambc_master(logging, spark_session, transvb_db_name, target_db_name, staging_location,
                             debug_mode_ind, debug_postfix) \
        .select("customer_lvl3_desc", "sambc_flag")

    utils.manageOutput(logging, spark_session, sambc_df, 1, "sambc_df", target_db_name,
                       staging_location, debug_mode_ind, "_{}{}".format(target_db_name, debug_postfix))

    logging.info("Joining hier data.")

    hier_join_df = tac_cust_hier_df.join(trade_chanl_df, "customer_lvl12_code", how='left')

    utils.manageOutput(logging, spark_session, hier_join_df, 1, "hier_join_df", target_db_name,
                       staging_location, debug_mode_ind, "_{}{}".format(target_db_name, debug_postfix))

    logging.info("Joining hier data has finished.")

    logging.info("Joining data (1).")

    #.join(f.broadcast(tac_cust_hier_df), "ship_to_party_id", how='left')\
    #.join(f.broadcast(trade_chanl_df), "customer_lvl12_code", how='left')\

    otd_vfr_pre_join1_df = otd_vfr_prepare_df \
        .join(tac_calcs_df, "load_id", how='left') \
        .join(tfs_df, "load_id", how='left')\
        .join(otd_vfr_grp2_df, ["load_id", "trans_stage_num"], how='inner') \
        .withColumn("min_trans_stage_flag", f.expr(expr.min_trans_stage_flag_expr)) \
        .join(otd_vfr_plan_df, ["load_id"], how='left') \
        .join(tfs_su_df, ["load_id"], how='left') \
        .join(otd_dh_df, ["load_id"], how='left') \
        .join(tac_awv_df, ["load_id", "carr_id", "tms_service_code"], how='left') \
        .join(f.broadcast(hier_join_df), "ship_to_party_id", how='left')\
        .join(f.broadcast(sambc_df), "customer_lvl3_desc", how='left')\
        .join(f.broadcast(tdc_df), "tdcval_code", how='left')\
        .withColumnRenamed("category", "categ_name") \
        .withColumn("weight_vol_plan_qty", f.expr(expr.weight_vol_plan_qty_expr)) \
        .withColumn("weight_vol_ship_qty", f.expr(expr.weight_vol_ship_qty_expr)) \
        .withColumn("origin_freight_code", f.expr(expr.origin_freight_expr)) \
        .withColumn("primary_carr_flag", f.expr(expr.primary_carr_flag_expr)) \
        .withColumn("pallet_shipped_qty", f.expr(expr.pallets_shipped_expr)) \
        .withColumn("true_fa_flag", f.expr(expr.true_fa_flag_expr)) \
        .withColumn("density_rate", f.col("weight_vol_ship_qty"))\
        .withColumn("subsector_sales_cnt", f.expr(expr.subsector_sales_cnt_expr))\
        .withColumn("load_material_weight_qty", f.expr(expr.load_material_weight_qty_expr))\
        .withColumn("load_material_vol_qty", f.expr(expr.load_material_vol_qty_expr)) \
        .withColumn("combined_gbu_code", f.expr(expr.combined_gbu_expr)) \
        .withColumn("custom_vehicle_fill_rate_id", f.expr(expr.custom_vehicle_fill_rate_id_exp))
        #.filter("min_trans_stage_flag = 'Y'")

    utils.manageOutput(logging, spark_session, otd_vfr_pre_join1_df, 1, "otd_vfr_pre_join1_df", target_db_name,
                       staging_location, debug_mode_ind, "_{}{}".format(target_db_name, debug_postfix))

    logging.info("Joining data (1) has finished.")

    logging.info("Calculating MAX data from join (1).")

    #    .where("COALESCE(oblb_gross_weight_plan_qty, oblb_gross_weight_shipped_qty) > 0") \
    # .withColumn("load_density_rate", f.expr(expr.density_rate_expr))\
    otd_vfr_low_den_df = \
        otd_vfr_pre_join1_df.select("load_id", "origin_freight_code", "oblb_gross_weight_plan_qty",
                                    "oblb_gross_vol_plan_qty", "oblb_gross_weight_shipped_qty",
                                    "oblb_gross_vol_shipped_qty", "shipped_gross_weight_qty", "shipped_gross_vol_qty",
                                    "origin_zone_ship_from_code", "dest_ship_from_code") \
        .where("origin_zone_ship_from_code is not null") \
        .distinct() \
        .groupBy("origin_zone_ship_from_code", "dest_ship_from_code")\
        .agg(
            f.sum("oblb_gross_weight_plan_qty").alias("oblb_gross_weight_plan_qty"),
            f.sum("oblb_gross_vol_plan_qty").alias("oblb_gross_vol_plan_qty"),
            f.sum("oblb_gross_weight_shipped_qty").alias("oblb_gross_weight_shipped_qty"),
            f.sum("oblb_gross_vol_shipped_qty").alias("oblb_gross_vol_shipped_qty"),
            f.sum("shipped_gross_vol_qty").alias("shipped_gross_vol_qty"),
            f.sum("shipped_gross_weight_qty").alias("shipped_gross_weight_qty")
        )\
        .withColumn("weight_vol_plan_qty", f.expr(expr.weight_vol_plan_qty_expr)) \
        .withColumn("weight_vol_ship_qty", f.expr(expr.weight_vol_ship_qty_expr)) \
        .withColumn("load_density_rate", f.col("weight_vol_ship_qty"))\
        .withColumn("low_density_site_val", f.expr(expr.low_density_site_val_expr)) \
        .drop("oblb_gross_weight_plan_qty").drop("oblb_gross_vol_plan_qty").drop("oblb_gross_weight_shipped_qty")\
        .drop("oblb_gross_vol_shipped_qty").drop("weight_vol_plan_qty").drop("weight_vol_ship_qty")\
        .drop("shipped_gross_weight_qty").drop("shipped_gross_vol_qty")
        #.drop("density_rate")

    utils.manageOutput(logging, spark_session, otd_vfr_low_den_df, 1, "otd_vfr_low_den_df", target_db_name,
                       staging_location, debug_mode_ind, "_{}{}".format(target_db_name, debug_postfix))

    logging.info("Calculating MAX data from join (1) has finished.")

    logging.info("Calculating SUM data from join (1).")

    otd_vfr_pallets_df = otd_vfr_pre_join1_df.select("load_id", "pallet_shipped_qty", "shipped_load_cnt") \
        .distinct().withColumn("pallets_load_calc", f.expr(expr.pallets_load_expr)) \
        .groupBy("load_id").agg(f.sum("pallets_load_calc").alias("pallet_load_qty"))

    logging.info("Calculating SUM data from join (1) has finished.")

    logging.info("Calculating final ship to data from join (1).")

    otd_vfr_final_ship_df = otd_vfr_pre_join1_df\
        .select("load_id", "trans_stage_num", "ship_to_party_id", "ship_to_party_desc") \
        .distinct()\
        .withColumn("rn",
                    f.row_number().over(Window.partitionBy("load_id").orderBy(f.col("trans_stage_num").desc()))) \
        .withColumnRenamed("ship_to_party_id", "final_ship_to_party_id")\
        .withColumnRenamed("ship_to_party_desc", "final_ship_to_party_desc")\
        .filter("rn = 1").drop("trans_stage_num")

    logging.info("Calculating final ship to data from join (1) has finished.")

    logging.info("Joining data (2).")

    # f.broadcast(otd_vfr_low_den_df)
    otd_vfr_join2_df = otd_vfr_pre_join1_df\
        .join(otd_vfr_pallets_df, "load_id", how='left') \
        .join(otd_vfr_final_ship_df, "load_id", how='left') \
        .join(otd_vfr_low_den_df, ["origin_zone_ship_from_code", "dest_ship_from_code"], how='left')\
        .withColumn("total_load_cost_amt", f.expr(expr.total_load_cost_amt_expr)) \
        .withColumn("check_density", f.expr(expr.check_density_expr)) \
        .withColumn("pallet_impact_amt", f.expr(expr.pallet_impact_amt_expr)) \
        .withColumn("pallet_impact_pct", f.expr(expr.pallet_impact_pct_expr)) \
        .withColumn("net_vol_order_qty", f.expr(expr.net_vol_order_qty_expr)) \
        .withColumn("net_weight_order_qty", f.expr(expr.net_weight_order_qty_expr)) \
        .withColumn("net_vol_fill_rate", f.expr(expr.net_vol_fill_rate_expr)) \
        .withColumn("max_net_weight_order_qty", f.expr(expr.max_net_weight_order_qty_expr)) \
        .withColumn("opertng_space_impact_amt", f.expr(expr.opertng_space_impact_amt_expr)) \
        .withColumn("opertng_space_pct", f.lit(0.11))

    logging.info("Joining data (2) has finished.")
    utils.manageOutput(logging, spark_session, otd_vfr_join2_df, 1, "otd_vfr_join2_df", target_db_name,
                       staging_location, debug_mode_ind, "_{}{}".format(target_db_name, debug_postfix))

    #logging.info("Calculating additional Low Density Site from OTD data.")
    #
    #low_density_site_null_df = \
    #    otd_vfr_join2_df.select("load_id", "low_density_site_val").filter("low_density_site_val is null").distinct()
    #
    #otd_low_density_df = \
    #    tvb.get_on_time_data_hub_star(logging, spark_session, target_db_name, target_db_name, staging_location,
    #                                  debug_mode_ind, debug_postfix)\
    #    .select("load_id", "frt_type_desc", "origin_zone_code", "origin_tac_zone_code").distinct()\
    #    .join(low_density_site_null_df, "load_id", how='inner')\
    #    .withColumnRenamed("origin_tac_zone_code", "origin_zone_ship_from_code")\
    #    .withColumn("vfr_freight_type_code", f.expr(expr.freight_type_code_expr))\
    #    .withColumn("origin_freight_code", f.expr(expr.origin_freight_expr))\
    #    .withColumnRenamed("vfr_freight_type_code", "otd_vfr_freight_type_code")\
    #    .drop("origin_zone_code").drop("frt_type_desc").drop("low_density_site_val").drop("origin_zone_ship_from_code")
    #
    #utils.manageOutput(logging, spark_session, otd_low_density_df, 0, "otd_low_density_df", target_db_name,
    #                   staging_location, debug_mode_ind, "_{}{}".format(target_db_name, debug_postfix))
    #
    #missing_low_density_df = \
    #    otd_low_density_df.join(
    #        otd_vfr_join2_df.select("origin_freight_code", "low_density_site_val")
    #            .filter("low_density_site_val is not null").distinct(),
    #        "origin_freight_code",
    #        how='inner'
    #    ).withColumnRenamed("low_density_site_val", "otd_low_density_site_val").drop("origin_freight_code")
    #
    #utils.manageOutput(logging, spark_session, missing_low_density_df, 1, "missing_low_density_df", target_db_name,
    #                   staging_location, debug_mode_ind, "_{}{}".format(target_db_name, debug_postfix))
    #
    #logging.info("Calculating additional Low Density Site from OTD data has finished.")

    logging.info("Calculating AVG data from join (2).")

    otd_vfr_join2_calc_df = otd_vfr_join2_df \
        .select("load_id", "max_net_weight_order_qty", "net_vol_fill_rate", "net_weight_order_qty", "net_vol_order_qty",
                "pallet_spot_qty")\
        .distinct()\
        .groupBy("load_id") \
        .agg(f.avg("max_net_weight_order_qty").alias("avg_max_weight_net_order"),
             f.avg("net_vol_fill_rate").alias("avg_net_vol_fill_rate"),
             f.avg("net_weight_order_qty").alias("avg_net_weight_order_qty"),
             f.avg("net_vol_order_qty").alias("avg_net_vol_order_qty"),
             f.sum("pallet_spot_qty").alias("sum_pallet_spot_qty")
             ) \
        .toDF("load_id", "avg_max_weight_net_order", "avg_net_vol_fill_rate", "avg_net_weight_order_qty",
              "avg_net_vol_order_qty", "sum_pallet_spot_qty")\
        .withColumn("net_density_order_qty", f.expr(expr.net_density_order_qty_expr))

    logging.info("Calculating AVG data from join (2) has finished.")

    logging.info("Joining data (3).")

    #    .join(missing_low_density_df, "load_id", how='left')\
    #    .withColumn("vfr_freight_type_code",
    #                f.coalesce(f.col("vfr_freight_type_code"), f.col("otd_vfr_freight_type_code"))) \
    #    .withColumn("low_density_site_val",
    #                f.coalesce(f.col("low_density_site_val"), f.col("otd_low_density_site_val"))) \
    #    .drop("otd_vfr_freight_type_code").drop("otd_low_density_site_val")\

    otd_vfr_join3_df = otd_vfr_join2_df.join(otd_vfr_join2_calc_df, "load_id", how='left') \
        .join(f.broadcast(origin_sf_dict_df),
              otd_vfr_join2_df.origin_zone_ship_from_code == origin_sf_dict_df.origin_sf_dict,
              how='left') \
        .withColumn("cases_impact_amt", f.expr(expr.cases_impact_amt_expr)) \
        .withColumn("for_max_net_vol_cal", f.expr(expr.for_max_net_vol_cal_expr)) \
        .withColumn("max_net_vol_qty", f.expr(expr.max_net_vol_qty_expr)) \
        .withColumn("combined_load_max_weight_qty", f.expr(expr.combined_load_max_weight_qty_expr).cast("double")) \
        .withColumn("calc_std_weight_qty", f.expr(expr.calc_std_weight_qty_expr)) \
        .withColumn("combined_load_max_vol_qty_old", f.expr(expr.combined_load_max_vol_qty_expr).cast("double")) \
        .withColumn("combined_load_max_vol_qty", f.lit("3533").cast("double")) \
        .withColumn("hopade_amt", f.expr(expr.hopade_amt_expr)) \
        .withColumn("hopade_amt_dec", f.col("hopade_amt").cast("decimal(38,6)")) \
        .withColumn("max_orders_incrmtl_amt", f.expr(expr.max_orders_incrmtl_amt_expr)) \
        .withColumn("glb_segment_impact_cat_ld_amt", f.expr(expr.glb_segment_impact_cat_ld_amt_expr)) \
        .withColumn("drf_last_truck_amt", f.expr(expr.drf_last_truck_amt_expr))\
        .withColumn("drf_last_truck_amt_dec", f.col("drf_last_truck_amt").cast("decimal(38,6)")) \
        .withColumn("cut_impact_rate", f.expr(expr.cut_impact_rate_expr)) \
        .withColumn("max_orders_non_drp_amt", f.expr(expr.max_orders_non_drp_amt_expr)) \
        .withColumn("max_orders_non_drp_amt_dec", f.col("max_orders_non_drp_amt").cast("decimal(38,6)")) \
        .withColumn("prod_density_gap_impact_amt", f.expr(expr.prod_density_gap_impact_amt_expr))\
        .withColumn("prod_density_gap_impact_pct", f.expr(expr.prod_density_gap_impact_pct_expr))\
        .withColumn("drf_last_truck_amt_dec", f.expr('CASE WHEN drf_last_truck_amt_dec < 0 THEN 0 ELSE drf_last_truck_amt_dec END'))\
        .withColumn("glb_segment_impact_cat_ld_amt", f.expr('CASE WHEN glb_segment_impact_cat_ld_amt < 0 THEN 0 ELSE glb_segment_impact_cat_ld_amt END'))\
        .withColumn("max_orders_incrmtl_amt", f.expr('CASE WHEN max_orders_incrmtl_amt < 0 THEN 0 ELSE max_orders_incrmtl_amt END')) \
        .withColumn("max_orders_non_drp_amt_dec", f.expr('CASE WHEN max_orders_non_drp_amt_dec < 0 THEN 0 ELSE max_orders_non_drp_amt_dec END'))\
        .withColumn("hopade_amt_dec", f.expr('CASE WHEN hopade_amt_dec < 0 THEN 0 ELSE hopade_amt_dec END')) \
        .withColumn("cut_impact_rate", f.expr('CASE WHEN cut_impact_rate < 0 THEN 0 ELSE cut_impact_rate END')) \

    logging.info("Joining data (3) has finished.")
    utils.manageOutput(logging, spark_session, otd_vfr_join3_df, 1, "otd_vfr_join3_df", target_db_name,
                       staging_location, debug_mode_ind, "_{}{}".format(target_db_name, debug_postfix))

    logging.info("Calculating max values from join (3).")

    otd_vfr_join3_calc_df = otd_vfr_join3_df.groupBy("load_gbu_id") \
        .agg(
             f.sum("max_orders_incrmtl_amt").alias("sum_max_orders_incrmtl_amt_"),
             f.sum("max_orders_non_drp_amt").alias("sum_max_orders_non_drp_amt_"),
             f.sum("hopade_amt").alias("sum_hopade_amt_"),
             f.sum("drf_last_truck_amt").alias("sum_drf_last_truck_amt_")
        ) \
        .toDF("load_gbu_id", "sum_max_orders_incrmtl_amt_",
              "sum_max_orders_non_drp_amt_", "sum_hopade_amt_", "sum_drf_last_truck_amt_")
        # .withColumnRenamed("load_gbu_id", "load_gbu_id2")

    logging.info("Calculating max values from join (3) has finished.")

    logging.info("Joining data (4).")

    # otd_vfr_join1_df
    otd_vfr_join4_df = otd_vfr_join3_df\
        .drop("oblb_gross_weight_plan_qty").drop("oblb_gross_weight_shipped_qty").drop("oblb_net_weight_plan_qty")\
        .drop("oblb_gross_vol_shipped_qty").drop("oblb_gross_vol_plan_qty").drop("oblb_net_vol_plan_qty")\
        .drop("net_vol_qty").drop("net_weight_qty") \
        .join(otd_vfr_join3_calc_df, "load_gbu_id", how='left') \
        .join(otd_vfr_df.drop("carr_id").drop("gbu_desc").drop("trans_plan_point_code").drop("gbu_code")
              .drop("order_num").drop("max_weight_qty").drop("max_vol_trans_mgmt_sys_qty")
              .drop("max_pallet_tms_trans_type_qty").drop("floor_position_qty").drop("actual_cost_amt")
              .drop("gross_weight_qty").drop("gross_vol_qty").drop("tdcval_desc")
              .drop("ship_to_party_desc").drop("ship_cond_val"),
              ["load_id", "load_gbu_id", "material_doc_num", "ship_to_party_id", "tdcval_code", "dlvry_item_num",
               "vfr_freight_type_code", "ship_cond_desc", "trans_stage_num", "dlvry_id"]) \
        .withColumn("total_vf_optny_amt", f.expr(expr.total_vf_opportunity_amt_expr))\
        .withColumn("last_update_utc_tmstp", f.to_utc_timestamp(f.from_unixtime(f.unix_timestamp()), 'PRT'))\
        .withColumn("weight_avg_qty", f.col("oblb_gross_weight_plan_qty")) \
        .withColumn("load_oblb_gross_weight_plan_qty", f.col("oblb_gross_weight_plan_qty")) \
        .withColumn("load_oblb_gross_vol_plan_qty", f.col("oblb_gross_vol_plan_qty"))\
        .withColumn("tac_freight_type_code", f.expr(expr.tac_freight_type_code_expr))\
        .withColumn("recvng_live_drop_code", f.expr(expr.recvng_live_drop_code_expr))\
        .withColumn("pre_load_type_code", f.expr(expr.pre_load_type_code_expr))
        #.withColumn("load_density_rate", f.col("density_rate"))

    logging.info("Joining data (4) has finished.")
    utils.manageOutput(logging, spark_session, otd_vfr_join4_df, 0, "otd_vfr_join4_df", target_db_name,
                       staging_location, debug_mode_ind, "_{}{}".format(target_db_name, debug_postfix))

    logging.info("INSERT data to vfr_data_hub_star.")

    #spark_session.conf.set('spark.sql.autoBroadcastJoinThreshold', -1)

    # Get target table column list and select data
    vfr_target_table_cols = spark_session.table('{}.vfr_data_hub_star'.format(target_db_name)).schema.fieldNames()
    otd_vfr_dh_select_df = otd_vfr_join4_df\
        .select(vfr_target_table_cols)
    insert_df_to_table(logging, otd_vfr_dh_select_df, target_db_name, 'vfr_data_hub_star')

    logging.info("INSERT data to vfr_data_hub_star has finished.")


def load_vfr_data_hub_star(
        logging, config_module, debug_mode_ind, debug_postfix):
    ''' Load the lot_star table'''
    # Temporary override
    #debug_mode_ind = 1

    params = utils.ConfParams.build_from_module(logging, config_module, debug_mode_ind, debug_postfix)
    #spark_params = utils.ConfParams.build_from_module(logging, config_spark_module, debug_mode_ind, debug_postfix)
    spark_conf = list(set().union(
        params.SPARK_GLOBAL_PARAMS,
        params.SPARK_PROD_OTD_PARAMS)
        )
    spark_session = utils.get_spark_session(logging, 'tfx_vfr', params.SPARK_MASTER, spark_conf)

    #Remove debug tables (if they are)
    utils.removeDebugTables(logging, spark_session, params.TARGET_DB_NAME, debug_mode_ind, debug_postfix)

    logging.info("Started loading {}.vfr_data_hub_star table.".format(params.TARGET_DB_NAME))

    # lot_star_df =
    get_vfr_data_hub_star(logging, spark_session, params.TRANS_VB_DB_NAME, params.RDS_DB_NAME, params.TARGET_DB_NAME,
                          params.STAGING_LOCATION, debug_mode_ind, debug_postfix)
