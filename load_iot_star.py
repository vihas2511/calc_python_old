'''Function loading the carrier dashboard tables
'''

import pyspark.sql.functions as f
from pyspark.sql import Window
import utils
from get_src_data import get_transfix as tvb


def get_iot_star(logging, spark_session, trans_vsblt_db_name, target_db_name, staging_location, debug_mode_ind,
                 debug_postfix):

    csot_data_df = \
        tvb.get_csot_data(logging, spark_session, target_db_name, target_db_name, staging_location, debug_mode_ind,
                          debug_postfix)

    logging.info("Filter csot data.")

    iot_data_df = csot_data_df.where(csot_data_df.freight_type_val == "INTERPLANT")

    utils.manageOutput(logging, spark_session, iot_data_df, 0, "iot_data_df", target_db_name, staging_location,
                       debug_mode_ind, "_{}{}".format(target_db_name, debug_postfix))
    logging.info("Filter csot data has finished.")

    return iot_data_df


def load_iot_star(logging, config_module, debug_mode_ind, debug_postfix):
    ''' Load the iot_star table'''

    # Create a spark session
    params = utils.ConfParams.build_from_module(logging, config_module, debug_mode_ind, debug_postfix)
    #spark_params = utils.ConfParams.build_from_module(logging, config_spark_module, debug_mode_ind, debug_postfix)
    spark_conf = list(set().union(
        params.SPARK_GLOBAL_PARAMS,
        params.SPARK_TFX_PARAMS)
        )
    spark_session = utils.get_spark_session(logging, 'tfx_iot', params.SPARK_MASTER, spark_conf)

    #Remove debug tables (if they are)
    utils.removeDebugTables(logging, spark_session, params.TARGET_DB_NAME, debug_mode_ind, debug_postfix)

    logging.info("Started loading {}.iot_star table.".format(params.TARGET_DB_NAME))

    refresh_sql = """
    refresh table {}.csot_star""".format(params.TARGET_DB_NAME)

    refresh = spark_session.sql(refresh_sql)

    # Get target table column list
    target_table_cols = spark_session.table('{}.iot_star'.format(params.TARGET_DB_NAME)).schema.fieldNames()

    iot_star_df = get_iot_star(logging, spark_session, params.TRANS_VB_DB_NAME, params.TARGET_DB_NAME,
                               params.STAGING_LOCATION, debug_mode_ind, debug_postfix).select(target_table_cols)

    logging.info("Inserting data into a table (overwriting old data)")

    iot_star_df.write.insertInto(
        tableName='{}.{}'.format(params.TARGET_DB_NAME,
                                 'iot_star'),
        overwrite=True)
    logging.info("Loading {}.iot_star table has finished".format(params.TARGET_DB_NAME))
