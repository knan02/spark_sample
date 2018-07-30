import os
import sys
import atexit
import datetime
import argparse
from gautham import *

_file_name = os.path.splitext(os.path.basename(__file__))[0] + ".py"

if __name__ == "__main__":
    from pyspark.sql import SparkSession
    from pyspark.sql.types import *
    import pyspark.sql.functions as F
    
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--requests", type = str, required=True, help = "input path for request logs")
    parser.add_argument("-m", "--requests_metadata", type = str, required=True, help = "input path for meta logs")
    parser.add_argument("-f", "--force", action="store_true", help = "overwrite output folder")
    parser.add_argument("-o", "--idemo__request", type = str, required=True, help = "output path for requests with demo")
    args = parser.parse_args()    
    
    spark = SparkSession.builder.appName("Request Log Fields Reduction " + args.idemo__request)\
        .enableHiveSupport()\
        .getOrCreate()

    spark.conf.set("mapreduce.fileoutputcommitter.algorithm.version", "2")
    
    # spark = SparkSession.builder.appName("Request Log Fields Reduction " + args.requests_metadata)\
    #     .config("spark.default.parallelism", 3600)\
    #     .config("spark.executor.memory", "36000m")\
    #     .config("spark.executor.instances", 90)\
    #     .config("spark.executor.cores", 10)\
    #     .config("spark.yarn.executor.memoryOverhead", "4096m")\
    #     .config("spark.yarn.driver.memoryOverhead", "2048m")\
    #     .config("spark.driver.cores",5)\
    #     .config("spark.driver.memory","12000m")\
    #     .config("spark.sql.pivotMaxValues", 20000)\
    #     .enableHiveSupport()\
    #     .getOrCreate()

    schema = StructType([StructField("request_time", StringType(), True),
        StructField("placement_id", IntegerType(), True),
        StructField("media_type", StringType(), True),
        StructField("ip", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("city", StringType(), True),
        StructField("zip", StringType(), True),
        StructField("dma", IntegerType(), True),
        StructField("state", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("birth_year", IntegerType(), True),
        StructField("consumer_id", StringType(), True),
        StructField("delivered_receipt_id", StringType(), True),
        StructField("zip_from_publisher", StringType(), True),
        StructField("zip_from_lat_lon", StringType(), True),
        StructField("zip_from_ip", StringType(), True),
        StructField("income", StringType(), True),
        StructField("country", StringType(), True),
        StructField("response_type", IntegerType(), True),
        StructField("ip_address_from_wifi", StringType(), True),
        StructField("user_agent", StringType(), True),
        StructField("mobile_device_os", StringType(), True),
        StructField("mobile_device_model", StringType(), True),
        StructField("mobile_device_os_version", StringType(), True),
        StructField("mobile_device_pointing_method", StringType(), True),
        StructField("street_name", StringType(), True),
        StructField("street_number", StringType(), True),
        StructField("radius", DoubleType(), True),
        StructField("keyword", StringType(), True),
        StructField("userid", StringType(), True),
        StructField("page_url", StringType(), True),
        StructField("mobile_device_brand", StringType(), True),
        StructField("carrier", StringType(), True),
        StructField("isp", StringType(), True),
        StructField("date_stamp_of_entry", StringType(), True),
        StructField("hour", IntegerType(), True),
        StructField("hours_since_epoch", IntegerType(), True),
        StructField("datekey", IntegerType(), True),
        StructField("request_id", StringType(), True),
        StructField("total_ad_serve_time", IntegerType(), True),
        StructField("consumer_lookup_time", IntegerType(), True),
        StructField("consumer_latlng_to_loc_lookup_time", IntegerType(), True),
        StructField("consumer_ip_to_loc_lookup_time", IntegerType(), True),
        StructField("timezone_offset", IntegerType(), True),
        StructField("consumer_id_method", IntegerType(), True),
        StructField("ate", StringType(), True),
        StructField("device_type", StringType(), True),
        StructField("bid_price_floor", DoubleType(), True),
        StructField("ad_width", IntegerType(), True),
        StructField("ad_height", IntegerType(), True),
        StructField("bid_id", StringType(), True),
        StructField("location_type", IntegerType(), True),
        StructField("app_id", StringType(), True),
        StructField("site_id", StringType(), True),
        StructField("mraid_version", IntegerType(), True),
        StructField("marketing_name", StringType(), True),
        StructField("device_brand_from_request", StringType(), True),
        StructField("device_model_from_request", StringType(), True),
        StructField("request_category", StringType(), True),
        StructField("request_source", IntegerType(), True),
        StructField("at_home_or_away", IntegerType(), True),
        StructField("iso_language", StringType(), True),
        StructField("video_max_duration", IntegerType(), True),
        StructField("video_min_duration", IntegerType(), True),
        StructField("clear_device_id", StringType(), True),
        StructField("assigned_household_id", StringType(), True),
        StructField("assigned_household_source", IntegerType(), True),
        StructField("assigned_household_dha", StringType(), True),
        StructField("boost_flag", IntegerType(), True),
        StructField("app_name", StringType(), True),
        StructField("site_name", StringType(), True),
        StructField("publisher_id", StringType(), True),
        StructField("publisher_name", StringType(), True),
        StructField("app_bundle", StringType(), True),
        StructField("store_url", StringType(), True),
        StructField("secure", IntegerType(), True)])
        
    request_log_filtered=spark.read.option("sep", "|")\
        .option("header", "True")\
        .schema(schema)\
        .csv(args.requests)\
        .where(  (F.col("request_source")==4) & (F.col("consumer_id_method")==150))\
        .select(F.unix_timestamp(F.col("request_time"), "yyyy-MM-dd HH:mm:ss.SSS ZZZZZ").alias("time_stamp"), 
            F.col("timezone_offset"), 
            F.col("consumer_id"), 
            F.col("gender"), 
            F.col("birth_year"), 
            F.col("latitude"), 
            F.col("longitude"), 
            F.col("location_type"), 
            F.col("ip"),
            F.col("request_id"),
            F.col("at_home_or_away"), 
            F.col("app_id"), 
            F.col("app_bundle"), 
            F.col("request_source")).cache()


    schema_meta_log = StructType([StructField("request_time", StringType(), True),
        StructField("request_id", StringType(), True),
        StructField("user_agent", StringType(), True),
        StructField("bid_id", StringType(), True),
        StructField("iab_category", StringType(), True)])

    meta_log=spark.read.option("sep", "|")\
                        .option("header", "True")\
                        .schema(schema_meta_log)\
                        .csv(args.requests_metadata)

    filtered_consumer_ids = request_log_filtered.select(F.col("consumer_id").alias("filtered_consumer_id"))\
                                                .groupby("filtered_consumer_id")\
                                                .agg(F.count(F.col("filtered_consumer_id")).alias("consumer_id_count"))\
                                                .filter(F.col("consumer_id_count") > 10000).cache()
                                                
    one_day_df= request_log_filtered.join(filtered_consumer_ids, request_log_filtered.consumer_id == filtered_consumer_ids.filtered_consumer_id, 'left_outer')\
                                    .where(F.col("filtered_consumer_id").isNull())\
                                    .join(meta_log.select(F.col("request_id"), F.col("iab_category")) ,["request_id"] , "left_outer")\
                                    .select(F.col("time_stamp"), 
                                            F.col("timezone_offset"), 
                                            F.col("consumer_id"), 
                                            F.col("gender"), 
                                            F.col("birth_year"), 
                                            F.col("latitude"), 
                                            F.col("longitude"), 
                                            F.col("location_type"), 
                                            F.col("ip"),
                                            F.col("request_id"),
                                            F.col("at_home_or_away"), 
                                            F.col("app_id"), 
                                            F.col("app_bundle"), 
                                            F.col("request_source"),
                                            F.col("iab_category"))\
                                    .repartition(3000,F.col("consumer_id"))\
                                    .sortWithinPartitions(F.col("consumer_id"), F.col("time_stamp"))
    
    mode = "overwrite" if args.force else None
    one_day_df.write.parquet(args.idemo__request.replace("s3://", "s3a://"), mode=mode)

def idemo_request(first_day, **kwargs):
    # python -c "from request_reduction import idemo_request_prod as r; r(dummy=0, cron=True)"
    ''' This job by default runs a daily job on request log '''
    check_set(kwargs,'dummy',False)
    check_set(kwargs,'cron',True)

    #res contains all the dependencies of the job | construct the job organizer and asks it to lock the job (if dummy=False)
    days = -1

    #check_set(kwargs,'reduce_machine_multiplier', 16)
    check_set(kwargs,'machine_multiplier',150)
    #check_set(kwargs,'outfile',False)
    #check_set(kwargs,'max_machines',500)
    check_set(kwargs,'overwrite',True) # Overwrite is highly recommended to be True for cron jobs.
    EMRRunnerBase.set_instance_info(kwargs, EMRRunnerBase.get_cheapest_instance_and_config('r3', kwargs.get('jobflow_id',None), machines=kwargs.get('_proc_machine_count',100)))

    if kwargs.get("overwrite"):
        extras = [['--arg',['--force']]]
    
    kwargs['tags'] = (lambda x: (x, x.update(kwargs.get('tags',{}))))({"product":"segments_demo"})[0]
 
    output_paths = { 'idemo/request':JobOrganizer.static_get_canonical_output('idemo/request',first_day) }
    # output_paths = {"idemo/request": "hdfs:///tmp/requests/"}

    class MyRunner(EMRRunnerBase):
        def get_input_paths(self, emr=True, small=True):
            if emr:
                return (
                    [

                        ['--'+proc.replace("/","__"), output_destination]
                        for proc, output_destination in output_paths.items()
                    ] +
                    super(MyRunner, self).get_input_paths(emr,small)
                    )
            else:
                return [
                    # Add new LOCAL input paths here
                    
                ] + super(MyRunner, self).get_input_paths(emr,small)
        def get_output_dir(self):
            return output_paths.values()

    
    obj = MyRunner(_file_name, 'idemo/request', first_day, days, log_sources=['requests', 'requests_metadata'], 
        extras=extras, project='inferred_demo', execution_mode = 'spark', **kwargs)
    obj.emr_spark()




def idemo_request_prod(**kwargs):
    # python -c "from request_reduction import idemo_request_prod as r; r(dummy=0, cron=True)"
    ''' This job by default runs a daily job on request log '''
    check_set(kwargs,'dummy',False)
    check_set(kwargs,'cron',True)

    #res contains all the dependencies of the job | construct the job organizer and asks it to lock the job (if dummy=False)
    res = JobOrganizer.get_dependent_run_data('idemo/request', ['date'], #default_proc = name of the job | date = the job only needs the date
                                              debug=not kwargs['cron'], #not kwargs['cron'],
                                              # Use this date to start, if no proc for this job found. 
                                              # For dependent jobs, override with a specific date that has predecessor data.
                                              first_day = yesterday(4), #Get data from 2 days ago (only if there is no state for the first time)
                                              job_organizer_params={'duration': 3*60*60, 'dummy': kwargs['dummy']}) #duration goal time
    #Checks if rob required
    if not res['_job_required']:
        return
    first_day = res['_this_proc']
    days = -1

    # check_set(kwargs,'reduce_machine_multiplier', 16)
    check_set(kwargs,'machine_multiplier',150)
    check_set(kwargs, 'min_machines', 150)
    #check_set(kwargs,'outfile',False)
    #check_set(kwargs,'max_machines',500)
    check_set(kwargs,'overwrite',True) # Overwrite is highly recommended to be True for cron jobs.
    EMRRunnerBase.set_instance_info(kwargs, EMRRunnerBase.get_cheapest_instance_and_config('r3', kwargs.get('jobflow_id',None), machines=res.get('_proc_machine_count',100)))

    extras = [['--arg',['--force']]]
    kwargs['tags'] = (lambda x: (x, x.update(kwargs.get('tags',{}))))({"product":"segments_demo"})[0]
 
    output_paths = { 'idemo/request':JobOrganizer.static_get_canonical_output('idemo/request',first_day)}
    # output_paths = {"idemo/request": "hdfs:///tmp/requests/"}

    class MyRunner(EMRRunnerBase):
        def get_input_paths(self, emr=True, small=True):
            if emr:
                return (
                    [
                        # Add additional EMR input paths here
                    ] +
                    [
                        # This section takes care of all the paths in res.
                        [ '--'+proc.replace("/","__"), JobOrganizer.static_get_canonical_output(proc, date) + '/part*' ]
                        for proc, date in res.items()
                        if not (proc.startswith('_') or proc == "date")
                    ] +
                    [

                        ['--'+proc.replace("/","__"), output_destination]
                        for proc, output_destination in output_paths.items()
                    ] +
                    super(MyRunner, self).get_input_paths(emr,small)
                    )
            else:
                return [
                    # Add new LOCAL input paths here
                    
                ] + super(MyRunner, self).get_input_paths(emr,small)
        def get_output_dir(self):
            return output_paths.values()

    
    obj = MyRunner(_file_name, 'idemo/request', first_day, days, log_sources=['requests', 'requests_metadata'],
             job_organizer=res['_job_organizer'], extras=extras, project='inferred_demo', execution_mode = 'spark',
             spark_configurations = "r3default", **kwargs)
    obj.emr_spark()


