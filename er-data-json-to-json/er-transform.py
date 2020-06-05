import sys
import time
import datetime
import json
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

## 每天凌晨 00:30 开始执行 ETL
## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

## formate date & path , get /year/month/day 
inputpath = "s3://preventech-core-chinastage/enriched-data"
outputpath = "s3://preventech-china-er-output"
yesterday = datetime.datetime.now() + datetime.timedelta(days=-1)
year = yesterday.strftime("%Y")
month = yesterday.strftime("%m")
day = yesterday.strftime("%d")
# year = "2020"
# month = "06"
# day = "01"

datepath = "/{}/{}/{}".format(year, month, day)
inputpath = inputpath + datepath
## database source
jdbcurl = "jdbc:postgresql://preventech-cn-stage.cmusx9j6p12i.rds.cn-north-1.amazonaws.com.cn:5432/da_nxg_chinadev"
dbusernam = "DA_NGCA_DEV_APP"
dbpassword = "DA_NGCA_DEV_APP_123"
grouptable = "master_data.groups"
customertable = "master_data.customer"

print("===================== start ETL step 1 =====================")
print(" ER json data: " + inputpath)
## DataSource  原始json 数据
erjsondata = glueContext.create_dynamic_frame_from_options("s3", {'paths': [inputpath], 'recurse':True},  format="json", transformation_ctx = "erjsondata")
## group & subgroup 数据
groupdata = glueContext.create_dynamic_frame_from_options("postgresql", {'url': jdbcurl, 'user': dbusernam, 'password': dbpassword, 'dbtable': grouptable}, transformation_ctx = "groupdata")
## customer 数据
customerdata = glueContext.create_dynamic_frame_from_options("postgresql", {'url': jdbcurl, 'user': dbusernam, 'password': dbpassword, 'dbtable': customertable}, transformation_ctx = "customerdata")

print("===================== mapping json data =====================")
##  mapping json data
erjsondatamapping = ApplyMapping.apply(frame = erjsondata, mappings = [("messageformatversion", "string", "messageformatversion", "string"), ("telematicspartnername", "string", "telematicspartnername", "string"), ("customerreference", "string", "customerreference", "string"), ("componentserialnumber", "string", "componentserialnumber", "string"), ("equipmentid", "string", "equipmentid", "string"), ("vin", "string", "vin", "string"), ("telematicsdeviceid", "string", "telematicsdeviceid", "string"), ("additionalsourceids", "array", "additionalsourceids", "array"), ("datasamplingconfigid", "string", "datasamplingconfigid", "string"), ("dataencryptionschemeid", "string", "dataencryptionschemeid", "string"), ("numberofsamples", "int", "numberofsamples", "string"), ("samples", "array", "samples", "string"), ("rel_smn", "string", "rel_smn", "string"), ("rel_vin", "string", "rel_vin", "string"), ("comp_type", "string", "comp_type", "string"), ("in_serv_loc", "int", "in_serv_loc", "int"), ("inactive_suppress_time", "int", "inactive_suppress_time", "int"), ("fuel_rating", "int", "fuel_rating", "string"), ("nominal_torque", "string", "nominal_torque", "string"), ("horse_power", "double", "horse_power", "string"), ("mode_counter", "int", "mode_counter", "string"), ("frame_mode", "string", "frame_mode", "string"), ("oper_mode", "string", "oper_mode", "string"), ("ecm_code", "string", "ecm_code", "string"), ("ecm_rev", "string", "ecm_rev", "string"), ("group_ids", "array", "group_ids", "array"), ("sub_group_ids", "array", "sub_group_ids", "array"), ("cust_id", "int", "cust_id", "int"), ("region", "string", "region", "string"), ("cpl", "string", "cpl", "string"), ("appl_code", "string", "appl_code", "string"), ("frm_rcvd_ts", "string", "frm_rcvd_ts", "string"), ("frm_gen_ts", "string", "frm_gen_ts", "string"), ("comm_ts", "string", "comm_ts", "string"), ("enrich_process_end_ts", "string", "enrich_process_end_ts", "string"), ("dbequipid", "int", "dbequipid", "int"), ("uuid", "string", "uuid", "string"), ("totalfuelconsumption", "string", "totalfuelconsumption", "string"), ("totalenginehour", "string", "totalenginehour", "string"), ("packageid", "string", "packageid", "string"), ("is_data_replay", "string", "is_data_replay", "string"), ("cust_equip_id", "string", "cust_equip_id", "string"), ("equip_market", "string", "equip_market", "string"), ("automated_email_flag", "string", "automated_email_flag", "string")], transformation_ctx = "erjsondatamapping")

print("===================== get frame1 data =====================")
## Filter get frame 1 data
def frame1_filter(dynamicRecord):
	return "Periodic1" in dynamicRecord["datasamplingconfigid"]

frame1data = Filter.apply(frame = erjsondatamapping, f = frame1_filter, transformation_ctx = "frame1data")

print("===================== get group subgroup calibration =====================")
## transform data ：
## 提取 group_id, subgroup_id
def get_group_subgroup(dynamicRecord):      
    dynamicRecord["group_id"] = dynamicRecord["group_ids"][0]
    dynamicRecord["subgroup_id"] = dynamicRecord["sub_group_ids"][0]
    dynamicRecord["calibration"] = dynamicRecord["ecm_code"] + "." + dynamicRecord["ecm_rev"]
    return dynamicRecord

mapeddata = Map.apply(frame = frame1data, f = get_group_subgroup, transformation_ctx = "mapeddata")

print("===================== group by customer smn equipmentid =====================")
## join group & subgroup & customer
groupdata = groupdata.select_fields(["group_id", "group_name"])
customerdata = customerdata.select_fields(["customer_id", "customer_account_name"])
group_df = groupdata.toDF()
customer_df = customerdata.toDF()
erdata_df = mapeddata.toDF()

group_df.createOrReplaceTempView("groups")
customer_df.createOrReplaceTempView("customers")
erdata_df.createOrReplaceTempView("erdata")

## group by customer_account_name, rel_smn, subgroup_id, componentserialnumber
# groupby_sql = "select ROW_NUMBER() OVER(order by equipmentid) as groupnum, tmp.* from (select c.customer_account_name as custname, rel_smn, equipmentid from erdata join customers c on erdata.cust_id=c.customer_id group by c.customer_account_name, rel_smn, equipmentid) tmp"
groupby_sql = "select c.customer_account_name as custname, rel_smn, equipmentid from erdata join customers c on erdata.cust_id=c.customer_id group by c.customer_account_name, rel_smn, equipmentid"

grouplist = spark.sql(groupby_sql).collect()

for row in grouplist:
    # row = grouplist.index(i)
    # print(row)
    cust = row['custname']
    smn = row['rel_smn']
    equip = row['equipmentid']
    group_sql = "select c.customer_account_name as custname, g.group_name as group_name, sg.group_name as subgroup_name, erdata.subgroup_id, rel_smn, componentserialnumber, vin, calibration, nominal_torque, fuel_rating, telematicsdeviceid, horse_power, frm_gen_ts, equipmentid, samples from erdata join customers c on erdata.cust_id=c.customer_id join groups g on erdata.group_id=g.group_id join groups sg on erdata.subgroup_id=sg.group_id"
    group_sql += " where c.customer_account_name ='" + cust + "' and rel_smn ='" + smn + "' and equipmentid ='" + equip +"'"
    group_sql += " order by frm_gen_ts "
    one_group_df = spark.sql(group_sql)
    one_part = one_group_df.coalesce(1)
    one_group = DynamicFrame.fromDF(one_part, glueContext, "one_group")
    # one_group.repartition(1)
    tempoutputpath = outputpath + "/" + cust + "/" + smn.replace(" ", "_") + "/" + equip + "/" + year + "/" + month + "/" + day
    print(">>>>>>>>>>>>>>>>>>>>> outputPath: [" + tempoutputpath + "]")
    glueContext.write_dynamic_frame.from_options(frame = one_group, connection_type = "s3", connection_options = {"path": tempoutputpath}, format = "json" )

print("===================== end ETL step 1 =====================")
job.commit()
