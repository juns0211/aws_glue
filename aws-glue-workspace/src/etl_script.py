import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# S3來源建立成DynamicFrame格式, 如同Apache Spark 的 DataFrame,但具有更豐富的功能，特別適合於處理半結構化資料和結構化資料。
# Script generated for node promotion_item_mapping
promotion_item_mapping_node1722577680509 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://rainforest-data-pipeline/raw/product_center/data_center/promotion_item_mapping/LOAD00000001.csv"], "recurse": True}, transformation_ctx="promotion_item_mapping_node1722577680509")

# Script generated for node vendor_main
vendor_main_node1722581132534 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://rainforest-data-pipeline/raw/product_center/data_center/vendor_main/LOAD00000001.csv"], "recurse": True}, transformation_ctx="vendor_main_node1722581132534")

# Script generated for node new_item_file
new_item_file_node1722579019266 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://rainforest-data-pipeline/raw/product_center/data_center/new_item_file/LOAD00000001.csv"], "recurse": True}, transformation_ctx="new_item_file_node1722579019266")

# Script generated for node item
item_node1722578643577 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://rainforest-data-pipeline/raw/product_center/data_center/item/LOAD00000001.csv"], "recurse": True}, transformation_ctx="item_node1722578643577")

# Script generated for node new_item_fooddscribe
new_item_fooddscribe_node1722579272313 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://rainforest-data-pipeline/raw/product_center/data_center/new_item_fooddscribe/LOAD00000001.csv"], "recurse": True}, transformation_ctx="new_item_fooddscribe_node1722579272313")

# Script generated for node new_item_logistic
new_item_logistic_node1722580063307 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://rainforest-data-pipeline/raw/product_center/data_center/new_item_logistic/LOAD00000001.csv"], "recurse": True}, transformation_ctx="new_item_logistic_node1722580063307")

# Script generated for node promotion_main
promotion_main_node1722577646548 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://rainforest-data-pipeline/raw/product_center/data_center/promotion_main/LOAD00000001.csv"], "recurse": True}, transformation_ctx="promotion_main_node1722577646548")

# Script generated for node new_item
new_item_node1722578830265 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://rainforest-data-pipeline/raw/product_center/data_center/new_item/LOAD00000001.csv"], "recurse": True}, transformation_ctx="new_item_node1722578830265")

# 程式碼是使用 AWS Glue 來處理 ETL（提取、轉換、加載）流程
# Script generated for node pm_join
pm_join_node1722577708853 = Join.apply(frame1=promotion_main_node1722577646548, frame2=promotion_item_mapping_node1722577680509, keys1=["id"], keys2=["promotion_main_id"], transformation_ctx="pm_join_node1722577708853")

# Script generated for node pm_Drop Fields
pm_DropFields_node1722578244099 = DropFields.apply(frame=pm_join_node1722577708853, paths=["`.update_user`", "`.create_user`", "`.id`", "`.update_date`", "id", "`.create_date`"], transformation_ctx="pm_DropFields_node1722578244099")

# Script generated for node i_Join
item_node1722578643577DF = item_node1722578643577.toDF()
pm_DropFields_node1722578244099DF = pm_DropFields_node1722578244099.toDF()
i_Join_node1722578677632 = DynamicFrame.fromDF(item_node1722578643577DF.join(pm_DropFields_node1722578244099DF, (item_node1722578643577DF['id'] == pm_DropFields_node1722578244099DF['item_id']), "right"), glueContext, "i_Join_node1722578677632")

# Script generated for node i_Drop Fields
i_DropFields_node1722578767104 = DropFields.apply(frame=i_Join_node1722578677632, paths=["id"], transformation_ctx="i_DropFields_node1722578767104")

# Script generated for node ni_Join
new_item_node1722578830265DF = new_item_node1722578830265.toDF()
i_DropFields_node1722578767104DF = i_DropFields_node1722578767104.toDF()
ni_Join_node1722578870664 = DynamicFrame.fromDF(new_item_node1722578830265DF.join(i_DropFields_node1722578767104DF, (new_item_node1722578830265DF['id'] == i_DropFields_node1722578767104DF['new_item_id']), "right"), glueContext, "ni_Join_node1722578870664")

# Script generated for node ni_Drop Fields
ni_DropFields_node1722578955678 = DropFields.apply(frame=ni_Join_node1722578870664, paths=["id"], transformation_ctx="ni_DropFields_node1722578955678")

# Script generated for node nif_Join
new_item_file_node1722579019266DF = new_item_file_node1722579019266.toDF()
ni_DropFields_node1722578955678DF = ni_DropFields_node1722578955678.toDF()
nif_Join_node1722579050292 = DynamicFrame.fromDF(new_item_file_node1722579019266DF.join(ni_DropFields_node1722578955678DF, (new_item_file_node1722579019266DF['new_item_id'] == ni_DropFields_node1722578955678DF['new_item_id']), "right"), glueContext, "nif_Join_node1722579050292")

# Script generated for node nif_Drop Fields
nif_DropFields_node1722579115623 = DropFields.apply(frame=nif_Join_node1722579050292, paths=["id"], transformation_ctx="nif_DropFields_node1722579115623")

# Script generated for node nifo_Join
new_item_fooddscribe_node1722579272313DF = new_item_fooddscribe_node1722579272313.toDF()
nif_DropFields_node1722579115623DF = nif_DropFields_node1722579115623.toDF()
nifo_Join_node1722579339288 = DynamicFrame.fromDF(new_item_fooddscribe_node1722579272313DF.join(nif_DropFields_node1722579115623DF, (new_item_fooddscribe_node1722579272313DF['new_item_id'] == nif_DropFields_node1722579115623DF['new_item_id']), "right"), glueContext, "nifo_Join_node1722579339288")

# Script generated for node nifo_Drop Fields
nifo_DropFields_node1722579943788 = DropFields.apply(frame=nifo_Join_node1722579339288, paths=["id"], transformation_ctx="nifo_DropFields_node1722579943788")

# Script generated for node nil_Join
new_item_logistic_node1722580063307DF = new_item_logistic_node1722580063307.toDF()
nifo_DropFields_node1722579943788DF = nifo_DropFields_node1722579943788.toDF()
nil_Join_node1722580515815 = DynamicFrame.fromDF(new_item_logistic_node1722580063307DF.join(nifo_DropFields_node1722579943788DF, (new_item_logistic_node1722580063307DF['new_item_id'] == nifo_DropFields_node1722579943788DF['new_item_id']), "right"), glueContext, "nil_Join_node1722580515815")

# Script generated for node nil_Drop Fields
nil_DropFields_node1722580780881 = DropFields.apply(frame=nil_Join_node1722580515815, paths=["id"], transformation_ctx="nil_DropFields_node1722580780881")

# Script generated for node vm_Join
vendor_main_node1722581132534DF = vendor_main_node1722581132534.toDF()
nil_DropFields_node1722580780881DF = nil_DropFields_node1722580780881.toDF()
vm_Join_node1722581292407 = DynamicFrame.fromDF(vendor_main_node1722581132534DF.join(nil_DropFields_node1722580780881DF, (vendor_main_node1722581132534DF['id'] == nil_DropFields_node1722580780881DF['vendor_id']), "right"), glueContext, "vm_Join_node1722581292407")

# Script generated for node vm_Drop Fields
vm_DropFields_node1722581628420 = DropFields.apply(frame=vm_Join_node1722581292407, paths=["id"], transformation_ctx="vm_DropFields_node1722581628420")

# Script generated for node comsunption_S3
from datetime import datetime
now = datetime.now()
year = now.year
month = now.month
day = now.day
comsunption_S3_node1722583342407 = glueContext.write_dynamic_frame.from_options(frame=vm_DropFields_node1722581628420, connection_type="s3", format="glueparquet", connection_options={"path": f"s3://rainforest-data-pipeline/comsumption/product_center/year={year}/month={month}/day={day}", "partitionKeys": []}, format_options={"compression": "uncompressed"}, transformation_ctx="comsunption_S3_node1722583342407")

job.commit()