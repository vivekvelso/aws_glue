import sys
import uuid
from pyspark.sql.functions import *
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

emp = glueContext.create_dynamic_frame.from_catalog(database = "default", table_name = "pjd", transformation_ctx = "emp")

dept = glueContext.create_dynamic_frame.from_catalog(database = "default", table_name = "pjd", transformation_ctx = "dept")

emp.drop_fields(['salary_increment'])
dept = dept.drop_fields(['first_name','last_name','salary'])

uuidUdf= udf(lambda : str(uuid.uuid4()),StringType())

deptDf = dept.toDF().distinct()
deptDf1 = deptDf.withColumn("department_id", uuidUdf())
dept = DynamicFrame.fromDF(deptDf1, glueContext, "dept")

empDf = emp.toDF()
empDf1 = empDf.withColumn("employee_id", uuidUdf())
emp = DynamicFrame.fromDF(empDf1, glueContext, "emp")

emp = Join.apply(emp,dept,'dept_name','dept_name').drop_fields(['dept_name'])

empmapping = ApplyMapping.apply(frame = emp, mappings = [("employee_id", "string", "id", "string") , ("first_name", "string", "first_name", "string"), ("last_name", "string", "last_name", "string"), ("salary", "long", "salary", "long"), ("department_id", "string", "department_id", "string")], transformation_ctx = "empmapping")

depmapping = ApplyMapping.apply(frame = dept, mappings = [("department_id", "string", "id", "string"), ("dept_name", "string", "name", "string"), ("salary_increment", "long", "salary_increment", "long")], transformation_ctx = "depmapping")

rc_emp = ResolveChoice.apply(frame = empmapping, choice = "make_cols", transformation_ctx = "rc_emp")

rc_dept = ResolveChoice.apply(frame = depmapping, choice = "make_cols", transformation_ctx = "rc_dept")

dropnullfields1 = DropNullFields.apply(frame = rc_emp, transformation_ctx = "dropnullfields1")
dropnullfields2 = DropNullFields.apply(frame = rc_dept, transformation_ctx = "dropnullfields2")

datasink_emp = glueContext.write_dynamic_frame.from_jdbc_conf(frame = dropnullfields1, catalog_connection = "pdf_db_mysql", connection_options = {"dbtable": "pjd_db_employee", "database": "pjd_db"}, transformation_ctx = "datasink_emp")

datasink_dep = glueContext.write_dynamic_frame.from_jdbc_conf(frame = dropnullfields2, catalog_connection = "pdf_db_mysql", connection_options = {"dbtable": "pjd_db_department", "database": "pjd_db"}, transformation_ctx = "datasink_dep")

job.commit()
