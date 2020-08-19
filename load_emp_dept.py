import sys
import uuid
from pyspark.sql.functions import *
from pyspark.sql.types import *
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

emp = glueContext.create_dynamic_frame.from_catalog(database = "default", table_name = "pjd", transformation_ctx = "emp")

dept = glueContext.create_dynamic_frame.from_catalog(database = "default", table_name = "pjd", transformation_ctx = "dept")

emp = emp.drop_fields(['salary_increment'])
dept = dept.drop_fields(['first_name','last_name','salary'])

uuidUdf= udf(lambda : str(uuid.uuid4()),StringType())

deptDf = dept.toDF().distinct()
deptDf1 = deptDf.withColumn("department_id", uuidUdf())
deptnew = DynamicFrame.fromDF(deptDf1, glueContext, "deptnew")

empDf = emp.toDF()
empDf1 = empDf.withColumn("employee_id", uuidUdf())
empnew = DynamicFrame.fromDF(empDf1, glueContext, "empnew")

empnew = Join.apply(empnew,deptnew,'dept_name','dept_name').drop_fields(['dept_name'])

empmapping = ApplyMapping.apply(frame = empnew, mappings = [("employee_id", "string", "id", "string") , ("first_name", "string", "first_name", "string"), ("last_name", "string", "last_name", "string"), ("salary", "long", "salary", "long"), ("department_id", "string", "department_id", "string")], transformation_ctx = "empmapping")

depmapping = ApplyMapping.apply(frame = deptnew, mappings = [("department_id", "string", "id", "string"), ("dept_name", "string", "name", "string"), ("salary_increment", "long", "salary_increment", "long")], transformation_ctx = "depmapping")

rc_emp = ResolveChoice.apply(frame = empmapping, choice = "make_cols", transformation_ctx = "rc_emp")

rc_dept = ResolveChoice.apply(frame = depmapping, choice = "make_cols", transformation_ctx = "rc_dept")

#rc_emp.printSchema()
#rc_emp.toDF().show(10)

#rc_dept.printSchema()
#rc_dept.toDF().show(10)

#dropnullfields1 = DropNullFields.apply(frame = rc_emp, transformation_ctx = "dropnullfields1")
#dropnullfields2 = DropNullFields.apply(frame = rc_dept, transformation_ctx = "dropnullfields2")


datasink_emp = glueContext.write_dynamic_frame.from_catalog(frame = rc_emp, database = "default", table_name = "pjd_db_employee", transformation_ctx = "datasink_emp")

datasink_dep = glueContext.write_dynamic_frame.from_catalog(frame = rc_dept, database = "default", table_name = "pjd_db_department", transformation_ctx = "datasink_dep")


job.commit()
