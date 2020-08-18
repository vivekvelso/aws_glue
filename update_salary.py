import sys
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

employee  = glueContext.create_dynamic_frame.from_catalog(database = "default", table_name = "pjd_db_employee", transformation_ctx = "employee")

department  = glueContext.create_dynamic_frame.from_catalog(database = "default", table_name = "pjd_db_department", transformation_ctx = "department")

employee = employee.drop_fields(['first_name',
'last_name'])
       
department = department.drop_fields(['name']).rename_field(
    'id', 'department_id')

emp_join_dept = Join.apply(employee, department, 'department_id', 'department_id').drop_fields(['department_id'])

data_frame = emp_join_dept.toDF()
updated_salary = data_frame["salary"]  + data_frame["salary"] * data_frame["salary_increment"] / 100
data_frame = data_frame.withColumn("salary", updated_salary)

dynamic_frame_write = DynamicFrame.fromDF(data_frame, glueContext, "dynamic_frame_write")

       
applymapping1 = ApplyMapping.apply(frame = dynamic_frame_write, mappings = [("id", "string", "employee_id", "string"), ("salary", "decimal(10,0)", "updated_salary", "decimal(10,0)")], transformation_ctx = "applymapping1")

selectfields2 = SelectFields.apply(frame = applymapping1, paths = ["updated_salary", "employee_id"], transformation_ctx = "selectfields2")

resolvechoice3 = ResolveChoice.apply(frame = selectfields2, choice = "MATCH_CATALOG", database = "default", table_name = "pjd_db_updated_salaries", transformation_ctx = "resolvechoice3")

resolvechoice4 = ResolveChoice.apply(frame = resolvechoice3, choice = "make_cols", transformation_ctx = "resolvechoice4")

datasink5 = glueContext.write_dynamic_frame.from_catalog(frame = resolvechoice4, database = "default", table_name = "pjd_db_updated_salaries", transformation_ctx = "datasink5")
job.commit()
