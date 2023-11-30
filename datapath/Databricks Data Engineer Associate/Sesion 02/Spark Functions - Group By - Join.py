# Databricks notebook source
simpleData = (("James", "Sales", 3000), \
    ("Michael", "Sales", 4600),  \
    ("Robert", "Sales", 4100),   \
    ("Maria", "Finance", 3000),  \
    ("James", "Sales", 3000),    \
    ("Scott", "Finance", 3300),  \
    ("Jen", "Finance", 3900),    \
    ("Jeff", "Marketing", 3000), \
    ("Kumar", "Marketing", 2000),\
    ("Saif", "Sales", 4100) \
  )
 
columns= ["employee_name", "department", "salary"]

df = spark.createDataFrame(data = simpleData, schema = columns)

df.printSchema()

display(df)

# COMMAND ----------

from pyspark.sql.functions import *


# COMMAND ----------

dfDiscount = df.withColumn("discount", col("salary") * 0.18)
display(dfDiscount)

# COMMAND ----------

dfValid = dfDiscount.withColumn("isTrue", when(col("discount") > 600, True).otherwise(False))
display(dfValid)

# COMMAND ----------

dfSelectedJustTrue = dfValid.where(col("isTrue") == True)
display(dfSelectedJustTrue)

# COMMAND ----------

dfSelect = dfSelectedJustTrue.select("employee_name", "salary","discount")
display(dfSelect)

# COMMAND ----------

dfSelect = dfSelectedJustTrue.select(col("employee_name").alias("Employee"), col("salary").alias("Salary"),"discount")
display(dfSelect)

# COMMAND ----------

#quitando duplicados

dfUniques = df.distinct() # Quitar duplicando validando todas columnas

display(dfUniques)

# COMMAND ----------

#quitando duplicados

dfUniques = df.drop_duplicates(["department"]) # Quitar duplicando validando la columna que se ponga entre el metodo y devuelve el primer valor encontrado

display(dfUniques)

# COMMAND ----------

#Empezemos agrupar usando el primer dataframe

display(df)

# COMMAND ----------

dfGroupBysimple = df.groupBy("department").agg(count("department"))
display(dfGroupBysimple)

# COMMAND ----------

dfGroupBysimple = df.groupBy("department").agg(collect_list("employee_name"))
display(dfGroupBysimple)

# COMMAND ----------

dfGroupBysimple = df.groupBy("department").agg(collect_set("employee_name"))
display(dfGroupBysimple)

# COMMAND ----------

#Rename la columna agrupada y ordenarlo por department

dfGroupBysimple = df.groupBy("department").agg(collect_set("employee_name").alias("Employees"))\
                    .orderBy("department")
display(dfGroupBysimple)

# COMMAND ----------

# MAGIC %md  
# MAGIC ## Join Tables
# MAGIC

# COMMAND ----------


#Creamos 2 DF para realizar un join por Inner 

emp = [(1,"Smith",-1,"2018","10","M",3000), \
    (2,"Rose",1,"2010","20","M",4000), \
    (3,"Williams",1,"2010","10","M",1000), \
    (4,"Jones",2,"2005","10","F",2000), \
    (5,"Brown",2,"2010","40","",-1), \
      (6,"Brown",2,"2010","50","",-1) \
  ]
empColumns = ["emp_id","name","superior_emp_id","year_joined", \
       "emp_dept_id","gender","salary"]

empDF = spark.createDataFrame(data=emp, schema = empColumns)
empDF.printSchema()
empDF.show(truncate=False)

dept = [("Finance",10), \
    ("Marketing",20), \
    ("Sales",30), \
    ("IT",40) \
  ]
deptColumns = ["dept_name","dept_id"]
deptDF = spark.createDataFrame(data=dept, schema = deptColumns)
deptDF.printSchema()
deptDF.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ![joins](/files/tmp/datapath/joins.PNG)

# COMMAND ----------

dfJoinInner = empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"inner") 

display(dfJoinInner)
    
