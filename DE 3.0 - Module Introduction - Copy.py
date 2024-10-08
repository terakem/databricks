# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Manage Data with Delta Lake
# MAGIC This module is part of the Data Engineer Learning Path by Databricks Academy.
# MAGIC
# MAGIC #### Lessons
# MAGIC Lecture: What is Delta Lake <br>
# MAGIC [DE 3.1 - Schemas and Tables]($./DE 3.1 - Schemas and Tables) <br>
# MAGIC [DE 3.2 - Set Up Delta Tables]($./DE 3.2 - Set Up Delta Tables) <br>
# MAGIC [DE 3.3 - Load Data into Delta Lake]($./DE 3.3 - Load Data into Delta Lake) <br>
# MAGIC [DE 3.4 - Load Data Lab]($./DE 3.4L - Load Data Lab) <br>
# MAGIC [DE 3.5 - Version and Optimize Delta Tables]($./DE 3.5 - Version and Optimize Delta Tables) <br>
# MAGIC [DE 3.6 - Manipulate Delta Tables Lab]($./DE 3.6L - Manipulate Delta Tables Lab) <br>
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC #### Prerequisites
# MAGIC * Beginner familiarity with cloud computing concepts (virtual machines, object storage, etc.)
# MAGIC * Ability to perform basic code development tasks using the Databricks Data Engineering & Data Science workspace (create clusters, run code in notebooks, use basic notebook operations, import repos from git, etc)
# MAGIC * Beginning programming experience with Spark SQL
# MAGIC   * Extract data from a variety of file formats and data sources
# MAGIC   * Apply a number of common transformations to clean data
# MAGIC   * Reshape and manipulate complex data using advanced built-in functions
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2024 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the 
# MAGIC <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/">Support</a>