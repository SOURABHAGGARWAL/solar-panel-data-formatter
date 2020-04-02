package com.renew_power.solar_panel_data_formatter

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col,date_format,unix_timestamp}
import com.renew_power.solar_panel_data_formatter.context.SparkContext
import org.apache.spark.sql.types.{TimestampType,DateType,DoubleType,StructType,StructField,StringType}

/**
 * Class used to convert the JSON data to tabular/table data.
 * @created By Sourabh Aggarwal on 31-MARCH-2020.
 */
object JSONToTableConvertor extends SparkContext {
  val BASE_PATH = "/Users/sourabhaggarwal/Desktop/software/UBA-workspace/wind-mill-application/data_files/";

  val schema = StructType(
      List(StructField("timestamp", TimestampType, nullable = false),
        StructField("power.ac", StringType, nullable = false),
        StructField("voltage.ac.ab", StringType, nullable = false),
        StructField("current.ac", StringType, nullable = false)))
        
        
  def main(args: Array[String]) {
    
    if (args.length < 1) {
      val BASE_PATH = args(0)
      println("Please provide the path where input file is there, output csv is created on that perticular path.")
      System.exit(1)
    }
    
    //Read file and create RDD and formatting the timestamp in the required format.
    val solarPanelJsonData = sparkSession.read.schema(schema).json(BASE_PATH + "sample_input_test")
                             .withColumn("timestamp", date_format(col("timestamp"),"yyyy-MM-dd'T'HH:mm:ss"))

    // registering the json data as the temp view for quering.
    solarPanelJsonData.createOrReplaceTempView("solarPanelJsonData")

    var solarPanelTable: DataFrame = sparkSession.sql("select * from solarPanelJsonData")

    // Writing the data in the table format any of the source right now we are saving it to csv format.
    solarPanelTable.coalesce(1).write.option("header", "true").mode("overwrite").format("com.databricks.spark.csv").save(BASE_PATH + "outputPath")

    //printing the tabler data also in the logs
    solarPanelTable.show()
    
    // stopping the spark context to ensure the faith full stopping of context once the work is over.
    sparkSession.stop()
  }
}