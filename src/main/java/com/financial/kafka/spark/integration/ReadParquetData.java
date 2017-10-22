package com.financial.kafka.spark.integration;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Created by Aman on 10/22/2017.
 */
public class ReadParquetData {

    public static  void main(String args[]){

        // Set Spark Configuration
        SparkConf sparkConf = new SparkConf().setAppName("spark-financial-analysis").setMaster("local[*]");
        sparkConf.set("spark.sql.parquet.compression.codec", "snappy");

        // Create Spark Session
        SparkSession session = SparkSession.builder().config(sparkConf).getOrCreate();

        Dataset<Row> aggregatedLoanDataParquet = session.read().parquet("/bigdata/loanStatFullDataset/*");

        aggregatedLoanDataParquet.show();
    }
}
