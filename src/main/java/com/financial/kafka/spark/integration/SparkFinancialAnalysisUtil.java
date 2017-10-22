package com.financial.kafka.spark.integration;

/**
 * Created by Aman on 10/14/2017.
 */
public class SparkFinancialAnalysisUtil {

    public static String cleanRecordField(String recordField){
        return recordField.replaceAll("[\"\"%]", "").trim();
    }
}
