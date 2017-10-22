package com.financial.kafka.spark.integration;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Aman on 10/15/2017.
 */
public class SparkFinancialAnalysisStreamingMain {

    private static String kafkaBrokerEndpoint = null;
    private static String loanDataIngestTopic = null;

    public static void main(String args[]) {

        // Read command Line Arguments for Kafka broker and Topic for consuming the Loan Records
        if (args != null) {

            kafkaBrokerEndpoint = args[0];
            loanDataIngestTopic = args[1];
        }

        // Set Spark Configuration
        SparkConf sparkConf = new SparkConf().setAppName("spark-financial-analysis").setMaster("local[*]");
        sparkConf.set("spark.sql.parquet.compression.codec", "snappy");

        SparkFinancialAnalysisStreamingMain sparkStreamingFinMain = new SparkFinancialAnalysisStreamingMain();

        // Perform aggregation on the Streaming Data
        sparkStreamingFinMain.aggregateOnKafkaStreamingData(sparkConf);


    }

    /**
     * Reads the Streaming data from Kafka with a batch interval of 20 seconds and converts it into a Dataset for running Spark SQL queries
     *
     * @param conf
     */
    public void aggregateOnKafkaStreamingData(SparkConf conf) {

        // Creates Java Streaming Context with a batch size of 20 seconds
        JavaStreamingContext sparkStreamingContext = new JavaStreamingContext(conf, Durations.seconds(20));

        // Creates a Discrete Stream by polling the loan data records on the Kafka Topic
        JavaInputDStream<ConsumerRecord<String, String>> loanConsumerRecordStream = KafkaUtils.createDirectStream(
                sparkStreamingContext,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String, String>Subscribe(Arrays.asList(loanDataIngestTopic), getKafkaConfiguration())
        );

        // Counter for the non-empty batch number
        AtomicInteger batchNumber = new AtomicInteger(0);

        // Convert the streams of JavaRDD of ConsumerRecord to JavaRDD of Java Beans
        loanConsumerRecordStream.foreachRDD((JavaRDD<ConsumerRecord<String, String>> loanConsumerRecordRDD) -> {

            // Flag to indicate if at least some records have been received and aggregated
            boolean isAggregatedDataAvailable = false;

            JavaRDD<LoanDataRecord> loanDataRecordRDD = loanConsumerRecordRDD.map((loanConsumerRecord) -> {

                LoanDataRecord loanDataRecord = new LoanDataRecord();

                String loanRecord = loanConsumerRecord.value().toString();

                if (!(loanRecord.isEmpty() || loanRecord.contains("member_id") || loanRecord.contains("Total amount funded in policy code"))) {

                    // Few records have emp_title with comma separated values resulting in records getting rejected. Cleaning the data before creating Dataset
                    String updatedLine = loanRecord.replace(", ", "|").replaceAll("[a-z],", "");

                    String loanRecordSplits[] = updatedLine.split(",\"");

                    loanDataRecord.setLoanAmt(SparkFinancialAnalysisUtil.cleanRecordField(loanRecordSplits[2]));
                    loanDataRecord.setFundedAmt(SparkFinancialAnalysisUtil.cleanRecordField(loanRecordSplits[3]));
                    loanDataRecord.setTerm(SparkFinancialAnalysisUtil.cleanRecordField(loanRecordSplits[5]));
                    loanDataRecord.setIntRate(SparkFinancialAnalysisUtil.cleanRecordField(loanRecordSplits[6]));
                    loanDataRecord.setGrade(SparkFinancialAnalysisUtil.cleanRecordField(loanRecordSplits[8]));
                    loanDataRecord.setHomeOwnership(SparkFinancialAnalysisUtil.cleanRecordField(loanRecordSplits[12]));
                    loanDataRecord.setAnnualIncome(SparkFinancialAnalysisUtil.cleanRecordField(loanRecordSplits[13]));
                    loanDataRecord.setAddressState(SparkFinancialAnalysisUtil.cleanRecordField(loanRecordSplits[23]));
                    loanDataRecord.setPolicyCode(SparkFinancialAnalysisUtil.cleanRecordField(loanRecordSplits[51]));
                } else {
                    System.out.println("Invalid Record line " + loanRecord);
                }

                return loanDataRecord;

            }).filter(record -> record.getFundedAmt() != null);

            // Create Spark Session
            SparkSession session = SparkSession.builder().config(conf).getOrCreate();

            // Create a Dataset of Rows (Dataframe) from the RDD of LoanDataRecord
            Dataset<Row> loanStatFullDataset = session.createDataFrame(loanDataRecordRDD, LoanDataRecord.class);

            //loanStatFullDataset.show();

            // Is data available in the Stream, save the data in Parquet format for future use. Also, calculate the total
            // funding amount for a given state

            if (!loanStatFullDataset.rdd().isEmpty()) {

                loanStatFullDataset.write().mode(SaveMode.Append).parquet("/bigdata/loanStatFullDataset" + "/batch_" + batchNumber.get());

                isAggregatedDataAvailable = true;

                System.out.println("Non Empty batch Number --> " + batchNumber.incrementAndGet());

                System.out.println("Streaming Financial Statistics Record count in current batch " + loanStatFullDataset.count());

                // Calculate the total funding amount for a given State in current RDD of streaming records
                List<String> fundedAmountsForState = loanStatFullDataset.javaRDD().filter(row -> "IL".equalsIgnoreCase(row.getString(0))).map(row -> row.getString(2)).collect();

                String totalFundedAmountForState = fundedAmountsForState.stream().reduce((x, y) -> Double.toString(Double.parseDouble(x) + Double.parseDouble(y))).get();

                System.out.println("Total Amount funded by Lending Club in IL State in current batch : $" + new BigDecimal(totalFundedAmountForState).toPlainString());
            }

            try {

                // Check if the aggregated data is available in the parquet file
                if (isAggregatedDataAvailable) {

                    // Read loan data stats from Parquet Files for each micro-batch and combine them to calculate aggregated results
                    Dataset<Row> aggregatedLoanDataParquet = session.read().parquet("/bigdata/loanStatFullDataset/*");

                    if (!aggregatedLoanDataParquet.rdd().isEmpty()) {

                        System.out.println("Streaming Financial Statistics aggregated Record count " + aggregatedLoanDataParquet.count());

                        // Calculate the total funding amount for a given State in current file of streaming records
                        List<String> fundedAmountsForStateAgg = aggregatedLoanDataParquet.javaRDD().filter(row -> "IL".equalsIgnoreCase(row.getString(0))).map(row -> row.getString(2)).collect();

                        String totalFundedAmountForStateAgg = fundedAmountsForStateAgg.stream().reduce((x, y) -> Double.toString(Double.parseDouble(x) + Double.parseDouble(y))).get();

                        System.out.println("Aggregated Total Amount funded by Lending Club in IL State : $" + new BigDecimal(totalFundedAmountForStateAgg).toPlainString());
                    }
                }
            } catch (Exception ex) {
                System.out.println("Error while processing the aggregated data");
                ex.printStackTrace();
            }
        });

        try {
            sparkStreamingContext.start();
            sparkStreamingContext.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * Gets the Configuration for making connection to Kafka
     * @return
     */
    private Map<String, Object> getKafkaConfiguration() {

        Map<String, Object> kafkaConfig = new HashMap();

        kafkaConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokerEndpoint);
        kafkaConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "loan-data-spark-streaming-ingest");
        kafkaConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        kafkaConfig.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        return kafkaConfig;
    }
}
