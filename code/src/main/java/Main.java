import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;
import scala.Tuple2;

import java.io.IOException;
import java.util.*;


public class Main {

    private static final org.apache.log4j.Logger LOGGER = org.apache.log4j.Logger.getLogger(Main.class);
    //DB Connection Settings
    final static String MYSQL_DRIVER = "com.mysql.jdbc.Driver";
    final static String MYSQL_USERNAME = "un";
    final static String MYSQL_PWD = "pw";
    final static String TABLE = "table";
    final static String DBHOST = "192.168.64.2";
    //Kafka Connection Settings
    final static String KAFKAHOST = "192.168.64.2";
    //Spark cluster settings
    final static String SPARKMASTER = "192.168.64.2";

    public static void main(String[] args) throws InterruptedException, IOException {

        //Read the io.lingk.streaming.poc.Recipe
        ClassLoader classLoader = Main.class.getClassLoader();

        final String MYSQL_CONNECTION_URL =
                "jdbc:mysql://" + DBHOST + "/studentdb?useLegacyDatetimeCode=false&serverTimezone=UTC";
        //"jdbc:mysql://"+sourceProvider.getProperties().get("host");

        final Properties connectionProperties = new Properties();
        connectionProperties.put("user", MYSQL_USERNAME);
        connectionProperties.put("password", MYSQL_PWD);

        SparkConf conf = new SparkConf().setMaster("spark://"+SPARKMASTER+":7077").setAppName("TransformerStreamPOC");
        conf.set("spark.driver.allowMultipleContexts", "true");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(60));

        Map<String, Object> kafkaParams = new HashMap<String, Object>();
        kafkaParams.put("bootstrap.servers", KAFKAHOST+":9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        Collection<String> topics = Arrays.asList("bcm");

        /*can be deleted - just to test kafka queue and flush after first read*/
        JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                        jssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
                );

        stream.mapToPair(record -> new Tuple2<>(record.key(), record.value()));

        //Actual Business Logic
        stream.foreachRDD(rdd -> {
            rdd.foreachPartition(partition -> {
            while (partition.hasNext()) {
                    ConsumerRecord<String, String> record = partition.next();
                    System.out.println(record.value()); //this is data received from API of IOT device
                }
            });
        });

        //Keep track of partitions being flushed from receiver
        stream.foreachRDD(rdd -> {
            OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
            rdd.foreachPartition(consumerRecords -> {
                OffsetRange o = offsetRanges[TaskContext.get().partitionId()];
                System.out.println(
                        o.topic() + " " +
                                o.partition() + " " +
                                o.fromOffset() + " " +
                                o.untilOffset());
            });
        });



        //persist kafka cursor for read streams
        stream.foreachRDD(rdd -> {
            OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
            ((CanCommitOffsets) stream.inputDStream()).commitAsync(offsetRanges);
        });

        jssc.start();
        jssc.awaitTermination();
    }

}
