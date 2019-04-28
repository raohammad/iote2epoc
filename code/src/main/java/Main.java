import com.fasterxml.jackson.databind.ObjectMapper;
import db.MySqlPoolableObjectFactory;
import domain.IOTRecord;
import exception.MySqlPoolableException;
import org.apache.commons.pool.ObjectPool;
import org.apache.commons.pool.PoolableObjectFactory;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.commons.pool.impl.GenericObjectPoolFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;
import scala.Tuple2;

import java.io.IOException;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;

/**
 * Created by hammadakhan on 27/04/2019.
 */

public class Main {

    //private static final org.apache.log4j.Logger LOGGER = org.apache.log4j.Logger.getLogger(Main.class);
    //DB Connection Settings
    final static String MYSQL_DRIVER = "com.mysql.jdbc.Driver";
    final static String MYSQL_USERNAME = "root";
    final static String MYSQL_PWD = "example";
    final static String TABLE = "bcm";
    final static String MYSQL_HOST = "192.168.64.2";
    //Kafka Connection Settings
    final static String KAFKAHOST = "192.168.64.2";
    //Spark cluster settings
    final static String SPARKMASTER = "192.168.64.2";

    public static void main(String[] args) throws InterruptedException, IOException {

        //Read the io.lingk.streaming.poc.Recipe
        ClassLoader classLoader = Main.class.getClassLoader();

//        final String MYSQL_CONNECTION_URL =
//                "jdbc:mysql://" + DBHOST + "/bcm?useLegacyDatetimeCode=false&serverTimezone=UTC";
        //"jdbc:mysql://"+sourceProvider.getProperties().get("host");

//        final Properties connectionProperties = new Properties();
//        connectionProperties.put("user", MYSQL_USERNAME);
//        connectionProperties.put("password", MYSQL_PWD);

        //SparkConf conf = new SparkConf().setAppName("bcm poc").setMaster("spark://"+SPARKMASTER+":7077");
        //conf.set("spark.driver.allowMultipleContexts", "true");
        SparkConf conf = new SparkConf().setAppName("bcm poc").setMaster("local[*]");
        conf.set("spark.driver.allowMultipleContexts", "true");

        JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(1000));

        //SQLContext sqlcontext = new org.apache.spark.sql.SQLContext(ssc.sparkContext())

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", KAFKAHOST+":9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", true);

        Collection<String> topics = Arrays.asList("bcm");

        JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                        ssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
                );

        stream.mapToPair(record -> new Tuple2<>(record.key(), record.value()));

        ObjectMapper mapper = new ObjectMapper();

        stream.foreachRDD(rdd->{

            rdd.foreachPartition(p->{
                ObjectPool pool = initMySqlConnectionPool(MYSQL_HOST,"3306","bcm",MYSQL_USERNAME,MYSQL_PWD);
                while (p.hasNext()) {
                    ConsumerRecord<String, String> record = p.next();

                    //SQLContext sqlContext = new SQLContext(sc);
                    IOTRecord iotRecord = mapper.readValue(record.value(), IOTRecord.class);
                    if(iotRecord.getCountry().equalsIgnoreCase("France")){

                        //System.out.println(record.value());
                        //1-find if record exists for this region
                        String sql = "select * from "+TABLE+" where region='"+findRegion(iotRecord.getLat(),iotRecord.getLon())+"' AND hour='"+iotRecord.getHour()+"'";
                        System.out.println(sql);
                        List<String> allRecords = new ArrayList();
                        Connection conn = null;
                        Statement st = null;
                        ResultSet res = null;

                        try {
                            conn = (Connection)pool.borrowObject();
                            st = conn.createStatement();
                            res = st.executeQuery(sql);
                            while (res.next()) {
                                String dbrec = (String.valueOf(res.getString(1))+","+res.getString(2) +","+ String.valueOf(res.getDouble(3))+","+String.valueOf(res.getDouble(4)));
                                //System.out.println(someRecord);
                                System.out.println(dbrec);
                                allRecords.add(dbrec);
                            }

                            if(allRecords.size()<1){
                                //its a new record for this hour for this region
                                String insertQuery =
                                        "INSERT INTO "+TABLE+"(region,hour, temperature,temperature3hoursbefore) VALUES ('"+findRegion(iotRecord.getLat(),iotRecord.getLon())
                                                +"','"+iotRecord.getHour()+"',"+iotRecord.getTemperature()+",10.33);";
                                //st = conn.prepareCall(insertQuery);
                                st.execute(insertQuery);

                            }else{ //means record exist for this hour
                                //IDF,2019-04-26 09,20.4694,10.33 [saved record in db]
                                double mean = (Double.valueOf(allRecords.get(0).split(",")[2])+iotRecord.getTemperature())/2;
                                String updateQuery = "UPDATE "+TABLE+" SET temperature="+mean+" WHERE region='"+findRegion(iotRecord.getLat(),iotRecord.getLon())+"' AND hour='"+iotRecord.getHour()+"'";
                                st.execute(updateQuery);
                            }

                            //irrespective check if 3hours ago data exist
                            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                            Date dateString=simpleDateFormat.parse(iotRecord.getTimestamp());
                            Calendar cal = Calendar.getInstance();
                            cal.setTime(dateString);
                            //System.out.println(simpleDateFormat.format(cal.getTime()));
                            cal.add(Calendar.HOUR_OF_DAY, -3);
                            //System.out.println(simpleDateFormat.format(cal.getTime()));

                            ResultSet threeHrsAgo = st.executeQuery("select * from "+TABLE+" where region='"+findRegion(iotRecord.getLat(),iotRecord.getLon())+"' AND hour='"+simpleDateFormat.format(cal.getTime()).substring(0,13)+"'");
                            while (threeHrsAgo.next()){
                                //if exist, just update the current record with 3hrs ago temperature field of resultset
                                st.execute("UPDATE "+TABLE+" SET temperature3hoursbefore="+String.valueOf(res.getDouble(4))+" WHERE region='"+findRegion(iotRecord.getLat(),iotRecord.getLon())+"' AND hour='"+iotRecord.getHour()+"'");
                            }

                        } catch (SQLException e) {
                            throw e;
                        }  catch (Exception e) {
                            throw new MySqlPoolableException("Failed to borrow connection from the pool", e);
                        } finally {
                            if(res!=null)
                                res.close();
                            if(st!=null)
                                st.close();
                            pool.returnObject(conn);
                        }

//                        //sparksql Read the table
//                        Dataset<Row> df = sqlContext
//                                .read()
//                                .jdbc(MYSQL_CONNECTION_URL,
//                                "bcm",
//                                connectionProperties);
//                        df.show();
//                        df.printSchema();
//                        Dataset<Row> appendSql = sqlContext.sql("INSERT INTO bcm VALUES('region' , 'hour', 21.4834 , -15)");


                    }
                }
            });
        });

        //persist kafka cursor for read streams
        stream.foreachRDD(rdd -> {
            OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
            ((CanCommitOffsets) stream.inputDStream()).commitAsync(offsetRanges);
        });
        //run forever
        ssc.start();
        ssc.awaitTermination();
    }

    private static ObjectPool initMySqlConnectionPool(String host, String port, String schema, String user, String password) throws IOException {

        PoolableObjectFactory mySqlPoolableObjectFactory = new MySqlPoolableObjectFactory(host,
                Integer.parseInt(port), schema, user, password);
        GenericObjectPool.Config config = new GenericObjectPool.Config();
        config.maxActive = 10;
        config.testOnBorrow = true;
        config.testWhileIdle = true;
        config.timeBetweenEvictionRunsMillis = 10000;
        config.minEvictableIdleTimeMillis = 60000;

        GenericObjectPoolFactory genericObjectPoolFactory = new GenericObjectPoolFactory(mySqlPoolableObjectFactory, config);
        ObjectPool pool = genericObjectPoolFactory.createPool();
        return pool;
    }

    private static String findRegion(double lat, double lon){
        System.out.println("Lat:"+lat+" Lon:"+lon);

        return "IDF";
    }

}
