package com.speckvonschmeck.spark.spark;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.column;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapRowTo;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.spark.connector.japi.rdd.CassandraJavaRDD;

public class backup {
	
	
	public final static String KAFKA_URL = System.getenv("KAFKA_URL") != null ? 
			System.getenv("KAFKA_URL")
			: "192.168.178.64:9092";
			
	public final static String KAFKA_TOPIC = System.getenv("KAFKA_TOPIC") != null ? 
			System.getenv("KAFKA_TOPIC")
			: "speckvonschmeck";
			
	public static void main(String[] args) throws Exception {

		
		SparkConf conf = new SparkConf().setAppName("speckvonschmeck").setMaster("local[1]").set("spark.cassandra.connection.host", "127.0.0.1");
		
		//JavaStreamingContext context = new JavaStreamingContext(conf, new Duration(2000));
		JavaSparkContext sc = new JavaSparkContext(conf);
		
        CassandraConnector connector = CassandraConnector.apply(sc.getConf());
        
        try (Session session = connector.openSession()) {
            session.execute("DROP KEYSPACE IF EXISTS alpha");
            session.execute("CREATE KEYSPACE ALPHA WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
            session.execute("CREATE TABLE SPECTRUM (id INT PRIMARY KEY, title TEXT, scans TEXT, pepmass TEXT, charge TEXT, rtinseconds TEXT, x LIST<INT>, y LIST<INT>)");
            //session.execute("CREATE TABLE SPECCOMPARE (id UUID PRIMARY KEY, product INT, price DECIMAL)");
        }
        
        //JavaRDD<Integer> pricesRDD = javaFunctions(sc).cassandraTable("test", "spectrum", mapColumnTo(Integer.class)).select("x");
       
        
        CassandraJavaRDD<Test> readrdd = javaFunctions(sc).cassandraTable("test", "spectrum", mapRowTo(Test.class)).select(
                column("id"),
                column("x"),
                column("y"));
        
        List<Test> objekte= Arrays.asList(new Test(4,4,5), new Test(5,3,5), new Test(6,4,5));
                
//       JavaRDD<Test> testrdd = sc.parallelize(objekte);
//       javaFunctions(testrdd).writerBuilder("test", "spectrum", mapToRow(Test.class)).saveToCassandra();
        javaFunctions(readrdd).writerBuilder("test", "spectrum1", mapToRow(Test.class)).saveToCassandra();

		
		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put("bootstrap.servers", KAFKA_URL);
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("group.id", "spectrumConsumer");
		kafkaParams.put("auto.offset.reset", "latest");
		kafkaParams.put("enable.auto.commit", false);

		Collection<String> topics = Arrays.asList(KAFKA_TOPIC);

//		final JavaInputDStream<ConsumerRecord<String, String>> dstream = KafkaUtils.createDirectStream(context,
//				LocationStrategies.PreferConsistent(),
//				ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));
//		dstream.map(new Function<ConsumerRecord<String, String>, Spectrum>() {
//
//			/**
//			 * 
//			 */
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public Spectrum call(ConsumerRecord<String, String> record) throws Exception {
//				return new Gson().fromJson(record.value(), Spectrum.class);
//			}
//
//		}).filter(new Function<Spectrum, Boolean>() {
//
//			/**
//			 * 
//			 */
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public Boolean call(Spectrum spectrum) throws Exception {
//				return spectrum.getData().get(0).getX()<=500;
//			}
//
//		}).foreachRDD(new VoidFunction<JavaRDD<Spectrum>>() {
//			
//			/**
//			 * 
//			 */
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public void call(JavaRDD<Spectrum> rdd) throws Exception {
//
//				rdd.foreach(new VoidFunction<Spectrum>() {
//
//					/**
//					 * 
//					 */
//					private static final long serialVersionUID = 1L;
//
//					@Override
//					public void call(Spectrum spectrum) throws Exception {
//						if (!rdd.isEmpty())
//						System.out.println("Spark Job received => " + spectrum);
//						
//					}
//				});
//				
//			}
//		});
//		
//
//		context.start();
//		try {
//			context.awaitTermination();
//		} catch (InterruptedException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		
		
	}	

}
