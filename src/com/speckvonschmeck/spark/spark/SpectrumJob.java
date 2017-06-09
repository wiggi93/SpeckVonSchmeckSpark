package com.speckvonschmeck.spark.spark;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapRowTo;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.slf4j.Logger;
import org.slf4j.impl.Log4jLoggerFactory;

import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.CassandraRow;
import com.google.gson.Gson;
import com.speckvonschmeck.spark.cassandra.CassandraTester;
import com.speckvonschmeck.spark.models.Spectrum;

public class SpectrumJob {
	
	
	public final static String KAFKA_URL = System.getenv("KAFKA_URL") != null ? 
			System.getenv("KAFKA_URL")
			: "192.168.178.27:9092";//64
			
	public final static String CASSANDRA_URL = "localhost";//64
			
	public final static String KAFKA_TOPIC = System.getenv("KAFKA_TOPIC") != null ? 
			System.getenv("KAFKA_TOPIC")
			: "speckvonschmeck";
			
	public static void main(String[] args) throws Exception {

		Logger log = new Log4jLoggerFactory().getLogger("");
				
		SparkConf conf = new SparkConf().setAppName("speckvonschmeck").setMaster("local[4]").set("spark.cassandra.connection.host", CASSANDRA_URL);
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaStreamingContext context = new JavaStreamingContext(sc, new Duration(1000));
		
		
        CassandraConnector connector = CassandraConnector.apply(conf);
        
//        try (Session session = connector.openSession()) {
//            session.execute("DROP KEYSPACE IF EXISTS BETA");
//            session.execute("CREATE KEYSPACE BETA WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
//            session.execute("CREATE TABLE BETA.SPECTRUM (title TEXT PRIMARY KEY, scans TEXT, pepmass TEXT, charge TEXT, rtinseconds TEXT, x LIST<INT>, y LIST<INT>)");
//        }
        
        
        List<Spectrum> spectra = CassandraTester.generateSpectraFromSpectrum();
		
//		JavaRDD<Spectrum> rdd2 = (sc).parallelize(spectra);
//		javaFunctions(rdd2).writerBuilder("alpha", "spectrum", mapToRow(Spectrum.class)).saveToCassandra();	
        
		JavaRDD<Spectrum> personRdd = CassandraJavaUtil.javaFunctions(sc)
                .cassandraTable("alpha", "spectrum", mapRowTo(Spectrum.class));
		javaFunctions(personRdd).writerBuilder("beta", "spectrum", mapToRow(Spectrum.class)).saveToCassandra();	
		
        
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
//		
//	    dstream.map(new Function<ConsumerRecord<String, String>, Spectrum>() {
//
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public Spectrum call(ConsumerRecord<String, String> record) throws Exception {
//				log.warn("-----------HALLLOOOOOOOOOO----------");
//				return new Gson().fromJson(record.value(), Spectrum.class);
//			}
//
//		}).foreachRDD(new VoidFunction<JavaRDD<Spectrum>>(){
//
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public void call(JavaRDD<Spectrum> rdd) throws Exception {
//				// TODO Auto-generated method stub
//				log.warn("-----------HALLLOOOOOOOOOO----------");
//				if(rdd!=null){
//					log.warn(rdd.toDebugString());
//					log.warn(String.valueOf(rdd.count()));
//			        //javaFunctions(rdd).writerBuilder("alpha", "spectrum", mapToRow(Spectrum.class)).saveToCassandra();			
//					rdd.foreachAsync(new VoidFunction<Spectrum>() {
//
//						private static final long serialVersionUID = 1L;
//
//						@Override
//						public void call(Spectrum spectrum) throws Exception {
//							// TODO Auto-generated method stub
//					        
//					        List<Spectrum> spectra = CassandraTester.generateSpectraFromSpectrum(spectrum);
//							
//							JavaRDD<Spectrum> rdd2 = (sc).parallelize(spectra);
//							javaFunctions(rdd2).writerBuilder("alpha", "spectrum", mapToRow(Spectrum.class)).saveToCassandra();							
//					        
//							
//							log.warn("-----------HALLLOOOOOOOOOO----------");
//							log.warn(spectrum.toString());
//							
//							
//						}
//					});
//				}
//				
//			}
//	    	
//	    });
//        
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
//
//
///*
// * noch nicht getestet: treffen mit roman am 09.06.17
// */
//JavaDStream<String> stream1 = dstream.transform(
//	      // Make sure you can get offset ranges from the rdd
//	      new Function<JavaRDD<ConsumerRecord<String, String>>,
//	        JavaRDD<ConsumerRecord<String, String>>>() {
//	          @Override
//	          public JavaRDD<ConsumerRecord<String, String>> call(
//	            JavaRDD<ConsumerRecord<String, String>> rdd
//	          ) {
//	            OffsetRange[] offsets = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
//	            offsetRanges.set(offsets);
//	            return rdd;
//	          }
//	        }
//	    ).map(
//	        new Function<ConsumerRecord<String, String>, String>() {
//	          @Override
//	          public String call(ConsumerRecord<String, String> r) {
//	        	  log.warn("HEY");
//	        	  log.warn(r.value());
//	            return r.value();
//	          }
//	        }
//	    );
//stream1.foreachRDD(new VoidFunction<JavaRDD<String>>() {
//
//	@Override
//	public void call(JavaRDD<String> rdd) throws Exception {
//		// TODO Auto-generated method stub
//		log.warn("HEYHEYHEY");
//		if(rdd!=null){
//			log.warn(rdd.toDebugString());
//			log.warn(String.valueOf(rdd.count()));
//	        //javaFunctions(rdd).writerBuilder("alpha", "spectrum", mapToRow(Spectrum.class)).saveToCassandra();			
//			rdd.foreachAsync(new VoidFunction<String>() {
//
//				@Override
//				public void call(String t) throws Exception {
//					// TODO Auto-generated method stub
//					log.warn("HIER KOMMT T");
//					log.warn(t);
//				}
//				
//			});
//		}
//	}
//});