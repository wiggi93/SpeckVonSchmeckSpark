package com.speckvonschmeck.spark.spark;
 
import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapRowTo;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.HasOffsetRanges;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.OffsetRange;
import org.slf4j.Logger;
import org.slf4j.impl.Log4jLoggerFactory;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.utils.UUIDs;
import com.datastax.spark.connector.CassandraRow;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.google.gson.Gson;
import com.speckvonschmeck.spark.cassandra.CassandraTester;
import com.speckvonschmeck.spark.models.Spectrum;

import scala.Tuple2;

public class SpectrumJob {
	
	
	
	public final static String KAFKA_URL = System.getenv("KAFKA_URL") != null ? 
			System.getenv("KAFKA_URL")
			: "192.168.178.64:9092";//64
			
	public final static String CASSANDRA_URL = "localhost";//64
			
	public final static String KAFKA_TOPIC = System.getenv("KAFKA_TOPIC") != null ? 
			System.getenv("KAFKA_TOPIC")
			: "speckvonschmeck";
			
	public static JavaSparkContext sc;
	public static void main(String[] args) throws Exception {

		List<Spectrum> list = new ArrayList<Spectrum>();
		
		Logger log = new Log4jLoggerFactory().getLogger("");
				
		SparkConf conf = new SparkConf().setAppName("speckvonschmeck").setMaster("local[3]");
		
		sc = new JavaSparkContext(conf);
		JavaStreamingContext context = new JavaStreamingContext(sc, new Duration(2000));
		 
		
        CassandraConnector connector = CassandraConnector.apply(conf);
        
//        try (Session session = connector.openSession()) {
//            session.execute("DROP KEYSPACE IF EXISTS ALPHA");
//            session.execute("CREATE KEYSPACE ALPHA WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
//            session.execute("CREATE TABLE ALPHA.SPECTRUM (uuid TIMEUUID PRIMARY KEY, title TEXT, scans TEXT, pepmass TEXT, charge TEXT, rtinseconds TEXT, x LIST<INT>, y LIST<INT>)");
//        }
        
       
        
        
 //       List<Spectrum> spectra = CassandraTester.generateSpectraFromSpectrum();
		
//		JavaRDD<Spectrum> rdd2 = (sc).parallelize(spectra);
//		javaFunctions(rdd2).writerBuilder("alpha", "spectrum", mapToRow(Spectrum.class)).saveToCassandra();	
        
//		JavaRDD<Spectrum> personRdd = CassandraJavaUtil.javaFunctions(sc)
//                .cassandraTable("alpha", "spectrum", mapRowTo(Spectrum.class));
//		javaFunctions(personRdd).writerBuilder("beta", "spectrum", mapToRow(Spectrum.class)).saveToCassandra();	
//		
         
        Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put("bootstrap.servers", KAFKA_URL);
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("group.id", "spectra");
		kafkaParams.put("auto.offset.reset", "latest");
		kafkaParams.put("enable.auto.commit", false);

		Collection<String> topics = Arrays.asList(KAFKA_TOPIC);
 
		final JavaInputDStream<ConsumerRecord<String, String>> dstream = KafkaUtils.createDirectStream(context,
				LocationStrategies.PreferConsistent(),
				ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));
		
		dstream.mapToPair(new PairFunction<ConsumerRecord<String, String>, String, String>() {
			@Override
		    public Tuple2<String, String> call(ConsumerRecord<String, String> record) {
				log.warn(record.value());
				return new Tuple2<>(record.key(), record.value());
		    }
		});	
		
		dstream.foreachRDD(new VoidFunction<JavaRDD<ConsumerRecord<String, String>>>() {
			  @Override
			  public void call(JavaRDD<ConsumerRecord<String, String>> rdd) {
				  final OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
				  rdd.foreachPartition(new VoidFunction<Iterator<ConsumerRecord<String, String>>>() {
				      @Override
				      public void call(Iterator<ConsumerRecord<String, String>> consumerRecords) {
				    	  OffsetRange o = offsetRanges[TaskContext.get().partitionId()];
				    	  System.out.println(o.topic() + " " + o.partition() + " " + o.fromOffset() + " " + o.untilOffset());
				    	
				 
				    	  while(consumerRecords.hasNext()){
				    		  ConsumerRecord<String, String> rec = consumerRecords.next();
				    		  
				    		  Spectrum t = new Gson().fromJson(rec.value(), Spectrum.class);
				    		  t.setUuid(UUIDs.timeBased());
				    		  list.clear();
				    		  list.add(t);
					    	  
					    	  
					    	  JavaRDD<Spectrum> specRDD = CassandraJavaUtil.javaFunctions(sc)
					                  .cassandraTable("alpha", "spectrum", mapRowTo(Spectrum.class));
					          specRDD.foreachPartition(new VoidFunction<Iterator<Spectrum>>() {

					  			@Override
					  			public void call(Iterator<Spectrum> t) throws Exception {
					  				// TODO Auto-generated method stub
					  			
					  			}
					  			});
					    	  
					          if (!list.isEmpty()){
							    	System.out.println(list.size());
							    	  JavaRDD<Spectrum> rdd2 = sc.parallelize(list);
							    	  javaFunctions(rdd2).writerBuilder("alpha", "spectrum", mapToRow(Spectrum.class)).saveToCassandra();	
							    	  }  
					    	  
				    	  }
				    	  
				    	  
				      }
				  });
			  }
		});
        
	    
	    
		context.start();
		try {
			context.awaitTermination();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}	

}