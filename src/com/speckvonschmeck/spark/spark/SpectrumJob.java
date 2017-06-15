package com.speckvonschmeck.spark.spark;
 
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
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

import com.datastax.spark.connector.cql.CassandraConnector;
import com.speckvonschmeck.spark.cassandra.CassandraConnection;

import scala.Tuple2;

public class SpectrumJob {
	
	public static boolean readyForNext = true;
	public final static String KAFKA_URL = "192.168.178.64:9092";//64
			
	public final static String CASSANDRA_URL = "localhost";//64
			
	public final static String KAFKA_TOPIC = "speckvonschmeck";
			
	public static JavaSparkContext sparkContext;
	public static void main(String[] args) throws Exception {

		Logger log = new Log4jLoggerFactory().getLogger("");
				
		SparkConf conf = new SparkConf().setAppName("speckvonschmeck").setMaster("local[5]");
		
		sparkContext = new JavaSparkContext(conf);
		JavaStreamingContext context = new JavaStreamingContext(sparkContext, new Duration(2000));
		 
        CassandraConnector connector = CassandraConnector.apply(conf);
        CassandraConnection cassi = new CassandraConnection(sparkContext, connector);
        cassi.createDB();     
        
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
			private static final long serialVersionUID = 8679810891248915794L;

			@Override
		    public Tuple2<String, String> call(ConsumerRecord<String, String> record) {
				log.warn(record.value());
				return new Tuple2<>(record.key(), record.value());
		    }
		});	
		dstream.foreachRDD(new VoidFunction<JavaRDD<ConsumerRecord<String, String>>>() {
			private static final long serialVersionUID = 1847656704510166993L;

			@Override
			  public void call(JavaRDD<ConsumerRecord<String, String>> rdd) {
				  final OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
				  rdd.foreachPartition(new VoidFunction<Iterator<ConsumerRecord<String, String>>>() {
					private static final long serialVersionUID = -4724561525660172057L;

					@Override
				      public void call(Iterator<ConsumerRecord<String, String>> consumerRecords) {
				    	  OffsetRange o = offsetRanges[TaskContext.get().partitionId()];
				    	  System.out.println(o.topic() + " " + o.partition() + " " + o.fromOffset() + " " + o.untilOffset());
				 
				    	  while(consumerRecords.hasNext()){
				    		  if(readyForNext){
				    			  cassi.saveSpec(consumerRecords.next());
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