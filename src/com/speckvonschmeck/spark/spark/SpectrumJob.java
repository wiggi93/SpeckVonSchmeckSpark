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

import scala.Tuple2;

public class SpectrumJob {
	
	
	public final static String KAFKA_URL = System.getenv("KAFKA_URL") != null ? 
			System.getenv("KAFKA_URL")
			: "192.168.178.64:9092";//64
			
			
	public final static String KAFKA_TOPIC = System.getenv("KAFKA_TOPIC") != null ? 
			System.getenv("KAFKA_TOPIC")
			: "speckvonschmeck";
			
	public static void main(String[] args) throws Exception {

		Logger log = new Log4jLoggerFactory().getLogger("");
				
		SparkConf conf = new SparkConf().setAppName("speckvonschmeck").setMaster("local[3]");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaStreamingContext context = new JavaStreamingContext(sc, new Duration(2000));
		
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
				    	  if(consumerRecords.hasNext()){
				    		  ConsumerRecord<String, String> rec = consumerRecords.next();
					    	  System.out.println(rec.key());
					    	  System.out.println(rec.value());
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


/*
 * noch nicht getestet: treffen mit roman am 09.06.17
 */
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