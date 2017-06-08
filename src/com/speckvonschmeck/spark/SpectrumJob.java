package com.speckvonschmeck.spark;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import com.datastax.spark.connector.cql.CassandraConnector;
import com.google.gson.Gson;
import com.speckvonschmeck.models.Spectrum;

public class SpectrumJob {
	
	
	public final static String KAFKA_URL = System.getenv("KAFKA_URL") != null ? 
			System.getenv("KAFKA_URL")
			: "192.168.178.64:9092";
			
	public final static String KAFKA_TOPIC = System.getenv("KAFKA_TOPIC") != null ? 
			System.getenv("KAFKA_TOPIC")
			: "speckvonschmeck";
			
	public static void main(String[] args) throws Exception {


		SparkConf conf = new SparkConf().setAppName("speckvonschmeck").setMaster("local[1]");

		JavaStreamingContext context = new JavaStreamingContext(conf, new Duration(2000));

		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put("bootstrap.servers", KAFKA_URL);
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("group.id", "spectrumConsumer");
		kafkaParams.put("auto.offset.reset", "latest");
		kafkaParams.put("enable.auto.commit", false);

		Collection<String> topics = Arrays.asList(KAFKA_TOPIC);

		final JavaInputDStream<ConsumerRecord<String, String>> dstream = KafkaUtils.createDirectStream(context,
				LocationStrategies.PreferConsistent(),
				ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));

		dstream.map(new Function<ConsumerRecord<String, String>, Spectrum>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Spectrum call(ConsumerRecord<String, String> record) throws Exception {
				return new Gson().fromJson(record.value(), Spectrum.class);
			}

		}).filter(new Function<Spectrum, Boolean>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Spectrum spectrum) throws Exception {
				return spectrum.getData().get(0).getX()<=500;
			}

		}).foreachRDD(new VoidFunction<JavaRDD<Spectrum>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void call(JavaRDD<Spectrum> rdd) throws Exception {

				rdd.foreach(new VoidFunction<Spectrum>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public void call(Spectrum spectrum) throws Exception {
						if (!rdd.isEmpty())
						System.out.println("Spark Job received => " + spectrum);
						
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
		CassandraConnector connector = CassandraConnector.apply(context.getConf());
	}

}
