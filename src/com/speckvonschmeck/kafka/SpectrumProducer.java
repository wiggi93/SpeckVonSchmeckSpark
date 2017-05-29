package com.speckvonschmeck.kafka;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.speckvonschmeck.models.Spectrum;

import scala.util.parsing.json.JSON;

public class SpectrumProducer {


	public final static String KAFKA_URL = System.getenv("KAFKA_URL") != null ? 
			System.getenv("KAFKA_URL")
			: "192.168.99.100:9092";
			
	public final static String KAFKA_TOPIC = System.getenv("KAFKA_TOPIC") != null ? 
			System.getenv("KAFKA_TOPIC")
			: "geolocationJob";

			
	public void sendToKafka(Spectrum spectrum) {
		
			Gson gson= new GsonBuilder().create();
		
		
			Properties props = new Properties();
			props.put("bootstrap.servers", KAFKA_URL);
			props.put("key.serializer", StringSerializer.class.getName());
			props.put("value.serializer", StringSerializer.class.getName());
			// props.put("advertised.host.name", "192.168.99.100");

			Producer<String, String> producer = new KafkaProducer<String, String>(props);
			
			ProducerRecord<String, String> record = new ProducerRecord<String, String>(KAFKA_TOPIC,
					gson.toJson(spectrum));
			
			producer.send(record);


			producer.close();
			
			
		
	}

}
