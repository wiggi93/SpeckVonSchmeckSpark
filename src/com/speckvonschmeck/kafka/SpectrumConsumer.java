package com.speckvonschmeck.kafka;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import com.google.gson.Gson;

public class SpectrumConsumer implements Runnable{

	private static final Gson GSON = new Gson();
	
	@Override
	public void run(){
		Properties props = new Properties();
		props.put("bootstrap.servers", "192.168.178.64:9092");
		props.put("group.id", "spectrumConsumer");
		props.put("key.deserializer", StringDeserializer.class.getName());
		props.put("value.deserializer", StringDeserializer.class.getName());
		
		try(KafkaConsumer<String,String> consumer = new KafkaConsumer<>(props)){
//			System.out.println("consumer");
			consumer.subscribe(Arrays.asList("speckvonschmeck"));
			while(true){
//				System.out.println("blubb");
				ConsumerRecords<String, String> records = consumer.poll(100);
				System.out.println(records.toString());
//				spectrum=GSON.fromJson(json, classOfT), Spectrum.class);
//				for(ConsumerRecord<String,String> record : records){
//					System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
//					REPO.addGeolocation(GSON.fromJson(record.value(), GeoLocation.class));
//				}
			}
		}catch(Exception e){
			System.err.println("Error while consuming geolocations. Details: " + e.getMessage());
		}
		
	}

}
