package com.speckvonschmeck.spark;

import java.util.ArrayList;
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

import static com.datastax.spark.connector.japi.CassandraJavaUtil.*;


import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.spark.connector.japi.rdd.CassandraJavaRDD;
import com.google.gson.Gson;
import com.speckvonschmeck.models.Data;
import com.speckvonschmeck.models.Meta;
import com.speckvonschmeck.models.SingleSpectrum;
import com.speckvonschmeck.models.Spectrum;

public class SpectrumJob {
	
	
	public final static String KAFKA_URL = System.getenv("KAFKA_URL") != null ? 
			System.getenv("KAFKA_URL")
			: "192.168.178.64:9092";
			
	public final static String KAFKA_TOPIC = System.getenv("KAFKA_TOPIC") != null ? 
			System.getenv("KAFKA_TOPIC")
			: "speckvonschmeck";
			
	public static void main(String[] args) throws Exception {

		Logger log = new Log4jLoggerFactory().getLogger("");
				
		SparkConf conf = new SparkConf().setAppName("speckvonschmeck").setMaster("local[4]").set("spark.cassandra.connection.host", "127.0.0.1");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaStreamingContext context = new JavaStreamingContext(sc, new Duration(1000));
		
		
        CassandraConnector connector = CassandraConnector.apply(conf);
        
        try (Session session = connector.openSession()) {
            session.execute("DROP KEYSPACE IF EXISTS ALPHA");
            session.execute("CREATE KEYSPACE ALPHA WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
            session.execute("CREATE TABLE ALPHA.SPECTRUM (title TEXT PRIMARY KEY, scans TEXT, pepmass TEXT, charge TEXT, rtinseconds TEXT, x LIST<INT>, y LIST<INT>)");
            //session.execute("CREATE TABLE SPECCOMPARE (id UUID PRIMARY KEY, product INT, price DECIMAL)");
        }
        
//        Spectrum test = new Spectrum();
//		Data testdata1= new Data();
//		Data testdata2= new Data();
//		Meta testmeta= new Meta();
//		List<Data> liste = new ArrayList<Data>();
//		
//		
//		testdata1.setX(142);
//		testdata1.setY(12422);
//		testdata2.setX(2533);
//		testdata2.setY(34);
//		liste.add(testdata1);
//		liste.add(testdata2);
//		testmeta.setCharge("slojgpos");
//		testmeta.setPepmass("osihgiosfh");
//		testmeta.setRtInSeconds("soihgoih");
//		testmeta.setScans("slighopsieg");
//		testmeta.setTitle("lshgoi");
//		
//		test.setData(liste);
//		test.setMeta(testmeta);
//		
//		
//		SingleSpectrum mitListe = new SingleSpectrum();
//		List<SingleSpectrum> spectra = new ArrayList<SingleSpectrum>();
//		
//		mitListe.setCharge(test.getMeta().getCharge());
//		mitListe.setPepmass(test.getMeta().getPepmass());
//		mitListe.setRtinseconds(test.getMeta().getRtInSeconds());
//		mitListe.setScans(test.getMeta().getScans());
//		mitListe.setTitle(test.getMeta().getTitle());
//		
//		List<Integer> x= new ArrayList<Integer>();
//		List<Integer> y= new ArrayList<Integer>();
//		
//		for (int i=0; i<test.getData().size(); i++){
//			x.add((int) test.getData().get(i).getX());
//			y.add((int) test.getData().get(i).getY());
//		}
//		mitListe.setX(x);
//		mitListe.setY(y);
//		spectra.add(mitListe);
//		
//		
//		JavaRDD<SingleSpectrum> rdd2 = (sc).parallelize(spectra);
//		javaFunctions(rdd2).writerBuilder("alpha", "spectrum", mapToRow(SingleSpectrum.class)).saveToCassandra();							
        
        
        
        //JavaRDD<Integer> pricesRDD = javaFunctions(sc).cassandraTable("test", "spectrum", mapColumnTo(Integer.class)).select("x");
                       
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

			private static final long serialVersionUID = 1L;

			@Override
			public Spectrum call(ConsumerRecord<String, String> record) throws Exception {
				log.warn("-----------HALLLOOOOOOOOOO----------");
				return new Gson().fromJson(record.value(), Spectrum.class);
			}

		}).foreachRDD(new VoidFunction<JavaRDD<Spectrum>>(){

			private static final long serialVersionUID = 1L;

			@Override
			public void call(JavaRDD<Spectrum> rdd) throws Exception {
				// TODO Auto-generated method stub
				log.warn("-----------HALLLOOOOOOOOOO----------");
				if(rdd!=null){
					log.warn(rdd.toDebugString());
					log.warn(String.valueOf(rdd.count()));
			        //javaFunctions(rdd).writerBuilder("alpha", "spectrum", mapToRow(Spectrum.class)).saveToCassandra();			
					rdd.foreachAsync(new VoidFunction<Spectrum>() {

						private static final long serialVersionUID = 1L;

						@Override
						public void call(Spectrum t) throws Exception {
							// TODO Auto-generated method stub
					        
					        
					        Spectrum test = new Spectrum();
							Data testdata1= new Data();
							Data testdata2= new Data();
							Meta testmeta= new Meta();
							List<Data> liste = new ArrayList<Data>();
							
							
							testdata1.setX(142);
							testdata1.setY(12422);
							testdata2.setX(2533);
							testdata2.setY(34);
							liste.add(testdata1);
							liste.add(testdata2);
							testmeta.setCharge("slojgpos");
							testmeta.setPepmass("osihgiosfh");
							testmeta.setRtInSeconds("soihgoih");
							testmeta.setScans("slighopsieg");
							testmeta.setTitle("lshgoi");
							
							test.setData(liste);
							test.setMeta(testmeta);
							
							
							SingleSpectrum mitListe = new SingleSpectrum();
							List<SingleSpectrum> spectra = new ArrayList<SingleSpectrum>();
							
							mitListe.setCharge(t.getMeta().getCharge());
							mitListe.setPepmass(t.getMeta().getPepmass());
							mitListe.setRtinseconds(t.getMeta().getRtInSeconds());
							mitListe.setScans(t.getMeta().getScans());
							mitListe.setTitle(t.getMeta().getTitle());
							
							List<Integer> x= new ArrayList<Integer>();
							List<Integer> y= new ArrayList<Integer>();
							
							for (int i=0; i<t.getData().size(); i++){
								x.add((int) t.getData().get(i).getX());
								y.add((int) t.getData().get(i).getY());
							}
							mitListe.setX(x);
							mitListe.setY(y);
							spectra.add(mitListe);
							
							
							JavaRDD<SingleSpectrum> rdd2 = (sc).parallelize(spectra);
							javaFunctions(rdd2).writerBuilder("alpha", "spectrum", mapToRow(SingleSpectrum.class)).saveToCassandra();							
					        
							
							log.warn("-----------HALLLOOOOOOOOOO----------");
							log.warn(t.toString());
							
							
							
						}
					});
				}
				
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
