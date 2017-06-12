package com.speckvonschmeck.spark.cassandra;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapRowTo;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.dstream.ConstantInputDStream;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.utils.UUIDs;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.rdd.CassandraJavaRDD;
import com.google.gson.Gson;
import com.speckvonschmeck.spark.models.SpecCompare;
import com.speckvonschmeck.spark.models.Spectrum;
import com.speckvonschmeck.spark.spark.ScoringFunction;
import com.speckvonschmeck.spark.spark.SpectrumJob;


public class CassandraConnection implements Serializable{

	private static JavaSparkContext sc;
	private CassandraConnector connector;
	private List<Spectrum> spectrumliste;
	private List<SpecCompare> speccompareliste;
	static int zaehler=0;
	
	public CassandraConnection(JavaSparkContext jsc, CassandraConnector connector){
		sc=jsc;
		this.connector=connector;
		this.spectrumliste=  new ArrayList<Spectrum>();
		this.speccompareliste=  new ArrayList<SpecCompare>();
	}
	
	
	public void createDB(){
	

      try (Session session = connector.openSession()) {
          session.execute("DROP KEYSPACE IF EXISTS ALPHA");
          session.execute("CREATE KEYSPACE ALPHA WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
          session.execute("CREATE TABLE ALPHA.SPECTRUM (uuid TIMEUUID PRIMARY KEY, title TEXT, scans TEXT, pepmass TEXT, charge TEXT, rtinseconds TEXT, x LIST<DOUBLE>, y LIST<DOUBLE>)");
          session.execute("CREATE TABLE ALPHA.SPECCOMPARE (uuid TIMEUUID PRIMARY KEY, Spectrum1ID TIMEUUID, Spectrum2ID TIMEUUID, score DOUBLE, user TEXT, time BIGINT)");
      }
	}
	
	public void saveSpec(ConsumerRecord<String, String> rec){
		SpectrumJob.readyForNext = false;
		System.out.println("------------SAVESPEC-----------");  
		Spectrum a = new Gson().fromJson(rec.value(), Spectrum.class);
		  a.setUuid(UUIDs.timeBased());
		  spectrumliste.clear();
		  spectrumliste.add(a);
		  zaehler=0;
		  
    	  JavaRDD<Spectrum> specRDD = CassandraJavaUtil.javaFunctions(sc)
          .cassandraTable("alpha", "spectrum", mapRowTo(Spectrum.class));
    	  long count=specRDD.count();
    	  System.out.println("------COUNT: "+count);
    	  if(count <= 0){
    		  writeData();
    	  }else
    	  specRDD.foreach(new VoidFunction<Spectrum>() {
    		  
			@Override
			public void call(Spectrum t) throws Exception {
				// TODO Auto-generated method stub
				zaehler++;
				System.out.println("----------"+zaehler+"-----------");
				  System.out.println("---------SPECCOMPARE---------");
  			  speccompareliste.add(ScoringFunction.Scoring(a,t));
  			  if (zaehler>=count){
  				  writeData();
  			  }
			}
    	  });
		  
         
		
	}
	public void writeData(){
		
		JavaRDD<SpecCompare> scoreRDD = sc.parallelize(speccompareliste);
  	  	javaFunctions(scoreRDD).writerBuilder("alpha", "speccompare", mapToRow(SpecCompare.class)).saveToCassandra();	
  	  	speccompareliste.clear();
  	  if (!spectrumliste.isEmpty()){
	    	System.out.println(spectrumliste.size());
	    	  JavaRDD<Spectrum> rdd2 = sc.parallelize(spectrumliste);
	    	  javaFunctions(rdd2).writerBuilder("alpha", "spectrum", mapToRow(Spectrum.class)).saveToCassandra();	
	    	  } 
  	  
  	  Thread t = new Thread(new Runnable() {
		
		@Override
		public void run() {
			try {
				Thread.sleep(700);
				SpectrumJob.readyForNext = true;
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
  	  });
  	  t.start();
	}
}
