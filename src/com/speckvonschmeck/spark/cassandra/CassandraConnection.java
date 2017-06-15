package com.speckvonschmeck.spark.cassandra;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapRowTo;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.utils.UUIDs;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.google.gson.Gson;
import com.speckvonschmeck.spark.models.SpecCompare;
import com.speckvonschmeck.spark.models.Spectrum;
import com.speckvonschmeck.spark.spark.ScoringFunctionHelper;
import com.speckvonschmeck.spark.spark.SpectrumJob;


public class CassandraConnection implements Serializable{
	private static final long serialVersionUID = -9142841410311367400L;
	
	private static JavaSparkContext sparkContext;
	private CassandraConnector connector;
	private List<Spectrum> specList;
	private List<SpecCompare> specCompareList;
	static int zaehler = 0;
	
	public CassandraConnection(JavaSparkContext sc, CassandraConnector connector){
		sparkContext = sc;
		this.connector = connector;
		this.specList =  new ArrayList<Spectrum>();
		this.specCompareList=  new ArrayList<SpecCompare>();
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
		Spectrum spectrum1 = new Gson().fromJson(rec.value(), Spectrum.class);
		spectrum1.setUuid(UUIDs.timeBased());
		specList.clear();
		specList.add(spectrum1);
		zaehler = 0;
		  
		JavaRDD<Spectrum> specRDD = CassandraJavaUtil.javaFunctions(sparkContext)
				.cassandraTable("alpha", "spectrum", mapRowTo(Spectrum.class));
		long count = specRDD.count();
		System.out.println("------COUNT: "+count);
		if(count <= 0){
			writeData();
		}else{
			specRDD.foreach(new VoidFunction<Spectrum>() {
				private static final long serialVersionUID = -723624019513843295L;

				@Override
				public void call(Spectrum spectrum2) throws Exception {
					zaehler++;
					System.out.println("----------"+zaehler+"-----------");
					System.out.println("---------SPECCOMPARE---------");
	  			  	specCompareList.add(ScoringFunctionHelper.compare(spectrum1, spectrum2));
	  			  	if (zaehler >= count){
	  			  		writeData();
	  			  	}
				}
			});
		}
	}
	
	public void writeData(){
		JavaRDD<SpecCompare> scoreRDD = sparkContext.parallelize(specCompareList);
  	  	javaFunctions(scoreRDD).writerBuilder("alpha", "speccompare", mapToRow(SpecCompare.class)).saveToCassandra();	
  	  	specCompareList.clear();
  	  	if (!specList.isEmpty()){
  	  		System.out.println(specList.size());
  	  		JavaRDD<Spectrum> rdd2 = sparkContext.parallelize(specList);
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
