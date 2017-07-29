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
	private static List<Spectrum> specList = new ArrayList<Spectrum>();
	private static List<SpecCompare> specCompareList = new ArrayList<SpecCompare>();
	private static List<SpecCompare> bufferlist = new ArrayList<SpecCompare>();
	
	public CassandraConnection(JavaSparkContext sc, CassandraConnector connector){
		sparkContext = sc;
		this.connector = connector;
	}
	
	/**
	* This method drops all cassandra tables if they exist and recreates them
	*/
	public void createDB(){
      try (Session session = connector.openSession()) {
          session.execute("DROP KEYSPACE IF EXISTS ALPHA");
          session.execute("CREATE KEYSPACE ALPHA WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
          session.execute("CREATE TABLE ALPHA.SPECTRUM (uuid TIMEUUID PRIMARY KEY, title TEXT, scans TEXT, pepmass TEXT, charge TEXT, rtinseconds TEXT, x LIST<DOUBLE>, y LIST<DOUBLE>)");
          session.execute("CREATE TABLE ALPHA.SPECCOMPARE (uuid TIMEUUID PRIMARY KEY, spectrum1id TIMEUUID, spectrum2id TIMEUUID, score1 DOUBLE, score2 DOUBLE, score3 DOUBLE, user TEXT, time BIGINT)");
      }catch(Exception e){
    	  e.printStackTrace();
      }
	}
	
	/**
	* This method takes a spectrum from kafka and compares it with every other spectrum that is already stored.
	* Then the result and given spectrum will be stored as well.
	* @param rec - spectrum as Map<String, String> from kafka
	*/
	public void computeCassandraOperations(ConsumerRecord<String, String> rec){
		Spectrum spectrum1 = new Gson().fromJson(rec.value(), Spectrum.class);
		spectrum1.setUuid(UUIDs.timeBased());
		specList.clear();
		specList.add(spectrum1);
		writeSpecRow("alpha", "spectrum");
		JavaRDD<Spectrum> specRDD = getTableAsRDD("alpha", "spectrum");
		specRDD.foreach(new VoidFunction<Spectrum>() {
			private static final long serialVersionUID = -723624019513843295L;
			@Override
			public void call(Spectrum spectrum2) throws Exception {
				if (!spectrum1.getUuid().equals(spectrum2.getUuid())){
					try{				
						specCompareList.add(ScoringFunctionHelper.compare(spectrum1, spectrum2));
					}catch(Exception e){
						e.printStackTrace();
					}
				}
  			  	if (specCompareList.size() >= 500){
  			  		writeSpecCompareRow("alpha", "speccompare");
  			  	}
			}
		});
		
		writeSpecCompareRow("alpha", "speccompare");
	}
	
	/**
	* This method returns all objects inside the given table as a RDD object.
	* @param keyspace - keyspace to save to 
	* @param table - table to save to
	* 
	* @return table content as RDD
	*/
	private JavaRDD<Spectrum> getTableAsRDD(String keyspace, String table){
		return CassandraJavaUtil.javaFunctions(sparkContext)
		.cassandraTable(keyspace, table, mapRowTo(Spectrum.class));
	}
	
	/**
	* This method writes the spectrum object in the given table
	* @param keyspace - keyspace to save to 
	* @param table - table to save to
	*/
	private void writeSpecRow(String keyspace, String table){
		JavaRDD<Spectrum> rdd2 = sparkContext.parallelize(specList);
  		javaFunctions(rdd2).writerBuilder(keyspace, table, mapToRow(Spectrum.class)).saveToCassandra();	
	}
	
	/**
	* This method writes the spectrumCompare object in the given table. Therefore it puts the specCompare objects inside 
	* a bufferList everytime the method is called and tries to write them to cassandra. If it succeeds, the bufferlist 
	* is being cleared.
	* @param keyspace - keyspace to save to 
	* @param table - table to save to
	*/
	private void writeSpecCompareRow(String keyspace, String table){
		try{
			bufferlist.addAll(specCompareList);
			specCompareList.clear();
			JavaRDD<SpecCompare> scoreRDD = sparkContext.parallelize(bufferlist);
	  	  	javaFunctions(scoreRDD).writerBuilder(keyspace, table, mapToRow(SpecCompare.class)).saveToCassandra();
	  	  	
	  	  	bufferlist.clear();
		}catch(Exception e){
			e.printStackTrace();
		}
		SpectrumJob.timeEnd=System.currentTimeMillis();
		System.out.println(SpectrumJob.timeEnd-SpectrumJob.timeStart);
	}
}
