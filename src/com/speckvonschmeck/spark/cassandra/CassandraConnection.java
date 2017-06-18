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
	private static List<Spectrum> specList =  new ArrayList<Spectrum>();
	private static List<SpecCompare> specCompareList =  new ArrayList<SpecCompare>();
	private static long count = 0;
	
	public CassandraConnection(JavaSparkContext sc, CassandraConnector connector){
		sparkContext = sc;
		this.connector = connector;
	}
	
	public void createDB(){
      try (Session session = connector.openSession()) {
          session.execute("DROP KEYSPACE IF EXISTS ALPHA");
          session.execute("CREATE KEYSPACE ALPHA WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
          session.execute("CREATE TABLE ALPHA.SPECTRUM (uuid TIMEUUID PRIMARY KEY, title TEXT, scans TEXT, pepmass TEXT, charge TEXT, rtinseconds TEXT, x LIST<DOUBLE>, y LIST<DOUBLE>)");
          session.execute("CREATE TABLE ALPHA.SPECCOMPARE (uuid TIMEUUID PRIMARY KEY, spectrum1id TIMEUUID, spectrum2id TIMEUUID, score1 DOUBLE, score2 DOUBLE, score3 DOUBLE, user TEXT, time BIGINT)");
      }
	}
	
	public void saveSpec(ConsumerRecord<String, String> rec){
		SpectrumJob.readyForNext = false;
		specCompareList.clear();
		Spectrum spectrum1 = new Gson().fromJson(rec.value(), Spectrum.class);
		spectrum1.setUuid(UUIDs.timeBased());
		specList.clear();
		specList.add(spectrum1);
		writeSpecRow("alpha", "spectrum");
		JavaRDD<Spectrum> specRDD = getTableAsRDD("alpha", "spectrum");
		count = specRDD.count();
		specRDD.foreach(new VoidFunction<Spectrum>() {
			private static final long serialVersionUID = -723624019513843295L;
			@Override
			public void call(Spectrum spectrum2) throws Exception {
				specCompareList.add(ScoringFunctionHelper.compare(spectrum1, spectrum2));
  			  	if (specCompareList.size() >= count){
  			  		writeSpecCompareRow("alpha", "speccompare");
  			  	}
			}
		});
	}
	
	private JavaRDD<Spectrum> getTableAsRDD(String keyspace, String table){
		return CassandraJavaUtil.javaFunctions(sparkContext)
		.cassandraTable(keyspace, table, mapRowTo(Spectrum.class));
	}
	
	private void writeSpecRow(String keyspace, String table){
		JavaRDD<Spectrum> rdd2 = sparkContext.parallelize(specList);
  		javaFunctions(rdd2).writerBuilder(keyspace, table, mapToRow(Spectrum.class)).saveToCassandra();	
	}
	
	private void writeSpecCompareRow(String keyspace, String table){
		try{
			JavaRDD<SpecCompare> scoreRDD = sparkContext.parallelize(specCompareList);
	  	  	javaFunctions(scoreRDD).writerBuilder(keyspace, table, mapToRow(SpecCompare.class)).saveToCassandra();
	  	  	specCompareList.clear();
		}catch(Exception e){
			e.printStackTrace();
		}
	}
}
