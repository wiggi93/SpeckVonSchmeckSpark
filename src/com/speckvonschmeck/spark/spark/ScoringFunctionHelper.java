package com.speckvonschmeck.spark.spark;

import java.util.Date;

import com.datastax.driver.core.utils.UUIDs;
import com.speckvonschmeck.spark.models.SpecCompare;
import com.speckvonschmeck.spark.models.Spectrum;

public class ScoringFunctionHelper {

	public static SpecCompare compare(Spectrum a, Spectrum b){
		
		SpecCompare comp = new SpecCompare();
		Date zeit = new Date();
		
		comp.setUuid(UUIDs.timeBased());
		comp.setScore(1);
		comp.setSpectrum1id(a.getUuid());
		comp.setSpectrum2id(b.getUuid());
		comp.setUser("ObiWan");
		comp.setTime(zeit.getTime());
		
		return comp;
	}
	
}
