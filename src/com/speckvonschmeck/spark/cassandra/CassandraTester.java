package com.speckvonschmeck.spark.cassandra;

import java.util.ArrayList;
import java.util.List;


import com.speckvonschmeck.spark.models.Spectrum;

public class CassandraTester {
	
	public static List<Spectrum> generateSpectraFromSpectrum(){
		Spectrum test = new Spectrum();
		List<Double> xliste = new ArrayList<Double>();
		List<Double> yliste = new ArrayList<Double>();
		
		
		xliste.add((double) 3513);
		xliste.add((double) 51465);
		yliste.add((double) 3513);
		yliste.add((double) 51465);
		
		
		test.setCharge("sloafsjgpos");
		test.setPepmass("osihgiosfh");
		test.setrtinseconds("soihgoih");
		test.setScans("slighopsieg");
		test.setTitle("lshgoi");
		
		test.setX(xliste);
		test.setY(yliste);
		
		List<Spectrum> spectra = new ArrayList<Spectrum>();
		spectra.add(test);
		
		return spectra;
	}

}
