package com.speckvonschmeck.spark.cassandra;

import java.util.ArrayList;
import java.util.List;

import com.speckvonschmeck.spark.models.Data;
import com.speckvonschmeck.spark.models.Meta;
import com.speckvonschmeck.spark.models.SingleSpectrum;
import com.speckvonschmeck.spark.models.Spectrum;

public class CassandraTester {
	
	public static List<SingleSpectrum> generateSingleSpectraFromSpectrum(Spectrum spectrum){
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
		
		mitListe.setCharge(spectrum.getMeta().getCharge());
		mitListe.setPepmass(spectrum.getMeta().getPepmass());
		mitListe.setRtinseconds(spectrum.getMeta().getRtInSeconds());
		mitListe.setScans(spectrum.getMeta().getScans());
		mitListe.setTitle(spectrum.getMeta().getTitle());
		
		List<Integer> x= new ArrayList<Integer>();
		List<Integer> y= new ArrayList<Integer>();
		
		for (int i=0; i<spectrum.getData().size(); i++){
			x.add((int) spectrum.getData().get(i).getX());
			y.add((int) spectrum.getData().get(i).getY());
		}
		mitListe.setX(x);
		mitListe.setY(y);
		spectra.add(mitListe);
		
		return spectra;
	}

}
