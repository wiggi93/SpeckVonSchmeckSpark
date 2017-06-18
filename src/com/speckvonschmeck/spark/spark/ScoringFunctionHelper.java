package com.speckvonschmeck.spark.spark;

import java.util.Date;

import com.datastax.driver.core.utils.UUIDs;
import com.speckvonschmeck.spark.models.SpecCompare;
import com.speckvonschmeck.spark.models.Spectrum;

public class ScoringFunctionHelper {
	
	private static Spectrum specA, specB;
	
	
	public static SpecCompare compare(Spectrum a, Spectrum b){
		specA = a;
		specB = b;
		
		SpecCompare comp = new SpecCompare();
		Date date = new Date();
		comp.setScore1(compareIdentical() ? 1 : 0);
		comp.setScore2(compareDistance());
		comp.setScore3(compareLength());
		comp.setUser("ObiWan");
		comp.setUuid(UUIDs.timeBased());
		comp.setTime(date.getTime());
		comp.setSpectrum1id(a.getUuid());
		comp.setSpectrum2id(b.getUuid());
		return comp;
	}

	private static boolean compareIdentical(){
		boolean identical = true;
		if(specB.getX().size() == specA.getX().size() && specB.getY().size() == specA.getY().size()){
			for(int i = 0; i < specA.getX().size(); i++){
				if(!specB.getX().get(i).equals(specA.getX().get(i))){
					return false;
				}
			}
			if(identical){
				for(int i = 0; i < specA.getY().size(); i++){
					if(!specB.getY().get(i).equals(specA.getY().get(i))){
						return false;
					}
				}
			}
		}else{
			return false;
		}
		
		return true;
	}
	
	private static double compareDistance(){
		double diff = 0d;
		double tempDiff = 0d;
		double tempAverage = 0d;
		
		for(int i = 0; i < specA.getX().size(); i++){
			if(specB.getX().size() > i){
				tempDiff = Math.abs(specA.getX().get(i)-specB.getX().get(i));
				diff += tempDiff;
				tempAverage += tempDiff;
			}
			else{
				diff += tempAverage/specB.getX().size();
			}
				
		}
		tempAverage = 0d;
		for(int i = 0; i < specA.getY().size(); i++){
			if(specB.getY().size() > i){
				tempDiff = Math.abs(specA.getY().get(i)-specB.getY().get(i));
				diff += tempDiff;
				tempAverage += tempDiff;
			}
			else{
				diff += tempAverage/specB.getY().size();
			}
		}
		return diff == 0 ? 1 : Math.max(0, 1-(diff*0.0000001d));
	}
	
	private static double compareLength(){
		int diff = Math.abs(specA.getX().size() - specB.getX().size());
		return diff == 0 ? 1 : Math.max(0, 1-(diff*0.001d));
	}
	
}
