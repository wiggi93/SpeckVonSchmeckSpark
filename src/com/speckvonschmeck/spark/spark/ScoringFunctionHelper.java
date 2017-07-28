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
		comp.setScore3(dotProduct());
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
	
	
	private static double dotProduct(){
        int specASumSize = 0, specBSumSize = 0;
        double specAMinValue=specA.getY().get(0);
        
        for(double a : specA.getY()){
        	if(a<specAMinValue)
        		specAMinValue=a;
        }
        
        specASumSize = (int) (specA.getX().get(specA.getX().size()-1)/100);
        specBSumSize = (int) (specB.getX().get(specB.getX().size()-1)/100);
        
        
       
        double[] specASum = new double[specASumSize>specBSumSize ? specASumSize+1 : specBSumSize+1];
        double[] specBSum = new double[specASumSize>specBSumSize ? specASumSize+1 : specBSumSize+1];
       
        for(int i = 0; i < specASum.length; i++){
            specASum[i] = (double) 0;
        }
        for(int i = 0; i < specBSum.length; i++){
            specBSum[i] = (double) 0;
        }
       
        for(int i = 0; i < specA.getX().size(); i++){
                specASum[(int) (specA.getX().get(i)/100)] += specA.getY().get(i);
        }
        for(int i = 0; i < specB.getX().size(); i++){
                specBSum[(int) (specB.getX().get(i)/100)] += specB.getY().get(i);
        }
        return calculateDotProductDerived(specASum, specBSum, specAMinValue, (double)1, (double)1, true);
	}
	
	
	
	
//	
//	
//	 /**
//     * This method calculates dot product score according to given weights.
//     * Maximum similarity gives 1. Minimum gives 0.
//     *
//     * @param xArray - binned-spectrumX with intensities summed up
//     * @param yArray - binned-spectrumY with intensities summed up
//     * @param x - weight for peak intensity
//     * @param y - weight for peak m/z
//     * @param isNormalized - true: normalize calculated dot-score, false: no
//     * normalization on dot-score
//     * @return a score
//     */
    public static double calculateDotProductDerived(double[] xArray, double[] yArray, double specAMinValue, double x, double y, boolean isNormalized) {
        // TODO: Make sure if any of binSpectra is empty, score i NaN! and return!         
        // now, calculate dot product score
        double dot_product_alpha_beta = 0,
                dot_product_alpha_alpha = 0,
                dot_product_beta_beta = 0;
        double fragmentTolerance=0.5;
        double score=0;
        
        double binSize = 2 * fragmentTolerance,
                mz = specAMinValue + binSize;
        
        for (int i = 0; i < xArray.length; i++) {
            double mz_peakA = mz,
                    mz_peakB = mz,
                    intensity_peakA = xArray[i],
                    intensity_peakB = yArray[i];
            boolean control = false;
            if (intensity_peakA == 0 && intensity_peakB == 0) {
                control = true;
            }
            if (!control) {
                if (i == xArray.length - 1) {// make sure last one is not empty..
                    mz_peakA = mz_peakA + 0.0000001;
                    mz_peakB = mz_peakB + 0.0000001;
                }
                double alpha = Math.pow(intensity_peakA, x) * Math.pow(mz_peakA, y),
                        beta = Math.pow(intensity_peakB, x) * Math.pow(mz_peakB, y);
                dot_product_alpha_beta = dot_product_alpha_beta + (double) (alpha * beta);
                dot_product_alpha_alpha = dot_product_alpha_alpha + (double) (alpha * alpha);
                dot_product_beta_beta = dot_product_beta_beta + (double) (beta * beta);
                mz = mz + binSize;
            }
        }
        if (isNormalized) {
            double normalized_dot_product = (double) dot_product_alpha_beta / (double) (Math.sqrt(dot_product_alpha_alpha * dot_product_beta_beta));
            score = normalized_dot_product;
        } else {
            score = dot_product_alpha_beta;
        }
        return score;
    }
	
	
	
	
	
	
	
	
	
}
