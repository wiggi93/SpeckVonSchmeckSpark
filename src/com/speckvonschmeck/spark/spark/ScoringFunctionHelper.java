package com.speckvonschmeck.spark.spark;

import java.util.Date;

import com.datastax.driver.core.utils.UUIDs;
import com.speckvonschmeck.spark.models.SpecCompare;
import com.speckvonschmeck.spark.models.Spectrum;

public class ScoringFunctionHelper {
	
	private static Spectrum specA, specB;
	
	/**
	* This method takes two spectra to compare and starts all calculations.
	* @param spectrumA - Spectrum A 
	* @param spectrumB - Spectrum B
	* @return a <object>SpecCompare</object> object
	*/
	public static SpecCompare compare(Spectrum spectrumA, Spectrum spectrumB){
		specA = spectrumA;
		specB = spectrumB;
		
		SpecCompare comp = new SpecCompare();
		Date date = new Date();
		comp.setScore1(compareIdentical() ? 1 : 0);
		comp.setScore2(0);
		comp.setScore3(dotProduct());
		comp.setUser("ObiWan");
		comp.setUuid(UUIDs.timeBased());
		comp.setTime(date.getTime());
		comp.setSpectrum1id(spectrumA.getUuid());
		comp.setSpectrum2id(spectrumB.getUuid());
		return comp;
	}

	/**
	* This method compares spectrum A with spectrum B. It returns true, if they are identical, false if not
	* @return true if identical, false if not
	*/
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
	
	/**
	* This method initializes spectrum A and B. It sums up the intensities of both spectra
	* and starts the calculation <function>calculateDotProductDerived</function>.
	* Maximum similarity gives 1. Minimum gives 0.
	* @return a score
	*/
	private static double dotProduct(){
        int specASumSize = 0, specBSumSize = 0;
        int stepSize = 100;
        double specAMinValue=specA.getY().get(0);
        
        for(double a : specA.getY()){
        	if(a<specAMinValue)
        		specAMinValue=a;
        }
        
        specASumSize = (int) (specA.getX().get(specA.getX().size()-1)/stepSize);
        specBSumSize = (int) (specB.getX().get(specB.getX().size()-1)/stepSize);
       
        double[] specASum = new double[specASumSize>specBSumSize ? specASumSize+1 : specBSumSize+1];
        double[] specBSum = new double[specASumSize>specBSumSize ? specASumSize+1 : specBSumSize+1];
       
        for(int i = 0; i < specASum.length; i++){
            specASum[i] = (double) 0;
        }
        for(int i = 0; i < specBSum.length; i++){
            specBSum[i] = (double) 0;
        }
       
        try{
	    	for(int i = 0; i < specA.getX().size(); i++){
	            specASum[(int) (specA.getX().get(i)/stepSize)] += specA.getY().get(i);
	        }
	        for(int i = 0; i < specB.getX().size(); i++){
	            specBSum[(int) (specB.getX().get(i)/stepSize)] += specB.getY().get(i);
	        }
        }catch(Exception e){
        	e.printStackTrace();
        }
        
        return calculateDotProductDerived(specASum, specBSum, specAMinValue, (double) 1, (double) 1, true);
	}
	
	/**
	* This method calculates dot product score according to given weights.
	* Maximum similarity gives 1. Minimum gives 0.
	*
	* @param specASum - Spectrum A with intensities summed up
	* @param specBSum - Spectrum B with intensities summed up
	* @param specAMinValue - min peak of spectrum A
	* @param x - weight for peak intensity
	* @param y - weight for peak m/z
	* @param isNormalized - true: normalize calculated dot-score, false: no
	* normalization on dot-score
	* @return a score
	*/
    private static double calculateDotProductDerived(double[] specASum, double[] specBSum, double specAMinValue, double x, double y, boolean isNormalized) {
        double dot_product_alpha_beta = 0,
                dot_product_alpha_alpha = 0,
                dot_product_beta_beta = 0;
        double fragmentTolerance=0.5;
        double score=0;
        
        double binSize = 2 * fragmentTolerance,
                mz = specAMinValue + binSize;
        
        for (int i = 0; i < specASum.length; i++) {
            double mz_peakA = mz,
                    mz_peakB = mz,
                    intensity_peakA = specASum[i],
                    intensity_peakB = specBSum[i];
            boolean control = false;
            if (intensity_peakA == 0 && intensity_peakB == 0) {
                control = true;
            }
            if (!control) {
                if (i == specASum.length - 1) {// make sure last one is not empty..
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
