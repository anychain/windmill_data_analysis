package com.ibm.poc.utils;


public final class PocUtils {

    public static double caculateSimValues(double[] srcList){
        double cal = 0;
        double avg = caculateAvg(srcList);
        double sum1 = 0;
        for (int i = 0; i < srcList.length; i++){
        	double v1 = srcList[i] - avg;
        	sum1 += v1 * v1;
        }
        
        cal = Math.sqrt(sum1) * deltaSum(srcList, false) + deltaSum(srcList, true); 
        return cal;
    }
    
    private static double caculateAvg(double[] srcList){
    	double sum = 0;
    	for (int i = 0; i < srcList.length; i++){
    		sum += srcList[i];
    	}
    	return sum / srcList.length;
    }
    
    private static double deltaSum(double[] srcList, boolean square){
    	double sum = 0;
    	for (int i = (srcList.length - 1); i >0; i--){
    		double delta = srcList[i] - srcList[i-1];
    		if (square) delta = delta * delta;
    		sum += delta;
    	}
    	return sum;
    }
    
}