package cn.edu.whu.spatialJoin.spatialPartitioning.ADP;

import org.locationtech.jts.geom.Envelope;
import scala.Tuple3;

import java.io.Serializable;

public class GridHistogramADP implements Serializable {

    private double[][] histogramCost;

    private int hMin;

    private int hMax;

    private int wMin;

    private int wMax;

    private double costSum;

    private GridHistogramADP[] children;

    public GridHistogramADP(double[][] histogramCost, int hMin, int hMax, int wMin, int wMax, double costSum){
        this.histogramCost = histogramCost;
        this.hMin = hMin;
        this.hMax = hMax;
        this.wMin = wMin;
        this.wMax = wMax;
        this.costSum = costSum;
        this.children = new GridHistogramADP[2];
    }

    public int split(){

        if ((hMax == hMin && wMax == wMin)||costSum==0){
//            System.out.println("singleCell or emptyCell: hMin:"+hMin+" wMin:"+wMin+" hMax:"+hMax+" wMax:"+wMax+" costSum:"+costSum);
            return 0;
        }

        Tuple3<Integer, Double, Double> arrH = search(hMin, hMax, wMin, wMax,true);
        Tuple3<Integer, Double, Double> arrW = search(wMin, wMax, hMin, hMax,false);
        double minDeltaH = Math.abs(arrH._2() - arrH._3());
        double minDeltaW = Math.abs(arrW._2() - arrW._3());
        if (minDeltaW==costSum && minDeltaH==costSum) {
            return 0;
        } else if (arrW._1()==0 || (arrH._1()!=0 && minDeltaH<minDeltaW)) {
            int splitIndex = arrH._1();
            this.children[0] = new GridHistogramADP(histogramCost, hMin, splitIndex - 1, wMin, wMax,arrH._2());
            this.children[1] = new GridHistogramADP(histogramCost, splitIndex, hMax, wMin, wMax, arrH._3());
//            System.out.println("W hMin: "+hMin+" hMax: "+hMax+" wMin: "+wMin+" wMax: "+wMax+" curCost: " +this.getCostSum()+" arrH:"+arrH.toString());
            return splitIndex;
        } else {
            int splitIndex = arrW._1();
            this.children[0] = new GridHistogramADP(histogramCost, hMin, hMax, wMin, splitIndex - 1, arrW._2());
            this.children[1] = new GridHistogramADP(histogramCost, hMin, hMax, splitIndex, wMax, arrW._3());
//            System.out.println("W hMin: "+hMin+" hMax: "+hMax+" wMin: "+wMin+" wMax: "+wMax+" curCost: " +this.getCostSum()+" arrW:"+arrW.toString());
            return -splitIndex;
        }
    }

    private Tuple3<Integer,Double,Double> search(int min1, int max1, int min2, int max2, boolean b){
        double leftSum = 0.0;
        double rightSum = 0.0;
        double minDelta = Double.MAX_VALUE;
        int splitIndex = min1;
        double sum1 = 0;
        double sum2 = 0;
        if (min1==max1) {return new Tuple3<>(0, 0.0, 0.0);}
        for(int i = min1; i < max1; i++){
            for(int j = min2; j <= max2; j++){
                leftSum += b ? histogramCost[i][j] : histogramCost[j][i];
            }
            rightSum = costSum - leftSum;
            if (Math.abs(leftSum - rightSum) <= minDelta){
                minDelta = Math.abs(leftSum - rightSum);
                splitIndex = i + 1;
                sum1 = leftSum;
                sum2 = rightSum;
            }else {
                break;
            }
        }
        return new Tuple3<>(splitIndex, sum1, sum2);
    }

    public int getHMin() {
        return hMin;
    }

    public int getHMax() {
        return hMax;
    }

    public int getWMin() {
        return wMin;
    }

    public int getWMax() {
        return wMax;
    }

    public Envelope getEnvelope(Envelope envelope,double cellLen) {
        double minX = envelope.getMinX() + wMin * cellLen;
        double maxX = minX + (wMax - wMin + 1) * cellLen;
        double minY = envelope.getMinY() + hMin * cellLen;
        double maxY = minY + (hMax - hMin + 1) * cellLen;
        return new Envelope(minX,maxX,minY,maxY);
    }

    public int[] getRange() {

        return new int[]{hMin,hMax,wMin,wMax};
    }

    public GridHistogramADP[] getChildren() { return children; }

    public double getCostSum() {
        return costSum;
    }

}
