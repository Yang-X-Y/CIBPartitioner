package cn.edu.whu.spatialJoin.spatialPartitioning.CIBP;

import org.locationtech.jts.geom.Envelope;
import scala.Tuple3;

import java.io.Serializable;

public class GridHistogramCI implements Serializable {

    private Envelope env; // 区域跨度

    private double cellLen; // 格网单元边长

    private double[][] histogramCost;

    private int hMin;

    private int hMax;

    private int wMin;

    private int wMax;

    private double costSum;

    private GridHistogramCI[] children;

    public GridHistogramCI(Envelope env, double cellLen, double[][] histogramCost, int hMin, int hMax, int wMin, int wMax, double costSum){
        this.histogramCost = histogramCost;
        this.env = env;
        this.cellLen = cellLen;
        this.hMin = hMin;
        this.hMax = hMax;
        this.wMin = wMin;
        this.wMax = wMax;
        this.costSum = costSum;
        this.children = new GridHistogramCI[2];
    }

    public int split1(){

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
            this.children[0] = new GridHistogramCI(env,cellLen,histogramCost, hMin, splitIndex - 1, wMin, wMax,arrH._2());
            this.children[1] = new GridHistogramCI(env,cellLen,histogramCost, splitIndex, hMax, wMin, wMax, arrH._3());
//            System.out.println("W hMin: "+hMin+" hMax: "+hMax+" wMin: "+wMin+" wMax: "+wMax+" curCost: " +this.getCostSum()+" arrH:"+arrH.toString());
            return splitIndex;
        } else {
            int splitIndex = arrW._1();
            this.children[0] = new GridHistogramCI(env,cellLen,histogramCost, hMin, hMax, wMin, splitIndex - 1, arrW._2());
            this.children[1] = new GridHistogramCI(env,cellLen,histogramCost, hMin, hMax, splitIndex, wMax, arrW._3());
//            System.out.println("W hMin: "+hMin+" hMax: "+hMax+" wMin: "+wMin+" wMax: "+wMax+" curCost: " +this.getCostSum()+" arrW:"+arrW.toString());
            return -splitIndex;
        }
    }

    /**
     *
     * @return 正则在Height上split，否则在Width上split, 0则不可分裂
     */
    public int split(){

        if (hMax - hMin == 0 && wMax - wMin == 0){
//            System.out.println("singleCell: H "+hMin+" W:"+wMax);
            return 0;
        }

        if (hMax - hMin > wMax - wMin){
            Tuple3<Integer, Double, Double> arr = search(hMin, hMax, wMin, wMax, true);
//            System.out.println("H hMin: "+hMin+" hMax: "+hMax+" wMin: "+wMin+" wMax: "+wMax+" curCost: " +this.getCostSum()+" arr:"+arr.toString());
            int splitIndex = arr._1();
            if (arr._2()>0) { this.children[0] = new GridHistogramCI(env,cellLen,histogramCost, hMin, splitIndex - 1, wMin, wMax,arr._2()); }
            if (arr._3()>0) { this.children[1] = new GridHistogramCI(env,cellLen,histogramCost, splitIndex, hMax, wMin, wMax, arr._3()); }
            return splitIndex;
        }else{
            Tuple3<Integer, Double, Double> arr = search(wMin, wMax, hMin, hMax,false);
//            System.out.println("W hMin: "+hMin+" hMax: "+hMax+" wMin: "+wMin+" wMax: "+wMax+" curCost: " +this.getCostSum()+" arr:"+arr.toString());
            int splitIndex = arr._1();
            if (arr._2()>0) { this.children[0] = new GridHistogramCI(env,cellLen,histogramCost, hMin, hMax, wMin, splitIndex - 1, arr._2()); }
            if (arr._3()>0) { this.children[1] = new GridHistogramCI(env,cellLen,histogramCost, hMin, hMax, splitIndex, wMax, arr._3()); }
            return -splitIndex;
        }
    }

    public int split2(){

        if ((hMax == hMin && wMax == wMin)||costSum==0){
//            System.out.println("singleCell or emptyCell: hMin:"+hMin+" wMin:"+wMin+" hMax:"+hMax+" wMax:"+wMax+" costSum:"+costSum);
            return 0;
        }

        Tuple3<Integer, Double, Double> arrH = search2(hMin, hMax, wMin, wMax,true);
        Tuple3<Integer, Double, Double> arrW = search2(wMin, wMax, hMin, hMax,false);
        double minDeltaH = Math.abs(arrH._2() - arrH._3());
        double minDeltaW = Math.abs(arrW._2() - arrW._3());
        if (minDeltaW==costSum && minDeltaH==costSum) {
            return 0;
        } else if (arrW._1()==0 || (arrH._1()!=0 && minDeltaH<minDeltaW)) {
            int splitIndex = arrH._1();
            this.children[0] = new GridHistogramCI(env,cellLen,histogramCost, hMin, splitIndex - 1, wMin, wMax,arrH._2());
            this.children[1] = new GridHistogramCI(env,cellLen,histogramCost, splitIndex, hMax, wMin, wMax, arrH._3());
//            System.out.println("W hMin: "+hMin+" hMax: "+hMax+" wMin: "+wMin+" wMax: "+wMax+" curCost: " +this.getCostSum()+" arrH:"+arrH.toString());
            return splitIndex;
        } else {
            int splitIndex = arrW._1();
            this.children[0] = new GridHistogramCI(env,cellLen,histogramCost, hMin, hMax, wMin, splitIndex - 1, arrW._2());
            this.children[1] = new GridHistogramCI(env,cellLen,histogramCost, hMin, hMax, splitIndex, wMax, arrW._3());
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

    private Tuple3<Integer,Double,Double> search2(int min1, int max1, int min2, int max2, boolean b){
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

    public Envelope getEnv() {
        return env;
    }

    public double getCellLen() { return cellLen; }

    public GridHistogramCI[] getChildren() { return children; }

    public double getCostSum() {
        return costSum;
    }

    public double[][] getHistogramCost() {
        return histogramCost;
    }
}
