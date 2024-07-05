package cn.edu.whu.spatialJoin.spatialPartitioning.ADP;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;

import java.io.Serializable;

public class ADPStatistic implements Serializable {

    private Envelope env; // 区域跨度

    private double cellLen; // 格网单元边长

    private int width; // 横向格网数量

    private int height; // 纵向格网数量

    private int wMin;
    private int wMax;
    private int hMin;
    private int hMax;
    private double costSum;
    private double[][] histogramCost; // refine cost

    public ADPStatistic(int cellNum) {
        this(new Envelope(-180.0,180.0,-90.0,90.0), cellNum);
    }

    public ADPStatistic(Envelope env, int cellNum) {
        this.env = env;
        this.cellLen = Math.max(env.getHeight(), env.getWidth()) / cellNum;
        if (env.getWidth() > env.getHeight()){
            this.width = cellNum;
            this.height = (int)Math.ceil(env.getHeight() / cellLen);
        }else{
            this.height = cellNum;
            this.width = (int)Math.ceil(env.getWidth() / cellLen);
        }
        this.histogramCost = new double[height][width];
        this.wMin = 0;
        this.wMax = width-1;
        this.hMin = 0;
        this.hMax = height-1;
        this.costSum=0.0;
    }

    public void stat(Envelope commonMBR, double cost){
        double refPointX = commonMBR.getMinX();
        double refPointY = commonMBR.getMinY();
        int indexW = insertW(refPointX);
        int indexH = insertH(refPointY);
        histogramCost[indexH][indexW]+=cost;
        costSum+=cost;
//        System.out.println(indexH+" "+indexH+" "+cost);
    }

    public void stat(Coordinate centre, double cost){
        double pointX = centre.getX();
        double pointY = centre.getY();
        int indexW = insertW(pointX);
        int indexH = insertH(pointY);
        histogramCost[indexH][indexW]+=cost;
        costSum+=cost;
//        System.out.println(indexH+" "+indexH+" "+cost);
    }

    private int insertH(double y){
        int h = (int)Math.floor((y - this.env.getMinY()) / cellLen);
        if (h == width) h--;
        return h;
    }

    private int insertW(double x){
        int w = (int)Math.floor((x - this.env.getMinX()) / cellLen);
        if (w == height) w--;
        return w;
    }

    public int getHeight() {
        return height;
    }

    public int getWidth() {
        return width;
    }

    public Envelope getEnvelope(int h,int w) {
        double minX = env.getMinX() + w * cellLen;
        double maxX = minX + cellLen;
        double minY = env.getMinY() + h * cellLen;
        double maxY = minY + cellLen;
        return new Envelope(minX,maxX,minY,maxY);
    }

    public GridHistogramADP computeCost() {
        return new GridHistogramADP(histogramCost,hMin,hMax,wMin,wMax,costSum);
    }

    public double[][] getHistogramCost() {
        return histogramCost;
    }

    public double getCostSum() {
        return costSum;
    }
}
