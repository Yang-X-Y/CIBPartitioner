package cn.edu.whu.spatialJoin.spatialPartitioning.CIBP;

import org.locationtech.jts.geom.*;

import java.io.Serializable;

public class GHCIStatistic implements Serializable {

    private Envelope env; // 区域跨度

    private double cellLen; // 格网单元边长

    private boolean isPoint; // 是否为点数据

    private double cellArea; // 格网单元面积

    private int width; // 横向格网数量

    private int height; // 纵向格网数量

    private int MinH=Integer.MAX_VALUE; // 包含数据的格网起始横坐标

    private int MinW=Integer.MAX_VALUE; // 包含数据的格网起始纵坐标

    private int MaxH=-1; // 包含数据的格网终止横坐标

    private int MaxW=-1; // 包含数据的格网终止纵坐标

    private int[][] histogramN; // 对象数量

    private double[][] histogramS; // 对象点数量

    private int[][] histogramC; // 角点数量

    private double[][] histogramO; // 相交面积和

    private double[][] histogramH; // 水平线长度和

    private double[][] histogramV; // 垂线长度和

    public GHCIStatistic(int cellNum) {
        this(new Envelope(-180.0,180.0,-90.0,90.0), cellNum, false);
    }

    public GHCIStatistic(int cellNum, boolean isPoint) {
        this(new Envelope(-180.0,180.0,-90.0,90.0), cellNum, isPoint);
    }

    public GHCIStatistic(Envelope env, int cellNum, boolean isPoint) {
        this.env = env;
        this.cellLen = Math.max(env.getHeight(), env.getWidth()) / cellNum;
        this.cellArea = cellLen*cellLen;
        this.isPoint=isPoint;
        if (env.getWidth() > env.getHeight()){
            this.width = cellNum;
            this.height = (int)Math.ceil(env.getHeight() / cellLen);
        }else{
            this.height = cellNum;
            this.width = (int)Math.ceil(env.getWidth() / cellLen);
        }
        this.histogramN = new int[height][width];
        if (!isPoint) {
            this.histogramS = new double[height][width];
            this.histogramC = new int[height][width];
            this.histogramO = new double[height][width];
            this.histogramH = new double[height][width];
            this.histogramV = new double[height][width];
        }
    }

    public void stat(Geometry geom){
        if (this.isPoint) {
            this.stat(geom.getCoordinate());
        }else {
            this.stat(geom.getEnvelopeInternal(),geom.getNumPoints());
        }
    }

    public void stat(Envelope insertMBR, int size){

        int envMinW = insertW(insertMBR.getMinX());
        int envMinH = insertH(insertMBR.getMinY());
        int envMaxW = insertW(insertMBR.getMaxX());
        int envMaxH = insertH(insertMBR.getMaxY());

//        double N = 1.0 / ((envMaxH - envMinH + 1) * (envMaxW - envMinW + 1));
        for (int wIndex=envMinW;wIndex<=envMaxW;wIndex++) {
            for (int hIndex=envMinH;hIndex<=envMaxH;hIndex++) {
//                if (wIndex<0 || wIndex>=this.width || hIndex<0 || hIndex>=this.height) {
//                    System.out.println("arrayout:"+insertMBR);
//                    continue;
//                }
                this.MinH=Math.min(this.MinH,hIndex);
                this.MinW=Math.min(this.MinW,wIndex);
                this.MaxH=Math.max(this.MaxH,hIndex);
                this.MaxW=Math.max(this.MaxW,wIndex);

                int C;
                double O;
                double H;
                double V;
                Envelope cell = getSubEnv(hIndex, wIndex);
                Envelope commonMBR = cell.intersection(insertMBR);
                O=commonMBR.getArea();
                if (O<=0) {continue;}
//                double N = O/insertMBR.getArea();
                if (commonMBR.getWidth()==cellLen) {
                    C=0;
                    V=0;
                    if (commonMBR.getHeight()==cellLen) {
                        H=0;
                    } else if (commonMBR.getHeight() == insertMBR.getHeight()) {
                        H=cellLen*2;
                    } else {
                        H=cellLen;
                    }
                } else if (commonMBR.getHeight()==cellLen) {
                    C=0;
                    H=0;
                    if (commonMBR.getWidth() == insertMBR.getWidth()) {
                        V=cellLen*2;
                    } else {
                        V=cellLen;
                    }
                } else {
                    if (commonMBR.getHeight()==insertMBR.getHeight() && commonMBR.getWidth()==insertMBR.getWidth()) {
                        C=4;
                        V=commonMBR.getHeight()*2;
                        H=commonMBR.getWidth()*2;
                    } else if ((commonMBR.getHeight()==insertMBR.getHeight() && commonMBR.getWidth()<insertMBR.getWidth())) {
                        C=2;
                        V=commonMBR.getHeight()*2;
                        H=commonMBR.getWidth();
                    } else if ((commonMBR.getHeight()<insertMBR.getHeight() && commonMBR.getWidth()==insertMBR.getWidth())) {
                        C=2;
                        V=commonMBR.getHeight();
                        H=commonMBR.getWidth()*2;
                    } else {
                        C=1;
                        V=commonMBR.getHeight();
                        H=commonMBR.getWidth();
                    }
                }
//                this.stat(hIndex,wIndex,N,size*N,C,O,H,V);
                this.stat(hIndex,wIndex,1,size*O,C,O,H,V);
            }
        }
    }

    public void stat(Coordinate insertPoint){

        int wIndex = insertW(insertPoint.getX());
        int hIndex = insertH(insertPoint.getY());

        if (wIndex<0 || wIndex>=this.width || hIndex<0 || hIndex>=this.height) {
            System.out.println("arrayout:"+insertPoint);
        } else {
            this.MinH=Math.min(this.MinH,hIndex);
            this.MinW=Math.min(this.MinW,wIndex);
            this.MaxH=Math.max(this.MaxH,hIndex);
            this.MaxW=Math.max(this.MaxW,wIndex);
            this.stat(hIndex,wIndex,1);
        }
    }

    private int insertH(double y){
        int h = (int)Math.floor((y - this.env.getMinY()) / cellLen);
        if (h == height) h--;
        return h;
    }

    private int insertW(double x){
        int w = (int)Math.floor((x - this.env.getMinX()) / cellLen);
        if (w == width) w--;
        return w;
    }

    private Envelope getSubEnv(int hIndex, int wIndex) {
        double minX = this.env.getMinX() + wIndex * cellLen;
        double minY = this.env.getMinY() + hIndex * cellLen;
        double maxX = Math.min(minX+cellLen,180.0);
        double maxY = Math.min(minY+cellLen,90.0);
        return new Envelope(minX,maxX,minY,maxY);
    }

    public void stat(int hIndex, int wIndex,int N){
        histogramN[hIndex][wIndex]+=N;
    }

    public void stat(int hIndex, int wIndex,int N, double size, int C,double O,double H, double V){
        histogramC[hIndex][wIndex]+=C;
        histogramN[hIndex][wIndex]+=N;
        histogramS[hIndex][wIndex]+=size;
        histogramO[hIndex][wIndex]+=O;
        histogramH[hIndex][wIndex]+=H;
        histogramV[hIndex][wIndex]+=V;
    }

    public double[][] getHistogramS(){return histogramS;}
    public int[][] getHistogramN(){return histogramN;}
    public int[][] getHistogramC(){return histogramC;}
    public double[][] getHistogramO(){return histogramO;}
    public double[][] getHistogramH(){return histogramH;}
    public double[][] getHistogramV(){return histogramV;}

    public int getHeight() {
        return height;
    }

    public int getWidth() {
        return width;
    }

    public int getMinH() {
        return MinH;
    }

    public int getMaxH() {
        return MaxH;
    }

    public int getMinW() {
        return MinW;
    }

    public int getMaxW() {
        return MaxW;
    }

    public GHCIStatistic combine(GHCIStatistic gridHistogramStatistic){

        MinH = Math.min(MinH, gridHistogramStatistic.MinH);
        MinW = Math.min(MinW, gridHistogramStatistic.MinW);
        MaxH = Math.max(MaxH, gridHistogramStatistic.MaxH);
        MaxW = Math.max(MaxW, gridHistogramStatistic.MaxW);

        for (int i = 0; i< this.height; i++){
            for(int j = 0; j < this.width; j++){
                histogramN[i][j] += gridHistogramStatistic.histogramN[i][j];
                if (!isPoint) {
                    histogramS[i][j] += gridHistogramStatistic.histogramS[i][j];
                    histogramC[i][j] += gridHistogramStatistic.histogramC[i][j];
                    histogramO[i][j] += gridHistogramStatistic.histogramO[i][j];
                    histogramH[i][j] += gridHistogramStatistic.histogramH[i][j];
                    histogramV[i][j] += gridHistogramStatistic.histogramV[i][j];
                }
            }
        }

        return this;
    }

    public GridHistogramCI computeCost(GHCIStatistic gridHistogramStatistic, double alpha, double indexLength){

        double[][] histogramCost = new double[height][width];
        double costSum=0.0;

        int commonMinH = Math.max(this.MinH, gridHistogramStatistic.MinH);
        int commonMinW = Math.max(this.MinW, gridHistogramStatistic.MinW);
        int commonMaxH = Math.min(this.MaxH, gridHistogramStatistic.MaxH);
        int commonMaxW = Math.min(this.MaxW, gridHistogramStatistic.MaxW);

//        System.out.println("commonMinH:"+commonMinH);
//        System.out.println("commonMinW:"+commonMinW);
//        System.out.println("commonMaxH:"+commonMaxH);
//        System.out.println("commonMaxW:"+commonMaxW);

        for (int i = commonMinH; i<= commonMaxH; i++){
            for(int j = commonMinW; j <= commonMaxW; j++) {
                double cost;
                double n1 = histogramN[i][j];
                double n2 = gridHistogramStatistic.histogramN[i][j];
                double O1 = histogramO[i][j];
                double O2 = gridHistogramStatistic.histogramO[i][j];

                if (O1==0 && O2==0) {
                    cost=0.0;
                } else {
                    double meanSize1 = (O1==0?0:(histogramS[i][j]/O1));
                    double meanSize2 = (O2==0?0:(gridHistogramStatistic.histogramS[i][j]/O2));
                    double intersectNums =
                            (gridHistogramStatistic.histogramC[i][j]*histogramO[i][j]
                                    +histogramC[i][j]* gridHistogramStatistic.histogramO[i][j]
                                    +histogramH[i][j]* gridHistogramStatistic.histogramV[i][j]
                                    +histogramV[i][j]* gridHistogramStatistic.histogramH[i][j])
                                    /(4*cellArea);
//                    cost = intersectNums;
//                    cost = intersectNums*(meanSize1+meanSize2)*Math.log(meanSize1+meanSize2);
                    cost = (1.0-alpha)*(n2*Math.log(indexLength))+alpha*intersectNums*(meanSize1+meanSize2)*Math.log(meanSize1+meanSize2);
                }
                histogramCost[i][j]=cost;
                costSum+=cost;
            }
        }

        return new GridHistogramCI(env,cellLen,histogramCost,commonMinH,commonMaxH,commonMinW,commonMaxW,costSum);
    }

    public GridHistogramCI computePIPCI(GHCIStatistic ghPointStat, double alpha, double indexLength){

        double[][] histogramCost = new double[height][width];
        double costSum=0.0;

        int commonMinH = Math.max(this.MinH, ghPointStat.getMinH());
        int commonMinW = Math.max(this.MinW, ghPointStat.getMinW());
        int commonMaxH = Math.min(this.MaxH, ghPointStat.getMaxH());
        int commonMaxW = Math.min(this.MaxW, ghPointStat.getMaxW());

//        System.out.println("commonMinH:"+commonMinH);
//        System.out.println("commonMinW:"+commonMinW);
//        System.out.println("commonMaxH:"+commonMaxH);
//        System.out.println("commonMaxW:"+commonMaxW);

        for (int i = commonMinH; i<= commonMaxH; i++){
            for(int j = commonMinW; j <= commonMaxW; j++) {
                double cost;
                double nNonPoints = histogramN[i][j];
                double nPoints = ghPointStat.getHistogramN()[i][j];
                double nonPointArea = histogramO[i][j];

                if (nPoints==0 && nonPointArea==0) {
                    cost=0.0;
                } else {
                    double meanSize = (nonPointArea==0?0:(histogramS[i][j]/nonPointArea));
                    double intersectNums =
                            (nPoints*nonPointArea/cellArea);
//                    cost = intersectNums;
//                    cost = intersectNums*(meanSize1+meanSize2)*Math.log(meanSize1+meanSize2);
                    cost = (1.0-alpha)*(nPoints*Math.log(indexLength))+alpha*intersectNums*(meanSize);
                }
                histogramCost[i][j]=cost;
                costSum+=cost;
            }
        }
        return new GridHistogramCI(env,cellLen,histogramCost,commonMinH,commonMaxH,commonMinW,commonMaxW,costSum);
    }


}
