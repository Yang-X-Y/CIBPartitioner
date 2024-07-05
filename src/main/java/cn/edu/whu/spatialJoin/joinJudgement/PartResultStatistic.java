package cn.edu.whu.spatialJoin.joinJudgement;

import org.locationtech.jts.geom.Envelope;

public class PartResultStatistic {
    public int partId;
    public Envelope partEnv;
    public double estimatedCost = 0.0;
    public double refineCost = 0.0;
    public double filterCost = 0.0;
    public int resultNum=0;
    public int toRefineNum=0;
    public long joinTime=0;
    public long filterTime=0;
    public long refineTime=0;
    public int indexedItemsNum=0;
    public int queryItemsNum=0;
}
