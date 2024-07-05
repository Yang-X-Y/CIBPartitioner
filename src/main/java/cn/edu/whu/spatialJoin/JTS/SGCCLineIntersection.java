package cn.edu.whu.spatialJoin.JTS;

import org.locationtech.jts.geom.*;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class SGCCLineIntersection {

    // 数量指标
    public static int pointSum = 0;
    public static int totalCount = 0;
    public static int MBRFilterCount = 0;
    public static int gridFilterCount = 0;
    public static int trueCount = 0;
    public static int trueHit = 0;
    public static int falseHit = 0;
    public static int uncertain = 0;

    // 时间指标
    public static long initGridObjectTime = 0;
    public static int MBRFilterTime = 0;
    public static long JTSRefineTime = 0;
    public static int gridFilterTime = 0;
    public static int gridRefineTime = 0;

    public static void main(String[] args) throws IOException, ParseException {

        boolean isJTS = true;
        int SEG = 10;

        List<Geometry> NewYorkDistricts = new ArrayList<>();
        long t = System.currentTimeMillis();

        String file1="D:\\data\\sgcc\\fujianDistrictsWKT.txt";
        String file2="D:\\data\\sgcc\\polylineWGS84.txt";

        BufferedReader read1 = new BufferedReader(new FileReader(file1));
        String str1;
        while ((str1 = read1.readLine()) != null) {
            Geometry geometries = parseWKT(str1);
            int geomNum = geometries.getNumGeometries();
            for (int i=0;i<geomNum;i++){
                Polygon geom = (Polygon) geometries.getGeometryN(i);
                pointSum+=geom.getNumPoints();
                if (isJTS){
                    NewYorkDistricts.add(geom);
                } else {
                    NewYorkDistricts.add(parseGridPolygon(geom, SEG));
                }
            }
        }

        read1.close();
        int pointSum1 = pointSum;
        int count1 = NewYorkDistricts.size();
        int count2 = 0;
        try {
            BufferedReader read2 = new BufferedReader(new FileReader(file2));
            String str;
            while ((str = read2.readLine()) != null) {

                Geometry g1Collection = parseWKT(str);
                int n = g1Collection.getNumGeometries();
                count2+=n;
                for (int i=0;i<n;i++){
                    Geometry g1 = g1Collection.getGeometryN(i);
                    pointSum+=g1.getNumPoints();
                    for (Geometry g2:NewYorkDistricts){
                        totalCount++;
                        if(MBRFilter(g1, g2)) {
                            MBRFilterCount++;
                            if (isJTS) {
                                long startTime = System.currentTimeMillis();
                                boolean JTSResult = g2.intersects(g1);
                                long endTime = System.currentTimeMillis();
                                JTSRefineTime += endTime - startTime;
                                if (JTSResult) {
                                    trueCount++;
                                }
                            } else {
                                g1 = parseGridLineString((LineString) g1,SEG);
                                long gridFilterStartTime = System.currentTimeMillis();
                                Grid[] filterGrids = GridLineIntersection2.gridFilter((GridLineString) g1, (GridPolygon) g2);
                                long gridFilterEndTime = System.currentTimeMillis();
                                gridFilterTime += gridFilterEndTime - gridFilterStartTime;
                                if (filterGrids == null) {
                                    gridFilterCount++;
                                    trueCount++;
                                    trueHit++;
                                } else if (filterGrids.length != 0){
                                    gridFilterCount++;
                                    uncertain++;
                                    long gridRefineStartTime = System.currentTimeMillis();
                                    boolean gridResult = GridLineIntersection2.gridRefine((GridLineString) g1, (GridPolygon) g2, filterGrids);
                                    long gridRefineEndTime = System.currentTimeMillis();
                                    gridRefineTime += gridRefineEndTime - gridRefineStartTime;
                                    if (gridResult) {
                                        trueCount++;
                                    }
                                } else {
                                    falseHit++;
                                }
                            }
                        }
                    }

                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        int pointSum2 = pointSum-pointSum1;

        System.out.println("行政区数量: "+count1+" 平均点数: " + pointSum1 / count1);
        System.out.println("数据集2数据量:"+count2+" 平均点数: " + pointSum2 / count2);
        System.out.println("isJTS: " + isJTS);
        System.out.println("SEG: " + SEG);
        System.out.println("总数据对: " + totalCount);
        System.out.println("MBR相交数据对: " + MBRFilterCount);
        System.out.println("格网相交数据对: " + gridFilterCount);
        System.out.println("真实相交数据量: " + trueCount);
        System.out.println("MBR过滤耗时: " + MBRFilterTime+ " ms");
        System.out.println("JTSRefine耗时：" + JTSRefineTime + " ms");
        System.out.println("创建格网对象时间: " + initGridObjectTime+" ms");
        System.out.println("GridFilter耗时：" + gridFilterTime + " ms");
        System.out.println("gridRefine耗时：" + gridRefineTime + " ms");
        System.out.println("MBRTrueHit: " + (double) trueCount/MBRFilterCount);
        System.out.println("GridTrueHit：" + (double) trueCount/gridFilterCount);
        System.out.println("trueHit：" + (double) trueHit/MBRFilterCount);
        System.out.println("falseHit：" + (double) falseHit/MBRFilterCount);
        System.out.println("uncertain：" + (double) uncertain/MBRFilterCount);
        System.out.println("总耗时: " + (System.currentTimeMillis() - t) + "ms");
    }

    public static Geometry parseWKT(String line) throws ParseException {
        WKTReader wktReader = new WKTReader();
        Geometry geometry = wktReader.read(line);
        return geometry;
    }


    public static GridPolygon parseGridPolygon(Polygon polygon, int SEG) {
        LinearRing shell = polygon.getExteriorRing();
        long t = System.currentTimeMillis();
        GridPolygon gridPolygon = new GridPolygon(shell, null, new GeometryFactory(),SEG);
        initGridObjectTime += System.currentTimeMillis() - t;
        return gridPolygon;
    }


    public static GridLineString parseGridLineString(LineString lineString, int SEG) {
        CoordinateSequence points = lineString.getCoordinateSequence();
        long t = System.currentTimeMillis();
        GridLineString gridLineString = new GridLineString(points, new GeometryFactory(), SEG);
        initGridObjectTime += System.currentTimeMillis() - t;
        return gridLineString;
    }

    public static boolean MBRFilter(Geometry g1, Geometry g2) {
        long start = System.currentTimeMillis();
        boolean bboxIntersection = g1.getEnvelopeInternal().intersects(g2.getEnvelopeInternal());
        long end = System.currentTimeMillis();
        MBRFilterTime += end - start;
        return bboxIntersection;
    }

}