package cn.edu.whu.spatialJoin.JTS;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.geom.impl.CoordinateArraySequence;
import org.locationtech.jts.io.ParseException;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class JTSLineIntersectionTestNew {

    // 数量指标
    public static int pointSum = 0;
    public static int totalCount = 0;
    public static int AVGGridsCount = 0;
    public static int AVGCoveredGridsCount = 0;
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
        boolean isJTS = Boolean.parseBoolean(args[0]);
        int SEG = Integer.parseInt(args[1]);
//        boolean isJTS = false;
//        int SEG = 10;
        int maxGridLevel = 31;
        int dataset1RecursiveTimes = 7;
        int dataset2RecursiveTimes = 0;
        List<Geometry> NewYorkDistricts = new ArrayList<>();
        long t = System.currentTimeMillis();
//        String file1="D:\\data\\NewYorkData\\NewYorkDistrict.json";
//        String file2="D:\\data\\NewYorkData\\NewYorkRoads.json";
//
        String file1="/home/yxy/gridMesa/data/compareJTS/NewYorkDistrict.json";
        String file2="/home/yxy/gridMesa/data/compareJTS/NewYorkRoads.json";

        BufferedReader read1 = new BufferedReader(new FileReader(file1));
        String str1;
        while ((str1 = read1.readLine()) != null) {
            Polygon geom = parsePolygon(str1, false);
            if (isJTS){
                NewYorkDistricts.add(geom);
            } else {
//                NewYorkDistricts.add(parseGridPolygon(geom, maxGridLevel,dataset1RecursiveTimes));
                NewYorkDistricts.add(parseGridPolygon(geom, SEG));
            }
        }
        int pointSum1 = pointSum;
        int count1 = NewYorkDistricts.size();
        int count2 = 0;
        HashMap<Integer, Integer> count = new HashMap<>();
        try {
            BufferedReader read2 = new BufferedReader(new FileReader(file2));
            String str;
            while ((str = read2.readLine()) != null) {
                count2++;
                LineString g1 = parseLineString(str, true);
                int g2_i = 0;
                for (Geometry g2:NewYorkDistricts){
                    g2_i+=1;
                    totalCount++;
                    if(MBRFilter(g1, g2)) {
                        int value = count.getOrDefault(g2_i,0)+1;
                        count.put(g2_i,value);
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
//                            g1 = parseGridLineString(g1,maxGridLevel,dataset2RecursiveTimes);
                            g1 = parseGridLineString(g1,SEG);
                            long gridFilterStartTime = System.currentTimeMillis();
                            Grid[] filterGrids = GridLineIntersection2.gridFilter((GridLineString) g1, (GridPolygon) g2);
//                            ArrayList<Tuple2<Grid,Grid>> filterGrids = GridLineIntersection2.gridFilter2((GridLineString) g1, (GridPolygon) g2);
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
//                                boolean gridResult = GridLineIntersection2.gridRefine3((GridLineString) g1, (GridPolygon) g2, filterGrids);
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
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        int pointSum2 = pointSum-pointSum1;

        System.out.println("行政区数量: "+count1+" 平均点数: " + pointSum1 / count1);
        System.out.println("数据集2数据量:"+count2+" 平均点数: " + pointSum2 / count2);
        System.out.println("isJTS: " + isJTS);
        System.out.println("maxGridLevel: " + maxGridLevel);
        System.out.println("dataset1RecursiveTimes: " + dataset1RecursiveTimes);
        System.out.println("dataset2RecursiveTimes: " + dataset2RecursiveTimes);
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
        count.forEach((key,value)->System.out.println("g2:"+key+"\t count:"+value));
    }

    public static Polygon parsePolygon(String json, boolean isMultiGeom) {
        List<Coordinate> cos = new ArrayList<Coordinate>();
        JSONObject jsonObject = JSONObject.parseObject(json);
        JSONObject geometry = jsonObject.getJSONObject("geometry");
        JSONArray co = geometry.getJSONArray("coordinates");
        JSONArray coordinates=null;
        if (isMultiGeom) {
            coordinates = co.getJSONArray(0).getJSONArray(0);
        } else {
            coordinates = co.getJSONArray(0);
        }
        pointSum += coordinates.size();
        Coordinate firstPoint = new Coordinate(coordinates.getJSONArray(0).getDouble(0), coordinates.getJSONArray(0).getDouble(1));
        cos.add(firstPoint);
        for (int i = 1; i < coordinates.size(); i++) {
            JSONArray coordinate = coordinates.getJSONArray(i);
            double lng = coordinate.getDouble(0);
            double lat = coordinate.getDouble(1);
            Coordinate currentPoint = new Coordinate(lng, lat);
            cos.add(new Coordinate(lng, lat));
            if (currentPoint.equals2D(firstPoint)) {
                break;
            }
        }
        LinearRing shell = new LinearRing(new CoordinateArraySequence(cos.toArray(new Coordinate[0])), new GeometryFactory());
        return new Polygon(shell, null, new GeometryFactory());
    }

    public static LineString parseLineString(String json, boolean isMultiGeom) {
        List<Coordinate> cos = new ArrayList<Coordinate>();
        JSONObject jsonObject = JSONObject.parseObject(json);
        JSONObject geometry = jsonObject.getJSONObject("geometry");
        JSONArray co = geometry.getJSONArray("coordinates");
        JSONArray coordinates=null;
        coordinates = co.getJSONArray(0);
        pointSum += coordinates.size();
        for (int i = 0; i < coordinates.size(); i++) {
            JSONArray coordinate = coordinates.getJSONArray(i);
            double lng = coordinate.getDouble(0);
            double lat = coordinate.getDouble(1);
            cos.add(new Coordinate(lng, lat));
        }
        return new LineString(new CoordinateArraySequence(cos.toArray(new Coordinate[0])), new GeometryFactory());
    }

    public static GridPolygon parseGridPolygon(Polygon polygon, int level, int recursiveTimes) {
        LinearRing shell = polygon.getExteriorRing();
        long t = System.currentTimeMillis();
        GridPolygon gridPolygon = new GridPolygon(shell, null, new GeometryFactory(), level, recursiveTimes);
        initGridObjectTime += System.currentTimeMillis() - t;
        return gridPolygon;
    }

    public static GridPolygon parseGridPolygon(Polygon polygon, int SEG) {
        LinearRing shell = polygon.getExteriorRing();
        long t = System.currentTimeMillis();
        GridPolygon gridPolygon = new GridPolygon(shell, null, new GeometryFactory(),SEG);
        initGridObjectTime += System.currentTimeMillis() - t;
        return gridPolygon;
    }

    public static GridLineString parseGridLineString(LineString lineString, int maxGridLevel, int recursiveTimes) {
        CoordinateSequence points = lineString.getCoordinateSequence();
        long t = System.currentTimeMillis();
        GridLineString gridLineString = new GridLineString(points, new GeometryFactory(), maxGridLevel, recursiveTimes);
        initGridObjectTime += System.currentTimeMillis() - t;
        return gridLineString;
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