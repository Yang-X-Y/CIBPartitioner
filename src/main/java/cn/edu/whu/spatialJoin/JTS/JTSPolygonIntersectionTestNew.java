package cn.edu.whu.spatialJoin.JTS;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.geom.impl.CoordinateArraySequence;
import org.locationtech.jts.io.ParseException;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class JTSPolygonIntersectionTestNew {

    // 数量指标
    public static int pointSum = 0;
    public static int totalCount = 0;
    public static int AVGGridsCount = 0;
    public static int AVGCoveredGridsCount = 0;
    public static int MBRFilterCount = 0;
    public static int gridFilterCount = 0;
    public static int trueCount = 0;
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
        int dataset1RecursiveTimes = 5;
        int dataset2RecursiveTimes = 0;

        List<Geometry> NewYorkDistricts = new ArrayList<>();
        long t = System.currentTimeMillis();
        //        String file1="D:\\data\\NewYorkData\\NewYorkDistrict.json";
//        String file2="D:\\data\\NewYorkData\\NewYorkBuildings.json";
//
        String file1="/home/yxy/gridMesa/data/compareJTS/NewYorkDistrict.json";
        String file2="/home/yxy/gridMesa/data/compareJTS/NewYorkBuildings.json";

        BufferedReader read1 = new BufferedReader(new FileReader(file1));
        String str1;
        while ((str1 = read1.readLine()) != null) {
            Polygon geom = parse(str1, false);
            if (isJTS){
                NewYorkDistricts.add(geom);
            } else {
//                NewYorkDistricts.add(parseGrid1(geom, 31,dataset1RecursiveTimes));
                NewYorkDistricts.add(parseGrid2(geom,"1", SEG));
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
                Polygon g1 = parse(str, true);
                int g2_i = 0;
                for (Geometry g2:NewYorkDistricts){
                    totalCount++;
                    g2_i+=1;
                    if(MBRFilter(g1, g2)) {
                        int value = count.getOrDefault(g2_i,0)+1;
                        count.put(g2_i,value);
                        MBRFilterCount++;
                        if (isJTS) {
                            long startTime = System.currentTimeMillis();
                            boolean JTSResult = g1.intersects(g2);
                            long endTime = System.currentTimeMillis();
                            JTSRefineTime += endTime - startTime;
                            if (JTSResult) {
                                trueCount++;
                            }
                        } else {
//                            g1 = parseGrid1(g1,31,dataset2RecursiveTimes);
                            g1 = parseGrid2(g1,"0",SEG);
                            long gridFilterStartTime = System.currentTimeMillis();
                            Grid[] filterGrids = GridPolygonIntersection.gridFilter((GridPolygon) g1, (GridPolygon) g2);
                            long gridFilterEndTime = System.currentTimeMillis();
                            gridFilterTime += gridFilterEndTime - gridFilterStartTime;
                            if (filterGrids == null) {
                                gridFilterCount++;
                                trueCount++;
                            } else if (filterGrids.length != 0){
                                gridFilterCount++;
                                long gridRefineStartTime = System.currentTimeMillis();
                                boolean gridResult = GridPolygonIntersection.gridRefine((GridPolygon) g1, (GridPolygon) g2, filterGrids);
                                long gridRefineEndTime = System.currentTimeMillis();
                                gridRefineTime += gridRefineEndTime - gridRefineStartTime;
                                if (gridResult) {
                                    trueCount++;
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
        System.out.println("maxGridLevel: " + maxGridLevel);
        System.out.println("dataset1RecursiveTimes: " + dataset1RecursiveTimes);
        System.out.println("dataset2RecursiveTimes: " + dataset2RecursiveTimes);
        System.out.println("总数据对: " + totalCount);
        System.out.println("MBR相交数据对: " + MBRFilterCount);
        System.out.println("真实相交数据对: " + trueCount);
        System.out.println("MBR过滤耗时: " + MBRFilterTime+ " ms");
        System.out.println("JTSRefine耗时：" + JTSRefineTime + " ms");
        System.out.println("创建格网对象时间: " + initGridObjectTime+" ms");
        System.out.println("GridFilter耗时：" + gridFilterTime + " ms");
        System.out.println("gridRefine耗时：" + gridRefineTime + " ms");
        System.out.println("MBRTrueHit: " + (double) trueCount/MBRFilterCount);
        System.out.println("GridTrueHit：" + (double) trueCount/gridFilterCount);
        System.out.println("总耗时: " + (System.currentTimeMillis() - t) + "ms");
        count.forEach((key,value)->System.out.println("g2:"+key+"\t count:"+value));
    }

//    public static void main(String[] args) throws IOException, ParseException {
//        long t = System.currentTimeMillis();
//        boolean isJTS = false;
//        int maxGridLevel = 31;
//        int recursiveTimes = 7;
//        Geometry[] polygons = new Geometry[2];
//        int flag = 0;
//        int doIntersection = -1;
//        try {
//            BufferedReader in = new BufferedReader(new FileReader("D:\\data\\NewYorkData\\NewYorkLandUse.json"));
//            String str;
//            if (isJTS){
//                while ((str = in.readLine()) != null) {
//                    totalCount++;
//                    polygons[flag] = parse(str);
//                    doIntersection++;
//                    if (doIntersection > 0) {
//                        JTSIntersect(polygons[0], polygons[1]);
//                    }
//                    flag = (flag == 0) ? 1 : 0;
//                }
//            } else {
//                while ((str = in.readLine()) != null) {
//                    totalCount++;
//                    polygons[flag] = parseGrid(str, maxGridLevel,recursiveTimes);
//                    doIntersection++;
//                    if (doIntersection > 0) {
//                        gridIntersect(polygons[0], polygons[1]);
//                    }
//                    flag = (flag == 0) ? 1 : 0;
//                }
//            }
//        } catch (FileNotFoundException e) {
//            e.printStackTrace();
//        }
//
//        System.out.println("isJTS: " + isJTS);
//        System.out.println("maxGridLevel: " + maxGridLevel);
//        System.out.println("recursiveTimes: " + recursiveTimes);
//        System.out.println("总数据量: " + totalCount);
//        System.out.println("有效数据量: " + MBRFilterCount);
//        System.out.println("创建对象时间: " + initObjectTime+" ms");
//        System.out.println("平均点数: " + pointSum / totalCount);
//        System.out.println("相交数据量: " + trueCount);
//        System.out.println("MBR-TrueHit: " + (double) trueCount/MBRFilterCount);
//        System.out.println("MBR过滤耗时: " + MBRFilterTime+ " ms");
//        System.out.println("JTSRefine耗时：" + JTSRefineTime + " ms");
//        System.out.println("GridFilter耗时：" + gridFilterTime + " ms");
//        System.out.println("gridRefine耗时：" + gridRefineTime + " ms");
//        System.out.println("Grid-TrueHit：" + (double) trueCount/gridFilterCount);
//        System.out.println("总耗时: " + (System.currentTimeMillis() - t) + "ms");
//    }


    public static Polygon parse(String json, boolean isMultiGeom) {
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

    public static GridPolygon parseGrid(String json, int level, int recursiveTimes, boolean isMultiGeom) {
        List<Coordinate> cos = new ArrayList<Coordinate>();
        JSONObject jsonObject = JSONObject.parseObject(json);
        JSONObject geometry = jsonObject.getJSONObject("geometry");
        JSONArray co = geometry.getJSONArray("coordinates");
        JSONArray coordinates = null;
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
        long t = System.currentTimeMillis();
        GridPolygon gridPolygon = new GridPolygon(shell, null, new GeometryFactory(), level, recursiveTimes);
        initGridObjectTime += System.currentTimeMillis() - t;
        return gridPolygon;
    }


    public static GridPolygon parseGrid1(Polygon polygon, int level, int recursiveTimes) {
        long t = System.currentTimeMillis();
//        GridPolygon gridPolygon = new GridPolygon(polygon.getExteriorRing(), null, new GeometryFactory(), 10,"test");
        GridPolygon gridPolygon = new GridPolygon(polygon.getExteriorRing(), null, new GeometryFactory(), level, recursiveTimes);
        initGridObjectTime += System.currentTimeMillis() - t;
        return gridPolygon;
    }

    public static GridPolygon parseGrid2(Polygon polygon, String objectId, int SEG) {
        long t = System.currentTimeMillis();
        GridPolygon gridPolygon = new GridPolygon(polygon.getExteriorRing(), null, new GeometryFactory(), objectId,SEG);
        initGridObjectTime += System.currentTimeMillis() - t;
        return gridPolygon;
    }

    public static boolean MBRFilter(Geometry g1, Geometry g2) {
        long start = System.currentTimeMillis();
        boolean bboxIntersection = g1.getEnvelopeInternal().intersects(g2.getEnvelopeInternal());
        long end = System.currentTimeMillis();
        MBRFilterTime += end - start;
        return bboxIntersection;
    }

    public static void JTSIntersect(Geometry g1, Geometry g2) {
        if(MBRFilter(g1, g2)) {
            MBRFilterCount++;
            long startTime = System.currentTimeMillis();
            boolean result = g1.intersects(g2);
            long endTime = System.currentTimeMillis();
            JTSRefineTime += endTime - startTime;
            if (result) {
                trueCount++;
            }
        }
    }

    public static void gridIntersect(Geometry g1, Geometry g2) {
        if(MBRFilter(g1, g2)) {
            MBRFilterCount++;
            long gridFilterStartTime = System.currentTimeMillis();
            Grid[] filterGrids = GridPolygonIntersection.gridFilter((GridPolygon) g1, (GridPolygon) g2);
            long gridFilterEndTime = System.currentTimeMillis();
            gridFilterTime += gridFilterEndTime - gridFilterStartTime;
            if (filterGrids == null) {
                gridFilterCount++;
                trueCount++;
            } else if (filterGrids.length != 0){
                gridFilterCount++;
                long gridRefineStartTime = System.currentTimeMillis();
                boolean gridResult = GridPolygonIntersection.gridRefine((GridPolygon) g1, (GridPolygon) g2, filterGrids);
                long gridRefineEndTime = System.currentTimeMillis();
                gridRefineTime += gridRefineEndTime - gridRefineStartTime;
                if (gridResult) {
                    trueCount++;
                }
            }
        }
    }

    public static void gridIntersect1(Geometry g1, Geometry g2) {
        if(MBRFilter(g1, g2)) {
            MBRFilterCount++;
            long gridFilterStartTime = System.currentTimeMillis();
            Grid[] filterGrids = GridPolygonIntersection.gridFilter((GridPolygon) g1, (GridPolygon) g2);
            long gridFilterEndTime = System.currentTimeMillis();
            gridFilterTime += gridFilterEndTime - gridFilterStartTime;
            if (filterGrids == null) {
                gridFilterCount++;
                trueCount++;
            } else if (filterGrids.length != 0){
                gridFilterCount++;
                long gridRefineStartTime = System.currentTimeMillis();
                boolean gridResult = GridPolygonIntersection.gridRefine((GridPolygon) g1, (GridPolygon) g2, filterGrids);
                long gridRefineEndTime = System.currentTimeMillis();
                gridRefineTime += gridRefineEndTime - gridRefineStartTime;
                if (gridResult) {
                    trueCount++;
                }
            }
        }
    }

}