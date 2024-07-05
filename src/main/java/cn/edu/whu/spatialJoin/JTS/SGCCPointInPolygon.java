package cn.edu.whu.spatialJoin.JTS;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.locationtech.jts.algorithm.locate.SimplePointInAreaLocator;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.geom.impl.CoordinateArraySequence;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SGCCPointInPolygon {

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

        boolean isJTS = true;
        int SEG = 10;

        int gridPointLevel = 24;
        List<Geometry> NewYorkDistricts = new ArrayList<>();
        long t = System.currentTimeMillis();

        String file1="D:\\data\\sgcc\\fujianDistrictsWKT.txt";
        String file2="D:\\data\\sgcc\\pointWGS84.txt";

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
                Geometry g1 = parseWKT(str);
                if (g1==null) {continue;}
                pointSum+=g1.getNumPoints();
                count2++;
                for (Geometry g2:NewYorkDistricts){
                    totalCount++;
                    if(MBRFilter(g1, g2)) {
                        MBRFilterCount++;
                        if (isJTS) {
                            long startTime = System.currentTimeMillis();
                            boolean JTSResult = g2.contains(g1);
                            long endTime = System.currentTimeMillis();
                            JTSRefineTime += endTime - startTime;
                            if (JTSResult) {
                                trueCount++;
                            }
                        } else {
                            g1 = parseGridPoint(g1,gridPointLevel);
                            long gridFilterStartTime = System.currentTimeMillis();
                            int position = gridFilter((GridPoint) g1, (GridPolygon) g2);
                            long gridFilterEndTime = System.currentTimeMillis();
                            gridFilterTime += (gridFilterEndTime - gridFilterStartTime);
                            if (position == -1) {
                                falseHit++;
                                continue;
                            } else if (((GridPolygon) g2).grids[position].contain) {
                                trueHit++;
                                gridFilterCount++;
                                trueCount++;
                            } else {
                                uncertain++;
                                gridFilterCount++;
                                /** refinement step **/
                                long start = System.currentTimeMillis();
                                boolean gridResult = SimplePointInAreaLocator.containsPointInPolygon(g1.getCoordinate(), (Polygon) g2);
                                long end = System.currentTimeMillis();
                                gridRefineTime += end - start;
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
        System.out.println("SEG: " + SEG);
        System.out.println("gridPointLevel: " + gridPointLevel);
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
        System.out.println("trueHit：" + (double) trueHit/MBRFilterCount);
        System.out.println("falseHit：" + (double) falseHit/MBRFilterCount);
        System.out.println("uncertain：" + (double) uncertain/MBRFilterCount);
        System.out.println("总耗时: " + (System.currentTimeMillis() - t) + "ms");
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

    public static Geometry parseWKT(String line) throws ParseException {
        WKTReader wktReader = new WKTReader();
        Geometry geometry = wktReader.read(line);
        return geometry;
    }

    public static Point parsePoint(String line) throws IOException {
        try {
            String[] lineSplit = line.split(",");
            double lng = Double.parseDouble(lineSplit[5]);
            double lat = Double.parseDouble(lineSplit[6]);
            if (lng > 180 || lng < -180 || lat > 90 || lat < -90){
                return null;
            }
            List<Coordinate> co = new ArrayList<>();
            co.add(new Coordinate(lng, lat));
            pointSum+=1;
            return new Point(new CoordinateArraySequence(co.toArray(new Coordinate[0])), new GeometryFactory());
        } catch(Exception e) {
            return null;
        }

    }

    public static GridPolygon parseGridPolygon(Polygon polygon, int SEG) {
        LinearRing shell = polygon.getExteriorRing();
        long t = System.currentTimeMillis();
        GridPolygon gridPolygon = new GridPolygon(shell, null, new GeometryFactory(),SEG);
        initGridObjectTime += System.currentTimeMillis() - t;
        return gridPolygon;
    }

    public static Point parseGridPoint(Geometry point, int level) {
        List<Coordinate> co = new ArrayList<>();
        co.add(point.getCoordinate());
        long t = System.currentTimeMillis();
        GridPoint gridPoint = new GridPoint(new CoordinateArraySequence(co.toArray(new Coordinate[0])), new GeometryFactory(), level, "0");
        initGridObjectTime += System.currentTimeMillis() - t;
        return gridPoint;
    }


    public static boolean MBRFilter(Geometry g1, Geometry g2) {
        long start = System.currentTimeMillis();
        boolean bboxIntersection = g2.getEnvelopeInternal().contains(g1.getEnvelopeInternal());
        long end = System.currentTimeMillis();
        MBRFilterTime += end - start;
        return bboxIntersection;
    }

    public static int gridFilter(GridPoint g1, GridPolygon g2) {
        /** filter step **/
        Grid target = g1.grid;
        Grid[] nums = g2.grids;
        int pivot, left = 0, right = nums.length - 1;
        if (nums.length == 0 || target.compareTo(nums[0]) < 0 || target.compareTo(nums[nums.length - 1]) > 0) {
            return -1;
        }
        while (left <= right) {
            pivot = left + (right - left) / 2;
            if (nums[pivot].compareTo(target) == 0) return pivot;
            if (target.compareTo(nums[pivot]) < 0) right = pivot - 1;
            else left = pivot + 1;
        }
        return -1;
    }


}