package cn.edu.whu.spatialJoin.JTS;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.geom.impl.CoordinateArraySequence;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class JTSPointContainTest {
    public static int pointSum = 0;
    public static long timeSum = 0;
    public static int count = 0;
    public static double averageGrid = 0;
    public static double averageCoveredGrid = 0;
    public static int truecount = 0;
    public static int jtsbboxcost = 0;

    static BufferedWriter out = null;

    public static void main(String[] args) throws IOException, ClassNotFoundException {
        int option = Integer.parseInt(args[0]);
        Geometry[] g = new Geometry[2];
        try {
            BufferedReader in = new BufferedReader(new FileReader(args[1]));
            BufferedReader in2 = new BufferedReader(new FileReader(args[2]));
            //out = new BufferedWriter(new FileWriter("D:\\gridintersect2.txt"));
            String str, str2;
            while ((str = in.readLine()) != null) {
                if (option == 0) {
                    g[0] = parseGridPolygon(str, Integer.parseInt(args[3]));
                } else {
                    g[0] = parsePolygon(str);
                }
                for (int i = 0; i < 57708; i++) {
                    str2 = in2.readLine();
                    if (option == 0) {
                        g[1] = parseGridPoint(str2, 24);
                    } else {
                        g[1] = parsePoint(str2);
                    }
                    contain(g[0], g[1]);
                }
            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        System.out.println(count + " point in polygon use " + (int) timeSum + " ms");
        System.out.println("point per polygon: " + pointSum / 189);
        System.out.println("intersection grid per polygon: " + averageGrid / count);
        System.out.println("covered grid per polygon: " + averageCoveredGrid / count);
        System.out.println("contain true num: " + truecount);
        System.out.println("jts calculate bounding box intersection time : " + jtsbboxcost);
        GridPointInPolygon.printTime();
        //out.close();
    }

    public static void contain(Geometry g1, Geometry g2) throws IOException {
        long start = System.currentTimeMillis();
        //boolean bboxcontains = g1.getEnvelopeInternal().contains(g2.getEnvelopeInternal());
        long end = System.currentTimeMillis();
        jtsbboxcost += end - start;
        //if(!bboxcontains){
        //    return;
        //}else{
        //if(g1.getNumPoints() >= 20){
        //averageGrid += ((GridPolygon)g1).intersection;
        //averageCoveredGrid += ((GridPolygon)g1).containNum;
        long startTime = System.currentTimeMillis();
        boolean result = g1.contains(g2);
        long endTime = System.currentTimeMillis();
        timeSum += endTime - startTime;
        count++;
        if (result) {
            truecount++;
            //out.write("line "+ (c-1) + " and line " + c + " intersects result: " +result+"\n");
        }

        //}

        //out.write("line "+ i + " and line " + (i+1) + " intersects result: " +result+"\n");
        //out.write("line "+ (i+1) + " grids: " +lines.get(i+1).gridIDs+"\n");
        //out.write("line "+ i + " grids: " +lines.get(i).gridIDs+"\n");
        //}
    }

    public static Polygon parsePolygon(String json) {
        List<Coordinate> cos = new ArrayList<Coordinate>();
        JSONObject jsonObject = JSONObject.parseObject(json);
        JSONObject geometry = jsonObject.getJSONObject("geometry");
        JSONArray co = geometry.getJSONArray("coordinates");
        JSONArray coordinates = co.getJSONArray(0);
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

    public static GridPolygon parseGridPolygon(String json, int level) {
        List<Coordinate> cos = new ArrayList<Coordinate>();
        JSONObject jsonObject = JSONObject.parseObject(json);
        JSONObject geometry = jsonObject.getJSONObject("geometry");
        JSONArray co = geometry.getJSONArray("coordinates");
        JSONArray coordinates = co.getJSONArray(0);
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
        GridPolygon gridPolygon = new GridPolygon(shell, null, new GeometryFactory(), level, 5);

        return gridPolygon;
    }

    public static Point parsePoint(String json) {
        List<Coordinate> co = new ArrayList<>();
        JSONObject jsonObject = JSONObject.parseObject(json);
        JSONObject geometry = jsonObject.getJSONObject("geometry");
        JSONArray coordinates = geometry.getJSONArray("coordinates");
        double lng = coordinates.getDouble(0);
        double lat = coordinates.getDouble(1);
        co.add(new Coordinate(lng, lat));
        return new Point(new CoordinateArraySequence(co.toArray(new Coordinate[0])), new GeometryFactory());
    }

    public static Point parseGridPoint(String json, int level) {
        List<Coordinate> co = new ArrayList<>();
        JSONObject jsonObject = JSONObject.parseObject(json);
        JSONObject geometry = jsonObject.getJSONObject("geometry");
        JSONArray coordinates = geometry.getJSONArray("coordinates");
        double lng = coordinates.getDouble(0);
        double lat = coordinates.getDouble(1);
        co.add(new Coordinate(lng, lat));
        return new GridPoint(new CoordinateArraySequence(co.toArray(new Coordinate[0])), new GeometryFactory(), level);
    }
}
