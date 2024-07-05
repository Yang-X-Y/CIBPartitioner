package cn.edu.whu.spatialJoin.JTS;


import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.impl.CoordinateArraySequence;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class JTSLineIntersectionTest {
    public static int pointSum = 0;

    public static long timeSum = 0;
    public static int count = 0;
    public static double averageGrid = 0;
    public static int truecount = 0;
    public static int jtsbboxcost = 0;
    public static long initObject = 0;

    static BufferedWriter out = null;

    public static void main(String[] args) throws IOException, ClassNotFoundException {
        long t = System.currentTimeMillis();

        int option = Integer.parseInt(args[0]);
        Geometry[] lines = new Geometry[2];
        int flag = 0;
        int doIntersection = -1;
        try {
            BufferedReader in = new BufferedReader(new FileReader(args[1]));
            //out = new BufferedWriter(new FileWriter("D:\\gridintersect2.txt"));
            String str;
            while ((str = in.readLine()) != null) {
                if (option == 0) {
                    lines[flag] = parseGrid(str, Integer.parseInt(args[2]));
                } else {
                    lines[flag] = parse(str);
                }
                doIntersection++;
                if (doIntersection > 0) {
                    intersect(lines[0], lines[1], doIntersection);
                }
                flag = (flag == 0) ? 1 : 0;
            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        System.out.println(count + " lines intersect use " + (int) timeSum + " ms");
        System.out.println("point per lineString: " + pointSum / count);
        System.out.println("grid per lineString: " + averageGrid / (count * 2));
        System.out.println("intersection true num: " + truecount);
        System.out.println("jts calculate bounding box intersection time : " + jtsbboxcost);
        GridLineIntersection2.printTime();
        //out.close();

        System.out.println("总时间 ： " + (System.currentTimeMillis() - t) + ", 创建对象时间:" + initObject);
    }

    public static void intersect(Geometry g1, Geometry g2, int c) throws IOException {
        long start = System.currentTimeMillis();
        boolean bboxIntersection = g1.getEnvelopeInternal().intersects(g2.getEnvelopeInternal());
        long end = System.currentTimeMillis();
        jtsbboxcost += end - start;
        //if(!bboxIntersection){
        //    return;
        //}else{
        if (g1.getNumPoints() >= 20) {
            //averageGrid += ((GridLineString)g2).gridIDs.length;
            //averageGrid += ((GridLineString)g1).gridIDs.length;
            long startTime = System.currentTimeMillis();
            boolean result = g1.intersects(g2);
            long endTime = System.currentTimeMillis();
            timeSum += endTime - startTime;
            count++;
            if (result) {
                truecount++;
                //out.write("line "+ (c-1) + " and line " + c + " intersects result: " +result+"\n");
            }

        }

        //out.write("line "+ i + " and line " + (i+1) + " intersects result: " +result+"\n");
        //out.write("line "+ (i+1) + " grids: " +lines.get(i+1).gridIDs+"\n");
        //out.write("line "+ i + " grids: " +lines.get(i).gridIDs+"\n");
        //}
    }

    public static LineString parse(String json) {
        List<Coordinate> cos = new ArrayList<Coordinate>();
        JSONObject jsonObject = JSONObject.parseObject(json);
        JSONObject geometry = jsonObject.getJSONObject("geometry");
        JSONArray coordinates = geometry.getJSONArray("coordinates");
        //JSONArray coordinates = co.getJSONArray(0);
        pointSum += coordinates.size();
        for (int i = 0; i < coordinates.size(); i++) {
            JSONArray coordinate = coordinates.getJSONArray(i);
            double lng = coordinate.getDouble(0);
            double lat = coordinate.getDouble(1);
            cos.add(new Coordinate(lng, lat));
        }
        return new LineString(new CoordinateArraySequence(cos.toArray(new Coordinate[0])), new GeometryFactory());
    }

    public static GridLineString parseGrid(String json, int level) {
        List<Coordinate> cos = new ArrayList<Coordinate>();
        JSONObject jsonObject = JSONObject.parseObject(json);
        JSONObject geometry = jsonObject.getJSONObject("geometry");
        JSONArray coordinates = geometry.getJSONArray("coordinates");
        //JSONArray coordinates = co.getJSONArray(0);
        pointSum += coordinates.size();
        for (int i = 0; i < coordinates.size(); i++) {
            JSONArray coordinate = coordinates.getJSONArray(i);
            double lng = coordinate.getDouble(0);
            double lat = coordinate.getDouble(1);
            cos.add(new Coordinate(lng, lat));
        }
        long t = System.currentTimeMillis();
        GridLineString gridLineString = new GridLineString(new CoordinateArraySequence(cos.toArray(new Coordinate[0])), new GeometryFactory(), (byte) level);
        initObject += System.currentTimeMillis() - t;
        return gridLineString;
    }
}
