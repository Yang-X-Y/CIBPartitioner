package cn.edu.whu.spatialJoin.JTS;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.geom.impl.CoordinateArraySequence;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class JTSPolygonIntersectionTest {

    public static int pointSum = 0;

    public static long timeSum = 0;
    public static int count = 0;
    public static double averageGrid = 0;
    public static double averageCoveredGrid = 0;
    public static int truecount = 0;
    public static int jtsbboxcost = 0;
    public static long initObject = 0;

    static BufferedWriter out = null;

    public static void main(String[] args) throws IOException, ParseException {

        WKTReader wktReader = new WKTReader();
        String queryObject = "POLYGON((16.36196696627082 58.67363430229014,16.36205161212007 58.6716208285834,16.365966542384726 58.671598822679215,16.36575492453258 58.67359029279905,16.36196696627082 58.67363430229014))";
//        String queryObject = "POLYGON((126.54428589451929 45.785771024837544,126.54393327735374 45.76843311734763,126.57531549218658 45.76769521167637,126.57531549218658 45.78503335090778,126.54428589451929 45.785771024837544))";
        Polygon polygon = (Polygon) wktReader.read(queryObject);
        GridPolygon gridPolygon = new GridPolygon(polygon.getExteriorRing(), null, new GeometryFactory(), 30, 2);
        for (Grid grid:gridPolygon.getGrids()) {

            String girdIDString = Long.toBinaryString(grid.gridID);
            int length = (grid.level*2);
            int rawLength = girdIDString.length();
            StringBuilder prefix= new StringBuilder();
            for (int i=0; i<(length-rawLength); i++) {
                prefix.append("0");
            }
            girdIDString = prefix.toString()+girdIDString;
            System.out.println("gridLevel:"+grid.level+"\tcontainFlag:"+grid.contain+"\tgridID:"+grid.gridID+"\tgridIDString:"+girdIDString);
            System.out.println("gridID:"+Long.parseLong(girdIDString,2)+"\tstringLength:"+girdIDString.length());
        }



//        long t = System.currentTimeMillis();
//
//        int option = Integer.parseInt(args[0]);
//        Geometry[] polygons = new Geometry[2];
//        int flag = 0;
//        int doIntersection = -1;
//        try {
//            BufferedReader in = new BufferedReader(new FileReader(args[1]));
//            //out = new BufferedWriter(new FileWriter("D:\\gridintersect2.txt"));
//            String str;
//            while ((str = in.readLine()) != null) {
//                if (option == 0) {
//                    polygons[flag] = parseGrid(str, Integer.parseInt(args[2]));
//                } else {
//                    polygons[flag] = parse(str);
//                }
//                doIntersection++;
//                if (doIntersection > 0) {
//                    intersect(polygons[0], polygons[1], doIntersection);
//                }
//                flag = (flag == 0) ? 1 : 0;
//            }
//
//        } catch (FileNotFoundException e) {
//            e.printStackTrace();
//        }
//
//        System.out.println(count + " polygon intersect use " + (int) timeSum + " ms");
//        System.out.println("point per polygon: " + pointSum / count);
//        System.out.println("intersection grid per polygon: " + averageGrid / (count * 2));
//        System.out.println("covered grid per polygon: " + averageCoveredGrid / (count * 2));
//        System.out.println("intersection true num: " + truecount);
//        System.out.println("jts calculate bounding box intersection time : " + jtsbboxcost);
//        GridPolygonIntersection.printTime();
//        //out.close();
//
//        System.out.println("总时间 ： " + (System.currentTimeMillis() - t) + ", 创建对象时间:" + initObject);
    }


    public static Polygon parse(String json) {
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

    public static GridPolygon parseGrid(String json, int level) {
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
        long t = System.currentTimeMillis();
        GridPolygon gridPolygon = new GridPolygon(shell, null, new GeometryFactory(), level, 5);
        initObject += System.currentTimeMillis() - t;
        return gridPolygon;
    }

    public static void intersect(Geometry g1, Geometry g2, int c) throws IOException {
        long start = System.currentTimeMillis();
        boolean bboxIntersection = g1.getEnvelopeInternal().intersects(g2.getEnvelopeInternal());
        long end = System.currentTimeMillis();
        jtsbboxcost += end - start;
        //if(!bboxIntersection){
        //    return;
        //}else{
        //if(g1.getNumPoints() == 8 && g2.getNumPoints() == 5){
        //averageCoveredGrid += ((UselessGridPolygon)g2).coveredGridIDs.length;
        //averageCoveredGrid += ((UselessGridPolygon)g1).coveredGridIDs.length;
        //averageGrid += ((UselessGridPolygon)g1).intersectGridIDs.length;
        //averageGrid += ((UselessGridPolygon)g2).intersectGridIDs.length;
        long startTime = System.currentTimeMillis();
        boolean result = g1.intersects(g2);
        long endTime = System.currentTimeMillis();
        timeSum += endTime - startTime;
        count++;
        if (result) {
            truecount++;
            //out.write("line "+ (c-1) + " and line " + c + " intersects result: " +result+"\n");
        }

        //}
        //System.out.println(count);
        //out.write("line "+ i + " and line " + (i+1) + " intersects result: " +result+"\n");
        //out.write("line "+ (i+1) + " grids: " +lines.get(i+1).gridIDs+"\n");
        //out.write("line "+ i + " grids: " +lines.get(i).gridIDs+"\n");
        //}
    }
}