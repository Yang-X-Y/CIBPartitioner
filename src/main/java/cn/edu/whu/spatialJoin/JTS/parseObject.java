package cn.edu.whu.spatialJoin.JTS;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.geom.impl.CoordinateArraySequence;

import java.util.ArrayList;
import java.util.List;

public class parseObject {

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
        if (isMultiGeom) {
            coordinates = co.getJSONArray(0).getJSONArray(0);
        } else {
            coordinates = co.getJSONArray(0);
        }
        for (int i = 0; i < coordinates.size(); i++) {
            JSONArray coordinate = coordinates.getJSONArray(i);
            double lng = coordinate.getDouble(0);
            double lat = coordinate.getDouble(1);
            cos.add(new Coordinate(lng, lat));
        }
        return new LineString(new CoordinateArraySequence(cos.toArray(new Coordinate[0])), new GeometryFactory());
    }

}
