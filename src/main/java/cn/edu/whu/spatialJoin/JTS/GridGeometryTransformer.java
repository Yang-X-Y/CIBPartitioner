package cn.edu.whu.spatialJoin.JTS;

import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;

public class GridGeometryTransformer {

    public static GridPoint transformPoint(Point point, String objectId,int level) {
        return new GridPoint(point.getCoordinateSequence(), point.getFactory(),objectId, level);
    }

    public static GridLineString transformLineString(LineString lineString, String objectId, int SEG) {
        return new GridLineString(lineString.getCoordinateSequence(), lineString.getFactory(),objectId,SEG);
    }

    public static GridPolygon transformPolygon(Polygon polygon, String objectId, int SEG) {
        return new GridPolygon(polygon.getExteriorRing(), null, polygon.getFactory(), objectId, SEG);
    }

}
