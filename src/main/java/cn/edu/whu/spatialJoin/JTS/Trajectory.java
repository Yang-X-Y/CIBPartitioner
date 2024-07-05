package cn.edu.whu.spatialJoin.JTS;

import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;

public class Trajectory extends LineString {

    public static final String GEOJSON_DATETIMES = "datetimes";

    public static final String DATETIME_PATTERN = "yyyy-MM-dd HH:mm:ss";

    public Trajectory(CoordinateSequence points, GeometryFactory factory) {
        super(points, factory);
        if (!points.hasZ() || Double.isNaN(points.getZ(0))) {
            throw new IllegalArgumentException("the points should has Z for DateTime");
        }
    }

    @Override
    public String toString() {
        String s = super.toString();
        s = s.replace("LINESTRING", "TRAJECTORY");
        return s;
    }

}
