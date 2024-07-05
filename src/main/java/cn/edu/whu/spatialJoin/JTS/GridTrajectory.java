package cn.edu.whu.spatialJoin.JTS;

import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.GeometryFactory;

public class GridTrajectory extends GridLineString {

    public GridTrajectory(CoordinateSequence points, GeometryFactory factory, byte gridLevel) {
        super(points, factory, gridLevel);
        if (!points.hasZ() || Double.isNaN(points.getZ(0))) {
            throw new IllegalArgumentException("the points should has Z for DateTime");
        }
    }

    public GridTrajectory(CoordinateSequence points, GeometryFactory factory, Long[] gridIDs, byte gridLevel) {
        super(points, factory, gridIDs, gridLevel);
        if (!points.hasZ() || Double.isNaN(points.getZ(0))) {
            throw new IllegalArgumentException("the points should has Z for DateTime");
        }
    }

    public GridTrajectory(CoordinateSequence points, GeometryFactory factory, Grid[] grids) {
        super(points, factory, grids);
        if (!points.hasZ() || Double.isNaN(points.getZ(0))) {
            throw new IllegalArgumentException("the points should has Z for DateTime");
        }
    }

    @Override
    public String toString() {
        String s = super.toString();
        s = s.replace("LINESTRING", "GRIDTRAJECTORY");
        return s;
    }

}
