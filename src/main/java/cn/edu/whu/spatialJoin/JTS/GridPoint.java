package cn.edu.whu.spatialJoin.JTS;

import cn.edu.whu.spatialJoin.utils.NanoIdUtils;
import org.locationtech.geomesa.curve.Z2SFC;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;


public class GridPoint extends Point implements GridGeometry {
    Grid grid;
    String pointId;

    public GridPoint(CoordinateSequence coordinates, GeometryFactory factory,String objectId, int level) {
        super(coordinates, factory);
        this.pointId = objectId;
        getGridID(coordinates, level);
    }

    public GridPoint(CoordinateSequence coordinates, GeometryFactory factory, int level) {
        super(coordinates, factory);
        this.pointId = NanoIdUtils.randomNanoId();
        getGridID(coordinates, level);
    }

    public GridPoint(CoordinateSequence coordinates, GeometryFactory factory, int level,String pointId) {
        super(coordinates, factory);
        this.pointId = pointId;
        getGridID(coordinates, level);
    }

    public GridPoint(CoordinateSequence coordinates, GeometryFactory factory, Grid id) {
        super(coordinates, factory);
        this.grid = id;
    }

    private void getGridID(CoordinateSequence coordinates, int level) {
        if (!this.isEmpty()) {
            Z2SFC z2 = new Z2SFC(level);
            long z2index = z2.index(coordinates.getX(0), coordinates.getY(0), false);
            this.grid = new Grid((byte) level, z2index);
        }
    }



    public Grid[] getGrids() {
        return new Grid[]{grid};
    }

    @Override
    public String getObjectId() {
        return this.pointId;
    }

    @Override
    public String toString() {
        String s = super.toString();
        s = s.replace("POINT", "GRIDPOINT");
        return s;
    }

}
