package cn.edu.whu.spatialJoin.JTS;

import org.locationtech.jts.geom.CoordinateFilter;

public interface GridGeometry {

    Grid[] getGrids();
    String getObjectId();
    void apply(CoordinateFilter filter);
}
