package cn.edu.whu.spatialJoin.JTS;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateFilter;

import java.util.ArrayList;
import java.util.List;

public abstract class EnvelopeFilter implements CoordinateFilter {
    List<Integer[]> pointInterval = new ArrayList<>();
    Coordinate preCo;
    int lastInterval = 0;

    EnvelopeFilter() {
    }

    public void done() {
    }

    public void initPointer() {
    }

    public void initLineCrosser() {
    }

    public List<Integer[]> getMap() {
        return null;
    }
}
