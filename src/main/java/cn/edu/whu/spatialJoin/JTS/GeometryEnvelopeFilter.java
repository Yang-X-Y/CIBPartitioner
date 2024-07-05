package cn.edu.whu.spatialJoin.JTS;

import org.locationtech.jts.algorithm.RectangleLineIntersector;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;

import java.util.List;

/*
point in envelop,return in linestring
 */
public class GeometryEnvelopeFilter extends EnvelopeFilter {

    boolean preContainFlag = false;

    int pointPointer = 1;
    int pointStart = 0;

    Envelope e;

    RectangleLineIntersector gridLineCrosser;

    GeometryEnvelopeFilter(Envelope e) {
        super();
        this.e = e;
    }

    public void initLineCrosser() {
        gridLineCrosser = new RectangleLineIntersector(e);
    }

    public void initPointer() {
        pointPointer = 1;
        pointStart = 0;
        lastInterval = 0;
        pointInterval.clear();
    }

    public List<Integer[]> getMap() {
        return pointInterval;
    }

    public void done() {
        if (preContainFlag) {
            if (pointStart == lastInterval && lastInterval != 0) {
                Integer[] interval = pointInterval.get(pointInterval.size() - 1);
                pointInterval.set(pointInterval.size() - 1, new Integer[]{interval[0], pointPointer - 1});
            } else {
                pointInterval.add(new Integer[]{pointStart, pointPointer - 1});
            }
            lastInterval = pointPointer - 1;
            pointStart = pointPointer;
        }
        preContainFlag = false;
    }

    public void filter(Coordinate coordinate) {
        if (gridLineCrosser.intersects(preCo, coordinate)) {
            if (!preContainFlag) {
                pointStart = pointPointer - 1;
            }
            pointPointer++;
            preContainFlag = true;
        } else {
            if (preContainFlag) {
                if (pointStart == lastInterval && lastInterval != 0) {
                    Integer[] interval = pointInterval.get(pointInterval.size() - 1);
                    pointInterval.set(pointInterval.size() - 1, new Integer[]{interval[0], pointPointer - 1});
                } else {
                    pointInterval.add(new Integer[]{pointStart, pointPointer - 1});
                }
                lastInterval = pointPointer - 1;
                pointStart = pointPointer;
            }
            pointPointer++;
            preContainFlag = false;
        }
        preCo = coordinate;
    }
}
