package cn.edu.whu.spatialJoin.JTS;

import cn.edu.whu.spatialJoin.utils.NanoIdUtils;
import org.locationtech.geomesa.curve.Z2SFC;
import org.locationtech.jts.algorithm.RectangleLineIntersector;
import org.locationtech.jts.geom.*;
import org.locationtech.sfcurve.zorder.Z2;
import scala.Tuple2;

import java.util.*;

public class GridLineString extends LineString implements GridGeometry {
    public Long[] gridIDs;
    public Grid[] grids;
    public byte level;
    public int maxGridLevel;
    public int finalLevel;
    String objectId;

    public GridLineString(CoordinateSequence points, GeometryFactory factory, byte gridLevel) {
        super(points, factory);
        this.objectId = NanoIdUtils.randomNanoId();
        this.level = gridLevel;
        this.gridIDs = getGridIDs(points);
        this.grids = createGrids();
    }

    public GridLineString(CoordinateSequence points, GeometryFactory factory, byte maxGridLevel, int recursiveTimes) {
        super(points, factory);
        this.objectId = NanoIdUtils.randomNanoId();
        this.level = maxGridLevel;
        this.gridIDs = getGridIDs(points,recursiveTimes);
    }

    public GridLineString(CoordinateSequence points, GeometryFactory factory, Long[] gridIDs, byte gridLevel) {
        super(points, factory);
        this.level = gridLevel;
        this.gridIDs = gridIDs;
        this.grids = createGrids();
    }

    public GridLineString(CoordinateSequence points, GeometryFactory factory, Grid[] grids) {
        super(points, factory);
        this.level = grids[0].getLevel();
        this.gridIDs = Arrays.stream(grids).map(Grid::getGridID).toArray(Long[]::new);

    }

    public GridLineString(CoordinateSequence points, GeometryFactory factory, int maxGridLevel, int recursiveTimes) {
        super(points, factory);
        this.objectId = "1";
        this.maxGridLevel = maxGridLevel;
        this.grids = getGrids(points,recursiveTimes);
    }

    public GridLineString(CoordinateSequence points, GeometryFactory factory,String objectId, int SEG) {
        super(points, factory);
        this.objectId = objectId;
        this.grids = getGridsBySEG(points,SEG);
        this.gridIDs = getGridIDs(this.grids);
        this.level = this.grids[0].level;
    }

    public GridLineString(CoordinateSequence points, GeometryFactory factory, int SEG) {
        super(points, factory);
        this.objectId = NanoIdUtils.randomNanoId();
        this.grids = getGridsBySEG(points,SEG);
        this.gridIDs = getGridIDs(this.grids);
    }

    public static void getEdgeNeighbors(long id, long[] neighbors) {
        Tuple2<Object, Object> XY;
        Z2 z2 = new Z2(id);
        XY = z2.decode();
        neighbors[0] = Z2.apply((Integer) XY._1(), (Integer) XY._2() + 1);
        neighbors[1] = Z2.apply((Integer) XY._1() + 1, (Integer) XY._2());
        neighbors[2] = Z2.apply((Integer) XY._1(), (Integer) XY._2() - 1);
        neighbors[3] = Z2.apply((Integer) XY._1() - 1, (Integer) XY._2());
    }

    public void getInitGrid(int level, Envelope e, LinkedList<Grid> candidateQueue,CoordinateSequence points) {
        double maxX = e.getMaxX();
        double maxY = e.getMaxY();
        double minX = e.getMinX();
        double minY = e.getMinY();

        Grid leftBottom = new Grid((byte) level, new Coordinate(minX, minY));

        if (mayIntersect(leftBottom,points)) {
            candidateQueue.add(leftBottom);
        }
        Grid rightTop = new Grid((byte) level, new Coordinate(maxX, maxY));
        if (rightTop.equals(leftBottom)) {
            if (candidateQueue.size() == 0) {
                candidateQueue.add(rightTop);
            }
            return;
        } else if (mayIntersect(rightTop,points)) {
            candidateQueue.add(rightTop);
        }

        Grid leftTop = new Grid((byte) level, new Coordinate(minX, maxY));
        if (leftTop.equals(leftBottom) || leftTop.equals(rightTop)) {
            return;
        } else if (mayIntersect(leftTop,points)) {
            candidateQueue.add(leftTop);
        }

        Grid rightBottom = new Grid((byte) level, new Coordinate(maxX, minY));
        if (mayIntersect(rightBottom,points)) {
            candidateQueue.add(rightBottom);
        }

        return;
    }

    public Long[] getGridIDs(CoordinateSequence points) {
        HashSet<Long> all = new HashSet<Long>();
        ArrayList<Long> frontier = new ArrayList<Long>();
        ArrayList<Long> output = new ArrayList<Long>();
        Z2SFC z2 = new Z2SFC(level);
        long startID = z2.index(points.getX(0), points.getY(0), true);
        //Grid start = new Grid(level, startID);
        all.add(startID);
        frontier.add(startID);

        while (!frontier.isEmpty()) {
            long id = frontier.get(frontier.size() - 1);
            frontier.remove(frontier.size() - 1);
            if (!this.mayIntersect(id, points)) {
                continue;
            }
            output.add(id);

            long[] neighbors = new long[4];
            getEdgeNeighbors(id, neighbors);
            for (int edge = 0; edge < 4; ++edge) {
                long nbr = neighbors[edge];
                if (!all.contains(nbr)) {
                    frontier.add(nbr);
                    all.add(nbr);
                }
            }
        }
        //sort before find intersection then O(n) can solve
        Long[] result = output.toArray(new Long[0]);
        Arrays.sort(result);
        return result;
    }

    public Long[] getGridIDs(Grid[] grids) {
        ArrayList<Long> output = new ArrayList<Long>();
        for (Grid grid:grids){
            output.add(grid.gridID);
        }
        Long[] result = output.toArray(new Long[0]);
        Arrays.sort(result);
        return result;
    }

    public Grid[] createGrids() {
        ArrayList<Grid> output = new ArrayList<>();
        for (long gridId:gridIDs){
            output.add(new Grid(level,gridId));
        }
        Grid[] result = output.toArray(new Grid[0]);
        Arrays.sort(result);
        return result;
    }

    public Long[] getGridIDs(CoordinateSequence points, int recursiveTimes) {
        HashSet<Long> all = new HashSet<Long>();
        ArrayList<Long> frontier = new ArrayList<Long>();
        ArrayList<Long> output = new ArrayList<Long>();
        Envelope e = points.expandEnvelope(new Envelope());
        int initLevel = (int) Math.floor(Math.log(360 / Math.max(e.getWidth(), e.getHeight() * 2)) / Math.log(2));
        int finalLevel = Math.min(initLevel + recursiveTimes, (int) level);
        Z2SFC z2 = new Z2SFC(finalLevel);
        long startID = z2.index(points.getX(0), points.getY(0), true);
        //Grid start = new Grid(level, startID);
        all.add(startID);
        frontier.add(startID);

        while (!frontier.isEmpty()) {
            long id = frontier.get(frontier.size() - 1);
            frontier.remove(frontier.size() - 1);
            if (!this.mayIntersect(id, points)) {
                continue;
            }
            output.add(id);

            long[] neighbors = new long[4];
            getEdgeNeighbors(id, neighbors);
            for (int edge = 0; edge < 4; ++edge) {
                long nbr = neighbors[edge];
                if (!all.contains(nbr)) {
                    frontier.add(nbr);
                    all.add(nbr);
                }
            }
        }
        //sort before find intersection then O(n) can solve
        Long[] result = output.toArray(new Long[0]);
        Arrays.sort(result);
        return result;
    }

    public boolean mayIntersect(Grid grid, CoordinateSequence points) {
        if (points.size() == 0) {
            return false;
        }
        double[] boundary = GridUtils.getGridBoundary(grid);
        Envelope e = new Envelope(boundary[0], boundary[1], boundary[2], boundary[3]);
        RectangleLineIntersector gridLineCrosser = new RectangleLineIntersector(e);

        for (int i = 0; i < points.size(); ++i) {
            if (e.covers(points.getCoordinate(i))) {
                return true;
            }
        }

        for (int i = 1; i < points.size(); ++i) {
            if (gridLineCrosser.intersects(points.getCoordinate(i - 1), points.getCoordinate(i))) {
                // There is a proper crossing, or two vertices were the same.
                return true;
            }
        }

        return false;
    }

    public boolean mayIntersect(long id, CoordinateSequence points) {
        if (points.size() == 0) {
            return false;
        }
        double[] boundary = GridUtils.getGridBoundary(new Grid(level, id));
        Envelope e = new Envelope(boundary[0], boundary[1], boundary[2], boundary[3]);
        RectangleLineIntersector gridLineCrosser = new RectangleLineIntersector(e);

        for (int i = 0; i < points.size(); ++i) {
            if (e.covers(points.getCoordinate(i))) {
                return true;
            }
        }

        for (int i = 1; i < points.size(); ++i) {
            if (gridLineCrosser.intersects(points.getCoordinate(i - 1), points.getCoordinate(i))) {
                // There is a proper crossing, or two vertices were the same.
                return true;
            }
        }

        return false;
    }

    public void apply(CoordinateFilter filter) {
        ((EnvelopeFilter) filter).initPointer();
        ((EnvelopeFilter) filter).preCo = this.points.getCoordinate(0);
        for (int i = 1; i < this.points.size(); ++i) {
            filter.filter(this.points.getCoordinate(i));
        }
        ((EnvelopeFilter) filter).done();
    }

    @Override
    public boolean intersects(Geometry g) {
        if (g instanceof GridLineString) {
            //System.out.println("das");
            return GridLineIntersection2.intersection(this, (GridLineString) g);
        } else {
            return super.intersects(g);
            //throw new UnsupportedOperationException("only support GridLineStrings intersection");
        }

    }

    public Grid[] getGrids() {
        return grids;
    }

    public Grid[] getGrids(CoordinateSequence points, int recursiveTimes) {
        HashSet<Long> all = new HashSet<Long>();
        ArrayList<Long> frontier = new ArrayList<Long>();
        ArrayList<Grid> output = new ArrayList<>();
        Envelope e = points.expandEnvelope(new Envelope());
        int initLevel = (int) Math.floor(Math.log(360 / Math.max(e.getWidth(), e.getHeight() * 2)) / Math.log(2));
        finalLevel = Math.min(initLevel + recursiveTimes, maxGridLevel);
        Z2SFC z2 = new Z2SFC(finalLevel);
        long startID = z2.index(points.getX(0), points.getY(0), true);
        //Grid start = new Grid(level, startID);
        all.add(startID);
        frontier.add(startID);

        while (!frontier.isEmpty()) {
            long id = frontier.get(frontier.size() - 1);
            frontier.remove(frontier.size() - 1);
            Grid grid = new Grid((byte) finalLevel, id);
            if (!this.mayIntersect(grid, points)) {
                continue;
            }
            output.add(grid);

            long[] neighbors = new long[4];
            getEdgeNeighbors(id, neighbors);
            for (int edge = 0; edge < 4; ++edge) {
                long nbr = neighbors[edge];
                if (!all.contains(nbr)) {
                    frontier.add(nbr);
                    all.add(nbr);
                }
            }
        }
        //sort before find intersection then O(n) can solve
        Grid[] result = output.toArray(new Grid[0]);
        Arrays.sort(result);
        return result;
    }

    public Grid[] getGridsBySEG(CoordinateSequence points, int SEG) {
        int boundaryGridsNum=points.size()/SEG;
        LinkedList<Grid> queue = new LinkedList<>();
        ArrayList<Grid> result = new ArrayList<>();
        Envelope e = points.expandEnvelope(new Envelope());
        int initLevel = (int) Math.floor(Math.log(360 / Math.max(e.getWidth(), e.getHeight() * 2)) / Math.log(2));
        getInitGrid(initLevel,e,queue,points);
        int currentBoundaryGrids=queue.size();
        //层次遍历
        while (currentBoundaryGrids < boundaryGridsNum) {
            int size = queue.size();
            for (int i = 0; i < size; i++) {
                Grid poll = queue.poll();
                for (Grid child : poll.getChildren()) {
                    if (mayIntersect(child,points)) {
                        queue.offer(child);
                    }
                }
            }
            currentBoundaryGrids = queue.size();
        }

        while (!queue.isEmpty()) {
            Grid grid = queue.poll();
//            grid.setIntervals(this);
            result.add(grid);
        }
        Grid[] result1 = result.toArray(new Grid[0]);
        Arrays.sort(result1);
        return result1;
    }


    @Override
    public String getObjectId() { return this.objectId; }

    @Override
    public String toString() {
        String s = super.toString();
        s = s.replace("LINESTRING", "GRIDLINESTRING");
        return s;
    }
}
