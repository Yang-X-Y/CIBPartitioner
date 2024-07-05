package cn.edu.whu.spatialJoin.JTS;

import org.locationtech.jts.algorithm.LineIntersector;
import org.locationtech.jts.algorithm.RobustLineIntersector;
import org.locationtech.jts.algorithm.locate.SimplePointInAreaLocator;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.index.sweepline.SweepLineIndex;
import org.locationtech.jts.index.sweepline.SweepLineInterval;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;

public class GridPolygonIntersection {
    public static int filterTime = 0;
    public static int filterCount = 0;
    public static int filterTrueCount = 0;
    public static int refinementTime = 0;
    public static double intersectionTrueCount = 0;

    public static GeometryFactory gf = new GeometryFactory();

    public static Grid[] gridFilter(GridPolygon g1, GridPolygon g2) {

        /** filter step **/
        return intersection(g1.grids, g2.grids);
    }

    public static boolean gridRefine(GridPolygon g1, GridPolygon g2,Grid[] intersectGrid) {


        Envelope[] env = new Envelope[intersectGrid.length];

        int i = 0;
        for (Grid id : intersectGrid) {
            double[] boundary = GridUtils.getGridBoundary(id);
            env[i] = new Envelope(boundary[0], boundary[1], boundary[2], boundary[3]);
            i++;
        }
        GeometryEnvelopsFilter gef = new GeometryEnvelopsFilter(env);
        gef.initLineCrosser();

        SweepLineIndex sw = new SweepLineIndex();
        LineIntersector lis = new RobustLineIntersector();

        g1.apply(gef);
        Coordinate[] cos = g1.getCoordinates();
        for (Integer[] kv : gef.getMap()) {
            for (int k = kv[0]; k < kv[1]; k++) {
                long start = System.currentTimeMillis();
                boolean result = SimplePointInAreaLocator.containsPointInPolygon(cos[k], g2);
                long end = System.currentTimeMillis();
                refinementTime += end - start;
                if (result) {
                    intersectionTrueCount++;
                    return true;
                }
                LineSegmentWithLID ls = new LineSegmentWithLID(cos[k], cos[k + 1], 1);
                sw.add(new SweepLineInterval(cos[k].getX(), cos[k + 1].getX(), ls));
            }
        }

        g2.apply(gef);
        Coordinate[] cos2 = g2.getCoordinates();
        for (Integer[] kv : gef.getMap()) {
            for (int k = kv[0]; k < kv[1]; k++) {
                long start = System.currentTimeMillis();
                boolean result = SimplePointInAreaLocator.containsPointInPolygon(cos2[k], g1);
                long end = System.currentTimeMillis();
                refinementTime += end - start;
                if (result) {
                    intersectionTrueCount++;
                    return true;
                }
                LineSegmentWithLID ls = new LineSegmentWithLID(cos2[k], cos2[k + 1], 2);
                sw.add(new SweepLineInterval(cos2[k].getX(), cos2[k + 1].getX(), ls));
            }
        }

        long start = System.currentTimeMillis();
        SegmentIntersectAction SO = new SegmentIntersectAction(lis);
        sw.computeOverlaps(SO);
        long end = System.currentTimeMillis();
        refinementTime += end - start;
        return SO.isIntersection();
    }


    public static boolean intersection(GridPolygon g1, GridPolygon g2) {

        /** filter step **/
        long startTime = System.currentTimeMillis();
        Grid[] intersectGrid = intersection(g1.grids, g2.grids);
        long endTime = System.currentTimeMillis();
        filterTime += endTime - startTime;
        if (intersectGrid == null) {
            filterTrueCount++;
            return true;
        } else if (intersectGrid.length == 0) {
            filterCount++;
            return false;
        }

        Envelope[] env = new Envelope[intersectGrid.length];

        int i = 0;
        for (Grid id : intersectGrid) {
            double[] boundary = GridUtils.getGridBoundary(id);
            env[i] = new Envelope(boundary[0], boundary[1], boundary[2], boundary[3]);
            i++;
        }
        GeometryEnvelopsFilter gef = new GeometryEnvelopsFilter(env);
        gef.initLineCrosser();

        SweepLineIndex sw = new SweepLineIndex();
        LineIntersector lis = new RobustLineIntersector();

        g1.apply(gef);
        Coordinate[] cos = g1.getCoordinates();
        for (Integer[] kv : gef.getMap()) {
            for (int k = kv[0]; k < kv[1]; k++) {
                long start = System.currentTimeMillis();
                boolean result = SimplePointInAreaLocator.containsPointInPolygon(cos[k], g2);
                long end = System.currentTimeMillis();
                refinementTime += end - start;
                if (result) {
                    intersectionTrueCount++;
                    return true;
                }
                LineSegmentWithLID ls = new LineSegmentWithLID(cos[k], cos[k + 1], 1);
                sw.add(new SweepLineInterval(cos[k].getX(), cos[k + 1].getX(), ls));
            }
        }

        g2.apply(gef);
        Coordinate[] cos2 = g2.getCoordinates();
        for (Integer[] kv : gef.getMap()) {
            for (int k = kv[0]; k < kv[1]; k++) {
                long start = System.currentTimeMillis();
                boolean result = SimplePointInAreaLocator.containsPointInPolygon(cos2[k], g1);
                long end = System.currentTimeMillis();
                refinementTime += end - start;
                if (result) {
                    intersectionTrueCount++;
                    return true;
                }
                LineSegmentWithLID ls = new LineSegmentWithLID(cos2[k], cos2[k + 1], 2);
                sw.add(new SweepLineInterval(cos2[k].getX(), cos2[k + 1].getX(), ls));
            }
        }

        long start = System.currentTimeMillis();
        SegmentIntersectAction SO = new SegmentIntersectAction(lis);
        sw.computeOverlaps(SO);
        long end = System.currentTimeMillis();
        refinementTime += end - start;

        return SO.isIntersection();
    }


    /*public static Grid[] filter(GridPolygon g1, GridPolygon g2){
        if(intersectionCoveredJudge(g1.gridiDs,g2.gridiDs)){
            return null;
        }

        if(g1.maxLevel <= g2.maxLevel){
            if(intersectionCoveredJudge(g1.coveredGridIDs,g2.intersectGridIDs)){
                return null;
            }
            for(Grid id:intersection(g2.coveredGridIDs,g1.intersectGridIDs)){
                if(EnvelopeIntersects.intersects(id.getGridBoundary(),g1)){
                    return null;
                }
            }
        }else{
            if(intersectionCoveredJudge(g2.coveredGridIDs,g1.intersectGridIDs)){
                return null;
            }
            for(Grid id:intersection(g1.coveredGridIDs,g2.intersectGridIDs)){
                if(EnvelopeIntersects.intersects(id.getGridBoundary(),g2)){
                    return null;
                }
            }
        }
        return intersection(g1.intersectGridIDs,g2.intersectGridIDs);
    }*/

    public static Grid[] intersection(Grid[] nums1, Grid[] nums2) {
        int length1 = nums1.length, length2 = nums2.length;
        if (length1 == 0 || length2 == 0 || nums1[0].compareTo(nums2[length2 - 1]) > 0 || nums2[0].compareTo(nums1[length1 - 1]) > 0) {
            return new Grid[0];
        }
        int index = 0, index1 = 0, index2 = 0;
        Grid[] intersection = new Grid[length1 + length2];
        while (index1 < length1 && index2 < length2) {
            Grid num1 = nums1[index1];
            Grid num2 = nums2[index2];
            if (num1.compareTo(num2) == 0) {
                if (num1.level > num2.level) {
                    if (num2.contain) {
                        return null;
                    } else if (index == 0 || num1.compareTo(intersection[index - 1]) != 0) {
                        intersection[index++] = num1;
                    }
                    index1++;
                } else if (num1.level < num2.level) {
                    if (num1.contain) {
                        return null;
                    } else if (index == 0 || num2.compareTo(intersection[index - 1]) != 0) {
                        intersection[index++] = num2;
                    }
                    index2++;
                } else {
                    if (num1.contain || num2.contain) {
                        return null;
                    } else if (index == 0 || num1.compareTo(intersection[index - 1]) != 0) {
                        intersection[index++] = num1;
                    }
                    index1++;
                    index2++;
                }
            } else if (num1.compareTo(num2) < 0) {
                index1++;
            } else {
                index2++;
            }
        }
        return Arrays.copyOfRange(intersection, 0, index);
    }

    public static ArrayList<Tuple2<Grid,Grid>> intersection2(Grid[] nums1, Grid[] nums2) {
        // 过滤得到Tuple2<Grid,Grid>
        ArrayList<Tuple2<Grid,Grid>> intersection = new ArrayList<>();
        int length1 = nums1.length, length2 = nums2.length;
        if (length1 == 0 || length2 == 0 || nums1[0].compareTo(nums2[length2 - 1]) > 0 || nums2[0].compareTo(nums1[length1 - 1]) > 0) {
            return intersection;
        }
        int index1 = 0, index2 = 0;

        while (index1 < length1 && index2 < length2) {
            Grid num1 = nums1[index1];
            Grid num2 = nums2[index2];
            if (num1.compareTo(num2) == 0) {
                if (num1.level > num2.level) {
                    if (num2.contain) {
                        return null;
                    }
                    index1++;
                } else if (num1.level < num2.level) {
                    if (num1.contain) {
                        return null;
                    }
                    index2++;
                } else {
                    if (num1.contain || num2.contain) {
                        return null;
                    }
                    index1++;
                    index2++;
                }
                if (!num1.contain && !num2.contain){
                    intersection.add(new Tuple2<>(num1,num2));
                }
            } else if (num1.compareTo(num2) < 0) {
                index1++;
            } else {
                index2++;
            }
        }
        return intersection;
    }

    public static boolean intersectionCoveredJudge(Grid[] nums1, Grid[] nums2) {
        int length1 = nums1.length, length2 = nums2.length;
        if (length1 == 0 || length2 == 0 || nums1[0].compareTo(nums2[length2 - 1]) > 0 || nums2[0].compareTo(nums1[length1 - 1]) > 0) {
            return false;
        }
        int index = 0, index1 = 0, index2 = 0;
        while (index1 < length1 && index2 < length2) {
            Grid num1 = nums1[index1];
            Grid num2 = nums2[index2];
            if (num1.compareTo(num2) == 0) {
                return true;
            } else if (num1.compareTo(num2) < 0) {
                index1++;
            } else {
                index2++;
            }
        }
        return false;
    }

    public static void printTime() {
        System.out.println("filterTime : " + filterTime);
        System.out.println("grid filter false count : " + filterCount);
        System.out.println("grid filter true count : " + filterTrueCount);
        System.out.println("refineTime : " + refinementTime);

    }
}
