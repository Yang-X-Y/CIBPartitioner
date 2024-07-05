package cn.edu.whu.spatialJoin.JTS;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.impl.CoordinateArraySequence;

import java.util.*;

public class GridLineIntersection {

    public static int filterTime = 0;
    public static int filterCount = 0;
    public static int refinementTime = 0;
    public static double intersectionPointCount = 0;
    public static double intersectionCount = 0;
    public static double intersectionTrueCount = 0;

    public static GeometryFactory gf = new GeometryFactory();

    public static boolean intersection(GridLineString g1, GridLineString g2) {

        /** filter step **/
        long startTime = System.currentTimeMillis();
        Long[] intersectGrid = intersection(g1.gridIDs, g2.gridIDs);
        long endTime = System.currentTimeMillis();
        filterTime += endTime - startTime;
        if (intersectGrid.length == 0) filterCount++;

        /** refinement step **/
        Envelope[] env = new Envelope[intersectGrid.length];
        List<LineString> ls1 = new ArrayList<LineString>();
        List<LineString> ls2 = new ArrayList<LineString>();

        int i = 0;
        for (Long id : intersectGrid) {
            double[] boundary = GridUtils.getGridBoundary(new Grid(g1.level, id));
            env[i] = new Envelope(boundary[0], boundary[1], boundary[2], boundary[3]);
            i++;
        }
        GeometryEnvelopsFilter gef = new GeometryEnvelopsFilter(env);
        gef.initLineCrosser();

        g1.apply(gef);
        for (Integer[] kv : gef.getMap()) {
            CoordinateArraySequence cas = new CoordinateArraySequence(Arrays.copyOfRange(g1.getCoordinates(), kv[0], kv[1] + 1));
            ls1.add(new LineString(cas, gf));
        }

        g2.apply(gef);
        for (Integer[] kv : gef.getMap()) {
            CoordinateArraySequence cas = new CoordinateArraySequence(Arrays.copyOfRange(g2.getCoordinates(), kv[0], kv[1] + 1));
            ls2.add(new LineString(cas, gf));
        }

        for (LineString l1 : ls1) {
            for (LineString l2 : ls2) {
                long start = System.currentTimeMillis();
                boolean result = l1.intersects(l2);
                long end = System.currentTimeMillis();
                refinementTime += end - start;

                intersectionCount++;
                intersectionPointCount += l1.getNumPoints();
                intersectionPointCount += l2.getNumPoints();
                if (result) {
                    intersectionTrueCount++;
                    return true;
                }
            }
        }
        return false;
    }

    //sort first and double pointer to find intersection
    //O(nlogn), but faster than hashset method via leetcode
    public static Long[] intersection(Long[] nums1, Long[] nums2) {
        //Arrays.sort(nums1);
        //Arrays.sort(nums2);
        int length1 = nums1.length, length2 = nums2.length;
        int index = 0, index1 = 0, index2 = 0;
        Long[] intersection = new Long[length1 + length2];
        while (index1 < length1 && index2 < length2) {
            long num1 = nums1[index1];
            long num2 = nums2[index2];
            if (num1 == num2) {
                //保证元素的唯一性
                if (index == 0 || num1 != intersection[index - 1]) {
                    intersection[index++] = num1;
                }
                index1++;
                index2++;
            } else if (num1 < num2) {
                index1++;
            } else {
                index2++;
            }
        }
        return Arrays.copyOfRange(intersection, 0, index);
    }

    //faster in theory but actually slow
    public static Long[] getIntersection(Long[] num1, Long[] num2) {
        Set<Long> set1 = new HashSet<>();
        Set<Long> intersectionSet = new HashSet<>();

        for (Long num : num1) {
            set1.add(num);
        }

        for (Long num : num2) {
            if (set1.contains(num)) {
                intersectionSet.add(num);
            }
        }

        Long[] intersection = new Long[intersectionSet.size()];
        int index = 0;
        for (Long num : intersectionSet) {
            intersection[index++] = num;
        }
        return intersection;
    }

    public static void printTime() {
        System.out.println("filterTime : " + filterTime);
        System.out.println("grid filter count : " + filterCount);
        System.out.println("refineTime : " + refinementTime);
        System.out.println("points of intersection line : " + intersectionPointCount / (intersectionCount * 2));
        System.out.println("refinement ratio : " + intersectionTrueCount / intersectionCount);
    }
}
