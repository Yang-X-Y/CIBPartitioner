package cn.edu.whu.spatialJoin.JTS;

import org.locationtech.jts.algorithm.locate.SimplePointInAreaLocator;
import org.locationtech.jts.geom.GeometryFactory;

public class GridPointInPolygon {
    public static int filterTime = 0;
    public static int filterCount = 0;
    public static int refinementTime = 0;
    public static double intersectionTrueCount = 0;

    public static GeometryFactory gf = new GeometryFactory();

    public static boolean contain(GridPolygon g1, GridPoint g2) {

        /** filter step **/
        long startTime = System.currentTimeMillis();
        int position = search(g1.grids, g2.grid);
        long endTime = System.currentTimeMillis();
        filterTime += endTime - startTime;
        if (position == -1) {
            filterCount++;
            return false;
        } else if (g1.grids[position].contain) {
            intersectionTrueCount++;
            return true;
        }


        /** refinement step **/
        long start = System.currentTimeMillis();
        boolean result = SimplePointInAreaLocator.containsPointInPolygon(g2.getCoordinate(), g1);
        long end = System.currentTimeMillis();
        refinementTime += end - start;
        if (result) {
            return true;
        }
        return false;
    }

    public static int search(Grid[] nums, Grid target) {
        int pivot, left = 0, right = nums.length - 1;
        if (nums.length == 0 || target.compareTo(nums[0]) < 0 || target.compareTo(nums[nums.length - 1]) > 0) {
            return -1;
        }
        while (left <= right) {
            pivot = left + (right - left) / 2;
            if (nums[pivot].compareTo(target) == 0) return pivot;
            if (target.compareTo(nums[pivot]) < 0) right = pivot - 1;
            else left = pivot + 1;
        }
        return -1;
    }

    public static void printTime() {
        System.out.println("filterTime : " + filterTime);
        System.out.println("grid filter false count : " + filterCount);
        System.out.println("grid filter true count : " + intersectionTrueCount);
        System.out.println("refineTime : " + refinementTime);

    }
}
