package cn.edu.whu.spatialJoin.JTS;

import org.locationtech.geomesa.curve.Z2SFC;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.sfcurve.zorder.Z2;
import scala.Tuple2;

import java.util.Arrays;

public class GridUtils {
    public static void getEdgeNeighbors(Grid currentId, Grid[] neighbors) {
        Tuple2<Object, Object> XY;
        Z2 z2 = new Z2(currentId.gridID);
        XY = z2.decode();
        neighbors[0] = new Grid(currentId.getLevel(), Z2.apply((Integer) XY._1(), (Integer) XY._2() + 1));
        neighbors[1] = new Grid(currentId.getLevel(), Z2.apply((Integer) XY._1() + 1, (Integer) XY._2()));
        neighbors[2] = new Grid(currentId.getLevel(), Z2.apply((Integer) XY._1(), (Integer) XY._2() - 1));
        neighbors[3] = new Grid(currentId.getLevel(), Z2.apply((Integer) XY._1() - 1, (Integer) XY._2()));
    }

    public static double[] getGridBoundary(Grid currentId) {
        Tuple2<Object, Object> centLonLat;
        Z2SFC z2 = new Z2SFC(currentId.level);
        centLonLat = z2.invert(currentId.gridID);
        double minLon = (Double) centLonLat._1() - (180.0 / (1 << currentId.level));
        double minLat = (Double) centLonLat._2() - (90.0 / (1 << currentId.level));
        double maxLon = (Double) centLonLat._1() + (180.0 / (1 << currentId.level));
        double maxLat = (Double) centLonLat._2() + (90.0 / (1 << currentId.level));
        return new double[]{minLon, maxLon, minLat, maxLat};
    }

    public static boolean coveredBy(Grid[] nums, Grid target) {
        if (nums==null || nums.length == 0) {
            return false;
        }

        if (target.compareTo(nums[0]) == 0 && target.compareTo(nums[nums.length - 1]) == 0) {
            return true;
        }
        return false;
    }


    /**
     * note: level of nums must less or equal to level of target
     * otherwise target will intersection with nums see intersection(Grid[] nums, Grid target)
     *
     * @param nums
     * @param target
     * @return
     */
    public static int contain(Grid[] nums, Grid target) {
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

    public static int intersection(Grid[] nums, Grid target) {
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

    public static Grid[] intersection(Grid[] nums1, Grid[] nums2) {

        //System.out.println(Arrays.toString(nums1) + "   " + Arrays.toString(nums2));

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
                if (num1.getLevel() > num2.getLevel()) {
                    if (num2.isContainGrid()) {
                        return null;
                    } else if (index == 0 || num1.compareTo(intersection[index - 1]) != 0) {
                        intersection[index++] = num1;
                    }
                    index1++;
                } else if (num1.getLevel() < num2.getLevel()) {
                    if (num1.isContainGrid()) {
                        return null;
                    } else if (index == 0 || num2.compareTo(intersection[index - 1]) != 0) {
                        intersection[index++] = num2;
                    }
                    index2++;
                } else {
                    if (num1.isContainGrid() || num2.isContainGrid()) {
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
}
