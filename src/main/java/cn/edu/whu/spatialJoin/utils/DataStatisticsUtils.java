package cn.edu.whu.spatialJoin.utils;

import java.util.*;

/**
 * 数据统计工具类
 * @author 胡湛霏
 * @since 2016-09-27
 */
public class DataStatisticsUtils {

    /**
     * List转为array
     **/
    public static double[] list2array(List<Double> nums) {
        double[] arr = new double[nums.size()];
        for (int i = 0 ; i < arr.length; i++){
            arr[i] = nums.get(i);
        }
        return arr;
    }
    
    /**
     * 求和
     *
     * @param arr
     * @return
     */
    public static double getSum(double[] arr) {
        double sum = 0;
        for (double num : arr) {
            sum += num;
        }
        return sum;
    }

    /**
     * 求均值
     *
     * @param arr
     * @return
     */
    public static double getMean(double[] arr) {
        return getSum(arr) / arr.length;
    }

    /**
     * 求众数
     *
     * @param arr
     * @return
     */
    public static double getMode(double[] arr) {
        Map<Double, Integer> map = new HashMap<Double, Integer>();
        for (int i = 0; i < arr.length; i++) {
            if (map.containsKey(arr[i])) {
                map.put(arr[i], map.get(arr[i]) + 1);
            } else {
                map.put(arr[i], 1);
            }
        }
        int maxCount = 0;
        double mode = -1;
        Iterator<Double> iter = map.keySet().iterator();
        while (iter.hasNext()) {
            double num = iter.next();
            int count = map.get(num);
            if (count > maxCount) {
                maxCount = count;
                mode = num;
            }
        }
        return mode;
    }

    /**
     * 求中位数
     *
     * @param arr
     * @return
     */
    public static double getMedian(double[] arr) {
        double[] tempArr = Arrays.copyOf(arr, arr.length);
        Arrays.sort(tempArr);
        if (tempArr.length % 2 == 0) {
            return (tempArr[tempArr.length >> 1] + tempArr[(tempArr.length >> 1) - 1]) / 2;
        } else {
            return tempArr[(tempArr.length >> 1)];
        }
    }


    /**
     * 求中列数
     *
     * @param arr
     * @return
     */
    public static double getMidrange(double[] arr) {
        double max = arr[0], min = arr[0];
        for (int i = 0; i < arr.length; i++) {
            if (arr[i] > max) {
                max = arr[i];
            }
            if (arr[i] < min) {
                min = arr[i];
            }
        }
        return (min + max) / 2;
    }

    /**
     * 求四分位数
     *
     * @param arr
     * @return 存放三个四分位数的数组
     */
    public static double[] getQuartiles(double[] arr) {
        double[] tempArr = Arrays.copyOf(arr, arr.length);
        Arrays.sort(tempArr);
        double[] quartiles = new double[3];
        // 第二四分位数（中位数）
        quartiles[1] = getMedian(tempArr);
        // 求另外两个四分位数
        if (tempArr.length % 2 == 0) {
            quartiles[0] = getMedian(Arrays.copyOfRange(tempArr, 0, tempArr.length / 2));
            quartiles[2] = getMedian(Arrays.copyOfRange(tempArr, tempArr.length / 2, tempArr.length));
        } else {
            quartiles[0] = getMedian(Arrays.copyOfRange(tempArr, 0, tempArr.length / 2));
            quartiles[2] = getMedian(Arrays.copyOfRange(tempArr, tempArr.length / 2 + 1, tempArr.length));
        }
        return quartiles;
    }

    /**
     * 求极差
     *
     * @param arr
     * @return
     */
    public static double getRange(double[] arr) {
        double max = arr[0], min = arr[0];
        for (int i = 0; i < arr.length; i++) {
            if (arr[i] > max) {
                max = arr[i];
            }
            if (arr[i] < min) {
                min = arr[i];
            }
        }
        return max - min;
    }

    /**
     * 求四分位数极差
     *
     * @param arr
     * @return
     */
    public static double getQuartilesRange(double[] arr) {
        return getRange(getQuartiles(arr));
    }

    /**
     * 求截断均值
     *
     * @param arr 求值数组
     * @param p   截断量p，例如p的值为20，则截断20%（高10%，低10%）
     * @return
     */
    public static double getTrimmedMean(double[] arr, int p) {
        int tmp = arr.length * p / 100;
        double[] tempArr = Arrays.copyOfRange(arr, tmp, arr.length + 1 - tmp);
        return getMean(tempArr);
    }

    public static double[] getNormalized(List<Double> list)  {
        double[] arr = list2array(list);
        double max = arr[0], min = arr[0];
        for (int i = 0; i < arr.length; i++) {
            if (arr[i] > max) {
                max = arr[i];
            }
            if (arr[i] < min) {
                min = arr[i];
            }
        }
        double range = max - min;
        for (int i = 0; i < arr.length; i++) {
            arr[i] = (arr[i]-min)/range;
        }
        return arr;
    }

    /**
     * 求方差
     *
     * @param arr
     * @return
     */
    public static double getVariance(double[] arr) {
        double variance = 0;
        double sum = 0, sum2 = 0;
        for (int i = 0; i < arr.length; i++) {
            sum += arr[i];
            sum2 += arr[i] * arr[i];
        }
        variance = sum2 / arr.length - (sum / arr.length) * (sum / arr.length);
        return variance;
    }

    /**
     * 求绝对平均偏差(AAD)
     *
     * @param arr
     * @return
     */
    public static double getAbsoluteAverageDeviation(double[] arr) {
        double sum = 0;
        double mean = getMean(arr);
        for (int i = 0; i < arr.length; i++) {
            sum += Math.abs(arr[i] - mean);
        }
        return sum / arr.length;
    }

    /**
     * 求中位数绝对偏差(MAD)
     *
     * @param arr
     * @return
     */
    public static double getMedianAbsoluteDeviation(double[] arr) {
        double[] tempArr = new double[arr.length];
        double median = getMedian(arr);
        for (int i = 0; i < arr.length; i++) {
            tempArr[i] = Math.abs(arr[i] - median);
        }
        return getMedian(tempArr);
    }

    /**
     * 求标准差
     * @param arr
     * @return
     */

    public static double getStandardDeviation(double[] arr) {
        double sum = 0;
        double mean = getMean(arr);
        for (double v : arr) {
            sum += (v - mean) * (v - mean);
        }
        return (Math.sqrt(sum/(arr.length)));
    }

    public static double getStandardDeviation(double[] arr,double mean,int size) {
        double sum = 0;
        for (double v : arr) {
            sum += (v - mean) * (v - mean);
        }
        return (Math.sqrt(sum/size));
    }

    /**
     * 求变异系数 CV: coefficient of variation
     * @param arr
     * @return
     */

    public static double getCoefficientVariation(double[] arr) {
        double sum = 0;
        double mean = getMean(arr);
        for (double v : arr) {
            sum += (v - mean) * (v - mean);
        }
        double sd = Math.sqrt(sum/(arr.length));
        return sd/mean;
    }

    public static double getCoefficientVariation(List<Double> nums) {
        double[] arr = list2array(nums);
        return getCoefficientVariation(arr);
    }

    /**
     * 求相关系数 PCCs: PearsonCorrelationScore
     */

    public static double getPearsonCorrelationScore(List<Double> x, List<Double> y) {
        if (x.size() != y.size())
            throw new RuntimeException("数据不正确！");
        double[] xData = list2array(x);
        double[] yData = list2array(y);
        return getPearsonCorrelationScore(xData,yData);
    }

    public static List<Double> combine(List<Double> x, List<Double> y, double alpha) {
        List<Double> list = new ArrayList<>();
        for (int i = 0 ; i < x.size(); i++) {
            list.add((1-alpha)*x.get(i)+alpha*y.get(i));
        }
        return list;
    }

    public static List<Double> combine(double[] x, double[] y, double alpha) {
        List<Double> list = new ArrayList<>();
        for (int i = 0 ; i < x.length; i++) {
            list.add((1-alpha)*x[i]+alpha*y[i]);
        }
        return list;
    }

    public static double getPearsonCorrelationScore(double[] xData, double[] yData) {
        if (xData.length != yData.length)
            throw new RuntimeException("数据不正确！");
        double xMeans;
        double yMeans;
        double numerator = 0;// 求解皮尔逊的分子
        double denominator = 0;// 求解皮尔逊系数的分母

        double result = 0;
        // 拿到两个数据的平均值
        xMeans = getMean(xData);
        yMeans = getMean(yData);
        // 计算皮尔逊系数的分子
        numerator = generateNumerator(xData, xMeans, yData, yMeans);
        // 计算皮尔逊系数的分母
        denominator = generateDenominator(xData, xMeans, yData, yMeans);
        // 计算皮尔逊系数
        result = numerator / denominator;
        return result;
    }

    /**
     * 计算分子
     *
     * @param xData
     * @param xMeans
     * @param yData
     * @param yMeans
     * @return
     */
    private static double generateNumerator(double[] xData, double xMeans, double[] yData, double yMeans) {
        double numerator = 0.0;
        for (int i = 0; i < xData.length; i++) {
            numerator += (xData[i] - xMeans) * (yData[i] - yMeans);
        }
        return numerator;
    }

    /**
     * 生成分母
     *
     * @param yMeans
     * @param yData
     * @param xMeans
     * @param xData
     * @return 分母
     */
    private static double generateDenominator(double[] xData, double xMeans, double[] yData, double yMeans) {
        double xSum = 0.0;
        for (int i = 0; i < xData.length; i++) {
            xSum += (xData[i] - xMeans) * (xData[i] - xMeans);
        }
        double ySum = 0.0;
        for (int i = 0; i < yData.length; i++) {
            ySum += (yData[i] - yMeans) * (yData[i] - yMeans);
        }
        return Math.sqrt(xSum) * Math.sqrt(ySum);
    }

//    public static double getStandardDevition(double[] arr) {
//        double sum = 0;
//        double mean = getMean(arr);
//        for (int i = 0; i < arr.length; i++) {
//            sum += Math.sqrt((arr[i] - mean) * (arr[i] - mean));
//        }
//        return (sum / (arr.length - 1));
//    }
}

