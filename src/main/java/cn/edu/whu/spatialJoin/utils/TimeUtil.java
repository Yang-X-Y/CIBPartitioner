package cn.edu.whu.spatialJoin.utils;

public class TimeUtil {

    private static long start;

    private static long end;

    public static void start(){
        start = System.currentTimeMillis();
    }

    public static void end(){
        end = System.currentTimeMillis();
    }

    public static long getT(){
        return end - start;
    }

    public static long endAndGetTAndStart(){
        end();
        long t = end - start;
        start();
        return t;
    }

}
