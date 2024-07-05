package cn.edu.whu.spatialJoin.showcase;

import cn.edu.whu.spatialJoin.utils.DataStatisticsUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class temp {
    public static void main(String[] args) {

        String path = "D:\\0我的工作\\0spatialJoin\\试验记录\\alpha调参\\";
        String files = "parks-lakes-CIBPTree-RTree-2000-1.0-1.0-03232258.txt;parks-pois-CIBPTree-RTree-2000-1.0-1.0-03240156.txt;parks-roads-CIBPTree-RTree-2000-1.0-1.0-03240001.txt";
        String[] fileList = files.split(";");
        for (String file:fileList) {
            System.out.println(file);
            List<Double> itemsNum = new ArrayList<>();
            List<Double> filterCost = new ArrayList<>();
            List<Double> refineCost = new ArrayList<>();
            List<Double> joinTime = new ArrayList<>();
            try (BufferedReader bufferedReader = new BufferedReader(new FileReader(path+file))) {
                String line;
                while ((line=bufferedReader.readLine())!=null) {
                    String[] strList = line.split("\t");
                    if (strList.length==4) {
                        if (!strList[0].equals("itemsNum")) {
                            itemsNum.add(Double.parseDouble(strList[0]));
                            filterCost.add(Double.parseDouble(strList[1]));
                            refineCost.add(Double.parseDouble(strList[2]));
                            joinTime.add(Double.parseDouble(strList[3]));
                        }
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            double itemsPCCs = DataStatisticsUtils.getPearsonCorrelationScore(itemsNum,joinTime);
            System.out.println("itemsPCCs:"+itemsPCCs);
            System.out.println("alpha\tCIPCCs");
            for (double alpha=0.0;alpha<=1;alpha+=0.001) {
                List<Double> CI = DataStatisticsUtils.combine(filterCost, refineCost, alpha);
                double CIPCCs = DataStatisticsUtils.getPearsonCorrelationScore(CI,joinTime);
                System.out.println(alpha+"\t"+CIPCCs);
            }
        }

    }
}
