package cn.edu.whu.spatialJoin.utils;

import com.alibaba.fastjson.JSONObject;
import java.io.*;

public class JsonUtil {

    /**
     * Read text file from local disk. Only for small file.
     *
     * @param path file path on local disk.
     * @return string of file content.
     */
    public static JSONObject readLocalJSONFile(String path) {
        File file = new File(path);
        StringBuilder sb = new StringBuilder();
        try {
            Reader reader = new InputStreamReader(new FileInputStream(file));
            BufferedReader br = new BufferedReader(reader);
            String line;
            while ((line = br.readLine()) != null) {
                sb.append(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return JSONObject.parseObject(sb.toString());
    }
}
