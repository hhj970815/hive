package com.kad.hfunc_udf;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.json.JSONException;

import java.util.*;

public class StringParseUDF extends UDF {
    public ArrayList<Map.Entry<String, Integer>> evaluate(String line) throws JSONException {

        String[] log = line.split("\\|");

        if (log.length < 1 || StringUtils.isBlank(log[0])) {
            return null;
        }

        TreeMap<String, Integer> map = new TreeMap<>();

        for (String entry : log) {
            if (entry.contains(":")){
                String[] kv = entry.split(":");
                if (kv.length > 1){
                    map.put(kv[0], Integer.valueOf(kv[1]));
                } else {
                    map.put(kv[0], 0);
                }
            }
        }

        // 这里将map.entrySet()转换成list
        ArrayList<Map.Entry<String, Integer>> list = new ArrayList<>(map.entrySet());

        list.sort((o1, o2) -> o2.getValue().compareTo(o1.getValue()));

        return list;
    }

    public static void main (String[] args) throws JSONException {
        String warecode_mon = "a:32\\|b:88\\|c:2\\|d:4";

        StringParseUDF stringParseUDF = new StringParseUDF();
        ArrayList<Map.Entry<String, Integer>> evaluate = stringParseUDF.evaluate(warecode_mon);
        System.out.println(evaluate.toString());
    }
}
