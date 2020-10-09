package com.kad.hfunc_udf;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.*;

public class IndexGetWare extends UDF {
    public String evaluate(String line, Integer index, String warename) {

        String[] log = line.split("\\|");

        if (log.length < 1 || StringUtils.isBlank(log[0])) {
            return "-1";
        }

        Map<String, Integer> map = new HashMap<>();

        for (String entry : log) {
            if (entry.contains("::")){
                String[] kv = entry.split("::");
                if (kv.length > 1){
                    map.put(kv[0], Integer.valueOf(kv[1]));
                } else map.put(kv[0], 0);
            } else {
                return "-1";
            }
        }

        // 排序
        LinkedHashMap<String, Integer> res = sortMapByValue(map);

        if (index == -1 && !warename.equals("")) {
            return res.get(warename).toString();
        } else if (index != -1 && warename.equals("") && index < res.size()) {
            String key = "";
            Iterator<String> iterator = res.keySet().iterator();
            Integer acc = index + 1;
            while (iterator.hasNext()) {
                acc --;
                if (acc < 0){
                    break;
                }
                key = iterator.next();
            }
            return key;
        } else if (index >= res.size()){
            return "-1";
        } else {
            return "0";
        }
    }

    /*
     * 对Map<String,String>中的value进行排序（正序）
     */
    private LinkedHashMap<String, Integer> sortMapByValue(Map<String, Integer> unSortMap) {
        if (unSortMap == null || unSortMap.isEmpty()) {
            return null;
        }

        List<Map.Entry<String, Integer>> listEntry = new ArrayList<>(unSortMap.entrySet());
        listEntry.sort((o1, o2) -> {
            // String的compareTo方法，返回负数，说明o1在o2的字典顺序之前。
            return o2.getValue().compareTo(o1.getValue());// 此处。getValue改成getKey即可对Map按照key进行排序
        });

        LinkedHashMap<String,Integer> sortedMap = new LinkedHashMap<>();
        for(Map.Entry<String, Integer> entry : listEntry){
            sortedMap.put(entry.getKey(), entry.getValue());
        }
        return sortedMap;
    }

    public static void main(String[] args) {
        String warecode_mon = "商品1::32|b::1|商品二::2|d::4";
//        String warecode_mon = "70";

        IndexGetWare indexGetWare = new IndexGetWare();
        String res = indexGetWare.evaluate(warecode_mon, 2, "");
        System.out.println(res);
    }
}
