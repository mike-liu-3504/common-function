package com.lhl.file.parse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @description: 单线程解析10G数据，统计各个年龄阶段出现的次数
 * @author: wl
 * @create: 2022/5/14
 **/
public class ParseData {
    private static final Logger LOGGER = LoggerFactory.getLogger(ParseData.class);

    private static Map<String, AtomicInteger> countMap = new ConcurrentHashMap();

    public static void main(String[] args) {
        ParseData.parseFile();
        statisticalMaximum();
    }

    public static void parseFile() {
        String file = "d://user.data";
        long startTime = System.currentTimeMillis();
        long startTimeLine = startTime;
        int count = 0;
        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file)))) {
            String line;
            while ((line = br.readLine()) != null) {
                splitLine(line);
                count++;
                if (count % 100 == 0) {
                    LOGGER.info("read {} line time loss:{}", count, (System.currentTimeMillis() - startTimeLine));
                    startTimeLine = System.currentTimeMillis();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        LOGGER.info("read {} line total time loss:{}", count, (System.currentTimeMillis() - startTime));
    }

    public static void splitLine(String line) {
        long starTime = System.currentTimeMillis();
        String[] arr = line.split(",");
        for (String str : arr) {
            countMap.computeIfAbsent(str, key -> new AtomicInteger(0)).getAndIncrement();
        }
        //LOGGER.info("parse line time loss:{}", (System.currentTimeMillis() - starTime));
    }

    public static void splitLine(String line, int splitNum) {
        long starTime = System.currentTimeMillis();
        int[] arr = new int[2];
        arr[1] = line.length() / splitNum;
        for (int i = 0; i < splitNum; i++) {
            String splitLine = splitLine(line, arr, splitNum);
            String[] splitArr = splitLine.split(",");
            for (String str : splitArr) {
                countMap.computeIfAbsent(str, key -> new AtomicInteger(0)).getAndIncrement();
            }
        }
        //LOGGER.info("split parse line time loss:{}", (System.currentTimeMillis() - starTime));
    }


    private static String splitLine(String line, int[] arr, int splitNum) {
        int startIndex = arr[0];
        int endIndex = arr[1];
        int len = line.length();
        if (endIndex >= len) {
            endIndex = len - 1;
        }
        char end = line.charAt(endIndex);
        while (end != ',' && ++endIndex < len) {
            end = line.charAt(endIndex);
        }
        arr[0] = endIndex;
        arr[1] = arr[0] + len / splitNum;
        // LOGGER.info("startIndex:{} endIndex:{} len:{}", startIndex, endIndex, len);
        return line.substring(startIndex, endIndex);
    }

    public static void statisticalMaximum() {
        LOGGER.info("{}", countMap);
        String targetKey = null;
        Integer targetValue = 0;
        for (Iterator<Map.Entry<String, AtomicInteger>> iterator = countMap.entrySet().iterator(); iterator.hasNext(); ) {
            Map.Entry<String, AtomicInteger> entry = iterator.next();
            String key = entry.getKey();
            Integer value = entry.getValue().get();
            if (value > targetValue) {
                targetKey = key;
                targetValue = value;
            }
        }
        LOGGER.info("max age is: {}={}", targetKey, targetValue);
    }
}
