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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @description:
 * @author: wl
 * @create: 2022/5/14
 **/
public class MuiltThreadParseData {
    private static final Logger LOGGER = LoggerFactory.getLogger(MuiltThreadParseData.class);

    private static Map<String, AtomicInteger> countMap = new ConcurrentHashMap();


    private static ThreadPoolExecutor consumerThreadPool = new ThreadPoolExecutor(4,
            4,
            100,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<>());

    private static ThreadPoolExecutor splitThreadPool = new ThreadPoolExecutor(1,
            1,
            100,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(100));

    private static volatile boolean isFinish = false;

    private static volatile boolean consumerFlag = true;

    public static void main(String[] args) {
        new Thread(() -> produce()).start();
        monitor();
    }

    public static void produce() {
        parseFile();
    }

    public static void parseFile() {
        String file = "d://user.data";
        long startTime = System.currentTimeMillis();
        long startTimeLine = startTime;
        int count = 0;
        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file)))) {
            String line;
            while ((line = br.readLine()) != null) {
                String finalLine = line;
                splitThreadPool.submit(() -> parseLine(finalLine, 6));
                count++;
                if (count % 100 == 0) {
                    LOGGER.info("read {} line time loss:{}", count, (System.currentTimeMillis() - startTimeLine));
                    startTimeLine = System.currentTimeMillis();
                }
                //LOGGER.info("queue size:{}", consumerQueue.size());
            }
            isFinish = true;
        } catch (IOException e) {
            e.printStackTrace();
        }
        LOGGER.info("read {} line total time loss:{}", count, (System.currentTimeMillis() - startTime));
    }

    /**
     * 将一个大的字符串分割成splitNum段，提高解析速度
     *
     * @param line
     * @param splitNum
     */
    public static void parseLine(String line, int splitNum) {
        int[] arr = new int[2];
        arr[1] = line.length() / splitNum;
        long starTime = System.currentTimeMillis();
        for (int i = 0; i < splitNum; i++) {
            String splitLine = splitLine(line, arr, splitNum);
            consumerThreadPool.submit(() -> {
                long startTime = System.currentTimeMillis();
                String[] splitArr = splitLine.split(",");
                for (String str : splitArr) {
                    countMap.computeIfAbsent(str, key -> new AtomicInteger(0)).getAndIncrement();
                }
                //LOGGER.info("consumer finsh: {}", (System.currentTimeMillis() - startTime));
            });
        }
        //LOGGER.info("split parse line time loss:{}", (System.currentTimeMillis() - starTime));
    }

    public static void monitor() {
        long startTime = System.currentTimeMillis();
        AtomicInteger count = new AtomicInteger(0);
        try {
            Thread.sleep(10 * 1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        while (consumerFlag) {
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            int consumerThreadPoolQueue = consumerThreadPool.getQueue().size();
            LOGGER.info("current consumerQueueSize:{} splitQueueSize:{}", consumerThreadPoolQueue, splitThreadPool.getQueue().size());
            if (isFinish && consumerThreadPoolQueue == 0 && count.getAndIncrement() == 3) {
                consumerFlag = false;
                LOGGER.info("consumer finish time loss:{}", (System.currentTimeMillis() - startTime));
                statisticalMaximum();
                System.exit(-1);
            }
        }
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
