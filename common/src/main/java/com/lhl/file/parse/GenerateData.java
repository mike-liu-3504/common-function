package com.lhl.file.parse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @description: 生成指定范围的数据
 * @author: wl
 * @create: 2022/5/14
 **/
public class GenerateData {
    private static final Logger LOGGER = LoggerFactory.getLogger(GenerateData.class);
    private static Random random = new Random();

    public static int generateRandomNum(int start, int end) {
        return start + random.nextInt(end - start + 1);
    }

    public static void generate2File(int start, int end, int gb) {
        String file = "d://user2.data";
        long totla = 342 * 1024 * 1024;  // 约等于1G
        long startTime = System.currentTimeMillis();
        StringBuilder sb = new StringBuilder();
        AtomicInteger count = new AtomicInteger(0);
        try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)))) {
            for (int j = 0; j < gb; j++) {
                for (long i = 1; i < totla; i++) {
                    sb.append(generateRandomNum(start, end) + ",");
                    // 没写100w行换行
                    if (i % 1000000 == 0) {
                        sb.append("\n");
                        bw.write(sb.toString());
                        sb = new StringBuilder();
                        count.getAndIncrement();
                    }
                }
            }
            if (sb.length() > 0) {
                bw.write(sb.toString());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        LOGGER.info("generate {}G data total time loss:{} write row:{}", gb, (System.currentTimeMillis() - startTime), count.get());

    }

    public static void main(String[] args) {
        generate2File(18, 90, 2);
    }

}
