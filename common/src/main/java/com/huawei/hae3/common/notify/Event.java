package com.huawei.hae3.common.notify;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @description: 事件接口
 * @author: wl
 * @create: 2022/5/15
 **/
public abstract class Event implements Serializable {

    private static final long serialVersionUID = -1804172169894757397L;

    // 事件序列号
    private static final AtomicLong SEQUENCE = new AtomicLong(0);

    private static long sequence = SEQUENCE.getAndIncrement();

    public static long getSequence() {
        return sequence;
    }
}
