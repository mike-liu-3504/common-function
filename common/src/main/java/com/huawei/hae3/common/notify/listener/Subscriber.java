package com.huawei.hae3.common.notify.listener;

import com.huawei.hae3.common.notify.Event;

import java.util.concurrent.Executor;

/**
 * @description:
 * @author: wl
 * @create: 2022/5/15
 **/
public abstract class Subscriber<T extends Event> {

    public abstract void onEvent(T event);

    public abstract Class<? extends Event> subscribeType();

    public Executor executor() {
        return null;
    }

    public boolean ignoreExpireEvent() {
        return false;
    }
}
