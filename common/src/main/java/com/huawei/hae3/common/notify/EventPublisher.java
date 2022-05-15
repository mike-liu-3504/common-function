package com.huawei.hae3.common.notify;

import com.huawei.hae3.common.lifecycle.Closeable;
import com.huawei.hae3.common.notify.listener.Subscriber;

/**
 * @description: 事件发布器
 * @author: wl
 * @create: 2022/5/15
 **/
public interface EventPublisher extends Closeable {

    void init(Class<? extends Event> type, int bufferSize);

    long currentEventSize();

    void addSubscriber(Subscriber subscriber);

    void removeSubscriber(Subscriber subscriber);

    boolean publish(Event event);

    void notifySubscriber(Subscriber subscriber, Event event);
}
