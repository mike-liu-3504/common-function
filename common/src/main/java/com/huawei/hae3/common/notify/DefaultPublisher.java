package com.huawei.hae3.common.notify;

import com.huawei.hae3.common.notify.listener.Subscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;

/**
 * @description:
 * @author: wl
 * @create: 2022/5/15
 **/
public class DefaultPublisher extends Thread implements EventPublisher {
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultPublisher.class);

    private volatile boolean initialized = false;

    private volatile boolean shutdown = false;

    protected final Set<Subscriber> subscribers = new HashSet<>();

    private BlockingQueue<Event> queue;

    private Class<? extends Event> eventType;

    private int queueMaxSize = -1;

    @Override
    public void init(Class<? extends Event> type, int bufferSize) {
        setDaemon(true);
        setName("hae2.publisher-" + type.getName());
        eventType = type;
        queueMaxSize = bufferSize;
        queue = new ArrayBlockingQueue<>(bufferSize);
        start();
    }

    @Override
    public synchronized void start() {
        if (!initialized) {
            super.start();
            if (queueMaxSize == -1) {
                queueMaxSize = Integer.getInteger("hae3.core.notify.ring-buffer-size", 16384);
            }
            initialized = true;
        }
    }

    @Override
    public long currentEventSize() {
        return queue.size();
    }

    @Override
    public void addSubscriber(Subscriber subscriber) {
        subscribers.add(subscriber);
    }

    @Override
    public void removeSubscriber(Subscriber subscriber) {
        subscribers.remove(subscriber);
    }

    @Override
    public boolean publish(Event event) {
        checkIsStart();
        //如果放入队列失败，直接处理该事件
        boolean success = queue.offer(event);
        if (!success) {
            receiveEvent(event);
        }
        return true;
    }

    private boolean hasSubscriber() {
        return subscribers.size() == 0;
    }

    private void receiveEvent(Event event) {
        if (hasSubscriber()) {
            LOGGER.warn("no subscriber {}", event);
            return;
        }
        for (Subscriber subscriber : subscribers) {
            notifySubscriber(subscriber, event);
        }
    }

    private void checkIsStart() {
        if (!initialized) {
            throw new IllegalStateException("Publisher does not start");
        }
    }

    @Override
    public void run() {
        openEventHandler();
    }

    void openEventHandler() {
        try {
            // 等订阅者注册进来
            int waitTimes = 60;
            for (; ; ) {
                if (shutdown || hasSubscriber() || waitTimes <= 0) {
                    break;
                }
                Thread.sleep(1000L);
                waitTimes--;
            }

            for (; ; ) {
                if (shutdown) {
                    break;
                }
                Event event = queue.take();
                receiveEvent(event);
            }

        } catch (Throwable ex) {
            LOGGER.error("Event listener exception:", ex);
        }
    }

    @Override
    public void notifySubscriber(Subscriber subscriber, Event event) {
        Runnable job = () -> subscriber.onEvent(event);
        Executor executor = subscriber.executor();
        if (executor != null) {
            executor.execute(job);
        } else {
            try {
                job.run();
            } catch (Throwable ex) {
                LOGGER.error("Event callback exception:", ex);
            }
        }
    }

    @Override
    public void shutdown() {
        shutdown = true;
        queue.clear();
    }
}
