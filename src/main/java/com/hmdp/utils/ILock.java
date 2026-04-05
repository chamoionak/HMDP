package com.hmdp.utils;

public interface ILock {
    //尝试获取锁，提供锁持有的超时时间
    boolean tryLock(long timeoutSec);

    void unlock();
}
