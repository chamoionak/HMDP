package com.hmdp.utils;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

@Component
public class RedisIdWorker {
    //开始时间戳
    private static final long BEGIN_TIMESTAMP=1767225600L;

    private static final int COUNT_BITS=32;

    private StringRedisTemplate stringRedisTemplate;
    public RedisIdWorker(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    public long nextId(String keyPrefix) {
        //生成时间戳
        LocalDateTime now = LocalDateTime.now();
        long nowEpochSecond = now.toEpochSecond(ZoneOffset.UTC);
        long timestamp=nowEpochSecond-BEGIN_TIMESTAMP;
        //生成序列号
        String date = now.format(DateTimeFormatter.ofPattern("yyyy:MM:dd"));
        Long increment = stringRedisTemplate.opsForValue().increment("icr" + keyPrefix + ":" + date);
        //拼接并返回
        //对时间戳进行左移32位，然后与序列号进行或运算
        return timestamp << COUNT_BITS | increment;
    }

    public static void main(String[] args) {
        LocalDateTime localDateTime = LocalDateTime.of(2026, 1, 1, 0, 0, 0);
        long epochSecond = localDateTime.toEpochSecond(ZoneOffset.UTC);
        System.out.println("seconds:"+epochSecond);
    }
}
