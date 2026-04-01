package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.entity.Shop;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hmdp.utils.RedisConstants.*;

@Slf4j
@Component
public class CacheClient {

    private final StringRedisTemplate stringRedisTemplate;
    public CacheClient(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    public void set(String key, Object value, Long time, TimeUnit unit){
        String str = JSONUtil.toJsonStr(value);
        stringRedisTemplate.opsForValue().set(key,str,time,unit);
    }

    public void setWithLogicalExpire(String key, Object value, Long time, TimeUnit unit){
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        String str = JSONUtil.toJsonStr(redisData);
        stringRedisTemplate.opsForValue().set(key,str);
    }

    public <R,ID>R queryWithPassThrough(String keyPrefix, ID id, Class<R>type, Function<ID,R>dbFallBack,
                                        Long time, TimeUnit unit){
        String key=keyPrefix+id;
        //1.先在Redis中查询
        String json = stringRedisTemplate.opsForValue().get(key);
        //2.若命中直接返回(只有有数据的情况下才会触发如json=“ABC”)
        if(StrUtil.isNotBlank(json)){
            return JSONUtil.toBean(json, type);
        }
        //不等于null相当于等于“”，所以这里用的是!=null来判断是否命中
        if(json!=null){
            return null;
        }
        //3.若未命中，根据ID查询数据库
        R r = dbFallBack.apply(id);
        if(r==null)
        {
            //将null写入redis
            stringRedisTemplate.opsForValue().set(key,"",CACHE_NULL_TTL, TimeUnit.MINUTES);
            //5.ID不一致则返回404
            return null;
        }
        //4.ID一致则返回，将查询结果放入Redis
        //stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(r),CACHE_SHOP_TTL, TimeUnit.MINUTES);
        this.setWithLogicalExpire(key,r,time,unit);
        return r;
    }

    //线程池
    private static final ExecutorService CACHE_REBUILD_EXECUTOR= Executors.newFixedThreadPool(10);

    private boolean trylock(String key){
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }
    private void unlock(String key){
        stringRedisTemplate.delete(key);
    }

    public <R,ID>R queryWithLogicalExpire(String keyPrefix,ID id,Class<R>type,Function<ID,R>dbFallBack,
                                          Long time, TimeUnit unit){
        String key=keyPrefix+id;

        //1.先在Redis中查询
        String json = stringRedisTemplate.opsForValue().get(key);
        //2.若命中直接返回(只有有数据的情况下才会触发如json=“ABC”)
        if(StrUtil.isBlank(json)){
            return null;
        }
        //如果缓存命中（反序列化），判断缓存是否过期
        RedisData redisData = JSONUtil.toBean(json, RedisData.class);
        LocalDateTime expireTime = redisData.getExpireTime();

        JSONObject data = (JSONObject) redisData.getData();
        R r = JSONUtil.toBean(data, type);
        //如未过期，返回商铺信息
        if(expireTime.isAfter(LocalDateTime.now())){
            return r;
        }
        //如已经过期，尝试获取互斥锁
        String lockKey=LOCK_SHOP_KEY+id;
        boolean trylock = trylock(lockKey);

        if (trylock){
            //二次判断是否过期
            if(expireTime.isAfter(LocalDateTime.now())){
                return r;
            }
            //如获取成功开启独立线程，实现缓存重建
            CACHE_REBUILD_EXECUTOR.submit(()->{
                try {
                    R r1= dbFallBack.apply(id);
                    this.setWithLogicalExpire(key,r1,time,unit);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    unlock(lockKey);
                }
            });
        }
        //如获取失败则返回
        return r;
    }

}
