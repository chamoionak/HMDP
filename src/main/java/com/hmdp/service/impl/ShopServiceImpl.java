package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisData;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private CacheClient cacheClient;

    @Override
    public Result queryById(Long id) {
        //实现缓存穿透
        //Shop shop = queryWithPassThrough(id);
        //工具类解决
        //Shop shop = cacheClient.queryWithPassThrough(CACHE_SHOP_KEY, id, Shop.class, this::getById, CACHE_SHOP_TTL, TimeUnit.MINUTES);

        //用互斥锁解决缓存击穿
        //Shop shop = queryWithMutex(id);
        //工具类解决
        Shop shop = cacheClient.queryWithLogicalExpire(CACHE_SHOP_KEY, id, Shop.class, this::getById, CACHE_SHOP_TTL, TimeUnit.MINUTES);

        //用逻辑过期解决缓存击穿
        //Shop shop = queryWithLogicalExpire(id);
        if(shop==null){
            return Result.fail("店铺不存在");
        }
        return Result.ok(shop);
    }

    //线程池
    private static final ExecutorService CACHE_REBUILD_EXECUTOR= Executors.newFixedThreadPool(10);

    public Shop queryWithLogicalExpire(Long id){
        String key=CACHE_SHOP_KEY+id;

        //1.先在Redis中查询
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        //2.若命中直接返回(只有有数据的情况下才会触发如json=“ABC”)
        if(StrUtil.isBlank(shopJson)){
            return null;
        }
        //如果缓存命中（反序列化），判断缓存是否过期
        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
        LocalDateTime expireTime = redisData.getExpireTime();
        JSONObject data = (JSONObject) redisData.getData();
        Shop shop = JSONUtil.toBean(data, Shop.class);
        //如未过期，返回商铺信息
        if(expireTime.isAfter(LocalDateTime.now())){
            return shop;
        }
        //如已经过期，尝试获取互斥锁
        String lockKey=LOCK_SHOP_KEY+id;
        boolean trylock = trylock(lockKey);

        if (trylock){
            //二次判断是否过期
            if(expireTime.isAfter(LocalDateTime.now())){
                return shop;
            }
            //如获取成功开启独立线程，实现缓存重建
            CACHE_REBUILD_EXECUTOR.submit(()->{
                try {
                    this.saveShop2Redis(id,20L);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    unlock(lockKey);
                }
            });
        }
        //如获取失败则返回
        return shop;
    }

    public Shop queryWithMutex(Long id){
        String key=CACHE_SHOP_KEY+id;
        //1.先在Redis中查询
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        //2.若命中直接返回(只有有数据的情况下才会触发如json=“ABC”)
        if(StrUtil.isNotBlank(shopJson)){
            return JSONUtil.toBean(shopJson, Shop.class);
        }
        //不等于null相当于等于“”，所以这里用的是!=null来判断是否命中
        if(shopJson!=null){
            return null;
        }

        //3.实现缓存重建
        //3.1尝试获取互斥锁
        String lockKey=LOCK_SHOP_KEY+id;
        //3.2判断是否获取成功
        Shop shop = null;
        try {
            boolean lock = trylock(lockKey);
            //3.3获取失败，休眠一段时间重新查询redis
            if(!lock){
                Thread.sleep(50);
                //使用递归来达到重新查询的效果
                return queryWithMutex(id);
            }
            //3.4获取成功，查询数据库，将数据写入缓存并释放互斥锁，返回数据
            //若未命中，根据ID查询数据库
            shop = getById(id);
            //模拟延时高并发
            //Thread.sleep(100);
            if(shop==null)
            {
                //将null写入redis
                stringRedisTemplate.opsForValue().set(key,"",CACHE_NULL_TTL, TimeUnit.MINUTES);
                //5.ID不一致则返回404
                return null;
            }
            //4.ID一致则返回，将查询结果放入Redis
            stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(shop),CACHE_SHOP_TTL, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        finally {
            unlock(lockKey);
        }
        return shop;
    }
    private boolean trylock(String key){
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }
    private void unlock(String key){
        stringRedisTemplate.delete(key);
    }

    public void saveShop2Redis(Long id,Long expireSeconds) throws InterruptedException {
        //查询店铺数据
        Shop shop = getById(id);
        Thread.sleep(100);
        //进行封装
        RedisData data = new RedisData();
        data.setData(shop);
        data.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));
        //写入Redis
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY+id,JSONUtil.toJsonStr(data));
    }

    public Shop queryWithPassThrough(Long id){
        String key=CACHE_SHOP_KEY+id;
        //1.先在Redis中查询
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        //2.若命中直接返回(只有有数据的情况下才会触发如json=“ABC”)
        if(StrUtil.isNotBlank(shopJson)){
            return JSONUtil.toBean(shopJson, Shop.class);
        }
        //不等于null相当于等于“”，所以这里用的是!=null来判断是否命中
        if(shopJson!=null){
            return null;
        }
        //3.若未命中，根据ID查询数据库
        Shop shop = getById(id);
        if(shop==null)
        {
            //将null写入redis
            stringRedisTemplate.opsForValue().set(key,"",CACHE_NULL_TTL, TimeUnit.MINUTES);
            //5.ID不一致则返回404
            return null;
        }
        //4.ID一致则返回，将查询结果放入Redis
        stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(shop),CACHE_SHOP_TTL, TimeUnit.MINUTES);
        return shop;
    }
    @Override
    @Transactional
    public Result update(Shop shop) {
        Long id=shop.getId();
        if(id==null)
        {
            return Result.fail("店铺ID不能为空");
        }
        //更新数据库
        updateById(shop);
        //删除缓存
        String key=CACHE_SHOP_KEY+id;
        stringRedisTemplate.delete(key);
        return  Result.ok();
    }
}
