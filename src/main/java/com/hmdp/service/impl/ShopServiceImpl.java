package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisData;
import com.hmdp.utils.SystemConstants;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.sql.Array;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
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

    @Override
    public Result queryShopByType(Integer typeId, Integer current, Double x, Double y) {
        //1.判断是否需要根据坐标查询
        if(x==null||y==null){
            // 根据类型分页查询
            Page<Shop> page = query()
                    .eq("type_id", typeId)
                    .page(new Page<>(current, SystemConstants.DEFAULT_PAGE_SIZE));
            // 返回数据
            return Result.ok(page.getRecords());
        }
        //计算分页参数
        int from = (current-1)*SystemConstants.DEFAULT_PAGE_SIZE;
        int end = current*SystemConstants.DEFAULT_PAGE_SIZE;
        //查询redis,按照距离排序，分页，结果：shopId,distance
        String key=SHOP_GEO_KEY+typeId;
        GeoResults<RedisGeoCommands.GeoLocation<String>> results = stringRedisTemplate.opsForGeo().search(key,
                GeoReference.fromCoordinate(x, y),
                new Distance(5000),
                RedisGeoCommands.GeoSearchCommandArgs.newGeoSearchArgs().includeDistance().limit(end)
        );
        //解析出ID
        if(results==null){
            return Result.ok(Collections.emptyList());
        }
        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> list = results.getContent();
        if (list.size() < from) {
            //起始查询位置大于数据总量，则说明没数据了，返回空集合
            return Result.ok(Collections.emptyList());
        }
        //根据ID查询shop
        List<Object> ids = new ArrayList<>(list.size());
        HashMap<String, Distance> distanceMap = new HashMap<>(list.size());
        list.stream().skip(from).forEach(result->{
            String shopIdStr = result.getContent().getName();
            ids.add(Long.valueOf(shopIdStr));
            Distance distance = result.getDistance();
            distanceMap.put(shopIdStr,distance);
        });
        String idStr = StrUtil.join(",", ids);

        List<Shop> shops = query().in("id", ids).last("ORDER BY FIELD( id," + idStr + ")").list();
        for (Shop shop : shops) {
            //设置shop的举例属性，从distanceMap中根据shopId查询
            shop.setDistance(distanceMap.get(shop.getId().toString()).getValue());
        }
        //6. 返回
        return Result.ok(shops);
    }
}
