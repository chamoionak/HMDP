package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

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

    @Autowired
    private StringRedisTemplate stringRedisTemplate;
    @Override
    public Result queryById(Long id) {
        //实现缓存穿透
        //Shop shop = queryWithPassThrough(id);
        //用互斥锁解决缓存击穿
        Shop shop = queryWithMutex(id);
        if(shop==null){
            return Result.fail("店铺不存在");
        }
        return Result.ok(shop);
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
            Thread.sleep(100);
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
