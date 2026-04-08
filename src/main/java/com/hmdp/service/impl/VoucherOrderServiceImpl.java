package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;

import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;

import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;
    @Resource
    private RedisIdWorker redisIdWorker;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private RedissonClient redissonClient;
    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;
    static {
        SECKILL_SCRIPT = new DefaultRedisScript();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();

    private BlockingQueue<VoucherOrder> orderTasks=new ArrayBlockingQueue<>(1024*1024);
    String queueName = "stream.orders";

    private class VoucherOrderHandler implements Runnable {

        @Override
        public void run() {
            while (true) {
                try {
                    //1. 获取队列中的订单信息 XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 STREAMS stream.orders >
                    List<MapRecord<String, Object, Object>> records = stringRedisTemplate.opsForStream().read(Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            //ReadOffset.lastConsumed()底层就是 '>'
                            StreamOffset.create(queueName, ReadOffset.lastConsumed()));
                    //2. 判断消息是否获取成功
                    if (records == null || records.isEmpty()) {
                        continue;
                    }
                    //3. 消息获取成功之后，我们需要将其转为对象
                    MapRecord<String, Object, Object> record = records.get(0);
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
                    //4. 获取成功，执行下单逻辑，将数据保存到数据库中
                    handleVoucherOrder(voucherOrder);
                    //5. 手动ACK，SACK stream.orders g1 id
                    stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", record.getId());
                } catch (Exception e) {
                    log.error("订单处理异常", e);
                    //订单异常的处理方式我们封装成一个函数，避免代码太臃肿
                    handlePendingList();
                }
            }
        }
    }

    private void handlePendingList() {
        while (true) {
            try {
                //1. 获取pending-list中的订单信息 XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 STREAMS stream.orders 0
                List<MapRecord<String, Object, Object>> records = stringRedisTemplate.opsForStream().read(
                        Consumer.from("g1", "c1"),
                        StreamReadOptions.empty().count(1),
                        StreamOffset.create(queueName, ReadOffset.from("0")));
                //2. 判断pending-list中是否有未处理消息
                if (records == null || records.isEmpty()) {
                    //如果没有就说明没有异常消息，直接结束循环
                    break;
                }
                //3. 消息获取成功之后，我们需要将其转为对象
                MapRecord<String, Object, Object> record = records.get(0);
                Map<Object, Object> values = record.getValue();
                VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
                //4. 获取成功，执行下单逻辑，将数据保存到数据库中
                handleVoucherOrder(voucherOrder);
                //5. 手动ACK，SACK stream.orders g1 id
                stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", record.getId());
            } catch (Exception e) {
                log.error("处理pending-list异常");
                //如果怕异常多次出现，可以在这里休眠一会儿
                try {
                    Thread.sleep(50);
                } catch (InterruptedException ex) {
                    throw new RuntimeException(ex);
                }
            }
        }
    }
//    String queueName = "stream:orders";
//    private class VoucherOrderHandler implements Runnable {
//
//        @Override
//        public void run() {
//
//            while (true)
//                try {
//                    //获取队列中的订单信息
////                    VoucherOrder voucherOrder = orderTasks.take();
//                    //1.获取消息队列中的订单信息 XREADGROUP GROUP g1 c2 COUNT 1 BLOCK 2000 STREAMS stream:orders >
//                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
//                            Consumer.from("g1", "c1"),
//                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
//                            StreamOffset.create(queueName, ReadOffset.lastConsumed())
//                    );
//                    //2.判断消息是否获取成功（是否为0）
//                    if (list == null || list.isEmpty()) {
//                        //2.1如果获取失败，说明没有消息，继续下一轮循环
//                        continue;
//                    }
//                    //3.如果获取成功，可以下单
//                    //先进行解析
//                    MapRecord<String, Object, Object> record = list.get(0);
//                    Map<Object, Object> values = record.getValue();
//                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
//                    //创建订单
//                    handleVoucherOrder(voucherOrder);
//                    //4.ACK确认 SACK stream:orders g1 id
//                    stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", record.getId());
//                } catch (Exception e) {
//                    log.error("订单处理异常", e);
//                    //订单异常的处理方式我们封装成一个函数，避免代码太臃肿
//                    handlePendingList();
//                }
//
//        }
//    }
//
//    private void handlePendingList() {
//        while (true)
//            try {
//                //获取队列中的订单信息
////                    VoucherOrder voucherOrder = orderTasks.take();
//                //1.获取消息队列中的订单信息 XREADGROUP GROUP g1 c2 COUNT 1 BLOCK 2000 STREAMS stream:orders >
//                List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
//                        Consumer.from("g1", "c1"),
//                        StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
//                        StreamOffset.create(queueName, ReadOffset.lastConsumed())
//                );
//                //2.判断消息是否获取成功（是否为0）
//                if(list==null||list.isEmpty()){
//                    //2.1如果获取失败，说明penging-list没有消息，跳出循环
//                    break;
//                }
//                //3.如果获取成功，可以下单
//                //先进行解析
//                MapRecord<String, Object, Object> record = list.get(0);
//                Map<Object, Object> values = record.getValue();
//                VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
//                //创建订单
//                handleVoucherOrder(voucherOrder);
//                //4.ACK确认 SACK stream:orders g1 id
//                stringRedisTemplate.opsForStream().acknowledge(queueName,"g1",record.getId());
//            } catch (Exception e) {
//                log.error("处理pending-list异常");
//                //如果怕异常多次出现，可以在这里休眠一会儿
//                try {
//                    Thread.sleep(50);
//                } catch (InterruptedException ex) {
//                    throw new RuntimeException(ex);
//                }
//            }
//    }

    @PostConstruct
    private void init() {
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }
//    private BlockingQueue<VoucherOrder> orderTasks=new ArrayBlockingQueue<>(1024*1024);
//    private class VoucherOrderHandler implements Runnable {
//        @Override
//        public void run() {
//            while (true)
//                try {
//                    //获取队列中的订单信息
//                    VoucherOrder voucherOrder = orderTasks.take();
//                    //创建订单
//                    handleVoucherOrder(voucherOrder);
//                } catch (InterruptedException e) {
//                    throw new RuntimeException(e);
//                }
//        }
//    }

    private IVoucherOrderService proxy;
    private void handleVoucherOrder(VoucherOrder voucherOrder) {
        Long userId = voucherOrder.getUserId();
        RLock lock = redissonClient.getLock("lock:order:" + userId);
        //boolean tryLock = lock.tryLock(1200);
        boolean tryLock = lock.tryLock();
            if(!tryLock){
                log.error("不允许重复下单");
                //如果获取失败
                return  ;
            }
            try {
                //5. 使用代理对象，由于这里是另外一个线程，
                proxy.createVoucherOrder(voucherOrder);
            } finally {
                lock.unlock();
            }
        }

    @Override
    public Result seckillVoucher(Long voucherId) {
        long orderId = redisIdWorker.nextId("order");
        Long result = stringRedisTemplate.execute(SECKILL_SCRIPT,
                Collections.emptyList(), voucherId.toString(),
                UserHolder.getUser().getId().toString(), String.valueOf(orderId));
        if (result.intValue() != 0) {
            return Result.fail(result.intValue() == 1 ? "库存不足" : "不能重复下单");
        }
        //主线程获取代理对象
        proxy = (IVoucherOrderService) AopContext.currentProxy();
        return Result.ok(orderId);
    }
//    @Override
//    public Result seckillVoucher(Long voucherId) {
//        //封装订单信息ID
//        long orderId = redisIdWorker.nextId("order");
//        //执行LUA脚本
//        Long result = stringRedisTemplate.execute(
//                SECKILL_SCRIPT,
//                Collections.emptyList(),
//                voucherId.toString(), UserHolder.getUser().getId().toString(),String.valueOf(orderId)
//        );
//        int r = result.intValue();
//        if(r !=0){
//            //判断是否为0
//            //不为0，代表没有购买资格
//            return Result.fail(r==1?"库存不足":"不允许重复下单");
//        }
////        //为0，代表有购买资格，并且把下单信息加入阻塞队列中
////        //long orderId = redisIdWorker.nextId("order");
////        //保存阻塞队列
////        VoucherOrder voucherOrder = new VoucherOrder();
////
////        voucherOrder.setId(orderId);
////        //封装用户ID
////        Long userId = UserHolder.getUser().getId();
////        voucherOrder.setUserId(userId);
////        //封装优惠券ID
////        voucherOrder.setVoucherId(voucherId);
////
////        orderTasks.add(voucherOrder);
//
//        //获取代理对象(事物)
//        //IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
//        //主线程获取代理对象
//        proxy = (IVoucherOrderService) AopContext.currentProxy();
//        return Result.ok(orderId);
//    }

//    @Override
//    public Result seckillVoucher(Long voucherId) {
//        //执行LUA脚本
//        Long result = stringRedisTemplate.execute(
//                SECKILL_SCRIPT,
//                Collections.emptyList(),
//                voucherId.toString(), UserHolder.getUser().getId().toString()
//        );
//        int r = result.intValue();
//        if(r !=0){
//            //判断是否为0
//            //不为0，代表没有购买资格
//            return Result.fail(r==1?"库存不足":"不允许重复下单");
//        }
//        //为0，代表有购买资格，并且把下单信息加入阻塞队列中
//        //long orderId = redisIdWorker.nextId("order");
//        //保存阻塞队列
//        VoucherOrder voucherOrder = new VoucherOrder();
//        //封装订单信息ID
//        long orderId = redisIdWorker.nextId("order");
//        voucherOrder.setId(orderId);
//        //封装用户ID
//        Long userId = UserHolder.getUser().getId();
//        voucherOrder.setUserId(userId);
//        //封装优惠券ID
//        voucherOrder.setVoucherId(voucherId);
//
//        orderTasks.add(voucherOrder);
//
//        //获取代理对象(事物)
//        //IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
//        //主线程获取代理对象
//        proxy = (IVoucherOrderService) AopContext.currentProxy();
//        return Result.ok(orderId);
//    }
//    @Override
//    public Result seckillVoucher(Long voucherId) {
//        //1.查询优惠券ID
//        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
//        //2.判断秒杀开始时间
//        if(voucher.getBeginTime().isAfter(LocalDateTime.now())){
//            return Result.fail("秒杀还未开始");
//        }
//        //3.判断秒杀结束时间
//        if(voucher.getEndTime().isBefore(LocalDateTime.now())){
//            return Result.fail("秒杀已经结束");
//        }
//        //4.判断库存是否足够
//        if(voucher.getStock()<=0){
//            return Result.fail("库存不足");
//        }
//
//        //一人一单
//        Long userId=UserHolder.getUser().getId();
////        synchronized(userId.toString().intern()){
//            //SimpleRedisLock lock = new SimpleRedisLock("order:" + userId, stringRedisTemplate);
//        RLock lock = redissonClient.getLock("lock:order:" + userId);
//        //boolean tryLock = lock.tryLock(1200);
//        boolean tryLock = lock.tryLock();
//            if(!tryLock){
//                //如果获取失败
//                return  Result.fail("不允许重复下单");
//            }
//            try {
//                //获取代理对象(事物)
//                IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
//                return proxy.createVoucherOrder(voucherId);
//            } finally {
//                lock.unlock();
//            }
//        }
   // }

    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder) {

            //Long userId=UserHolder.getUser().getId();
         // 一人一单逻辑
        Long userId = voucherOrder.getUserId();
        Long voucherId = voucherOrder.getVoucherId();
        //查询用户与该订单的记录是否存在
            Integer count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
            if (count > 0) {
                log.error("您已经购买过了!");
                return;
                //return Result.fail("您已经购买过了!");
            }

        //5.扣减库存
//        int nowStock=voucher.getStock()-1;
//        voucher.setStock(nowStock);
        //采用CAS(compare and set)的方法来达到乐观锁的操作解决高并发
        //eq("stock",voucher.getStock())
        boolean success = seckillVoucherService.update()
                .setSql("stock=stock-1")//set
                .eq("voucher_id", voucherId)
                .gt("stock",0)//where id=?,stock=?
                .update();
        if(!success){
//            return Result.fail("库存不足");
            log.error("库存不足");
            return;
        }
        //7. 将订单数据保存到表中
        save(voucherOrder);
//            //6.提交订单
//            VoucherOrder voucherOrder = new VoucherOrder();
//            //封装订单信息ID
//            long orderId = redisIdWorker.nextId("order");
//            voucherOrder.setId(orderId);
//            //封装用户ID
//            //Long userId = UserHolder.getUser().getId();
//            voucherOrder.setUserId(userId);
//            //封装优惠券ID
//            voucherOrder.setVoucherId(voucherOrder);
//            //保存回数据库
            //save(voucherOrder);
            //7.返回结果
//            return Result.ok(orderId);

    }


}
