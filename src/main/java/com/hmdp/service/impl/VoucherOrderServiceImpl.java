package com.hmdp.service.impl;

import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.SimpleRedisLock;
import com.hmdp.utils.UserHolder;
import org.springframework.aop.framework.AopContext;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.LocalDateTime;

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
    @Override
    public Result seckillVoucher(Long voucherId) {
        //1.查询优惠券ID
        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
        //2.判断秒杀开始时间
        if(voucher.getBeginTime().isAfter(LocalDateTime.now())){
            return Result.fail("秒杀还未开始");
        }
        //3.判断秒杀结束时间
        if(voucher.getEndTime().isBefore(LocalDateTime.now())){
            return Result.fail("秒杀已经结束");
        }
        //4.判断库存是否足够
        if(voucher.getStock()<=0){
            return Result.fail("库存不足");
        }

        //一人一单
        Long userId=UserHolder.getUser().getId();
        synchronized(userId.toString().intern()){
            SimpleRedisLock lock = new SimpleRedisLock("order:" + userId, stringRedisTemplate);

            boolean tryLock = lock.tryLock(1200);
            if(!tryLock){
                //如果获取失败
                return  Result.fail("不允许重复下单");
            }
            try {
                //获取代理对象(事物)
                IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
                return proxy.createVoucherOrder(voucherId);
            } finally {
                lock.unlock();
            }
        }
    }

    @Transactional
    public  Result createVoucherOrder(Long voucherId) {

            Long userId=UserHolder.getUser().getId();

            //查询用户与该订单的记录是否存在
            Integer count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
            if (count > 0) {
                return Result.fail("您已经购买过了!");
            }

        //5.扣减库存
//        int nowStock=voucher.getStock()-1;
//        voucher.setStock(nowStock);
        //采用CAS(compare and set)的方法来达到乐观锁的操作解决高并发
        //eq("stock",voucher.getStock())
        boolean success = seckillVoucherService.update()
                .setSql("stock=stock-1")//set
                .eq("voucher_id", voucherId).gt("stock",0)//where id=?,stock=?
                .update();
        if(!success){
            return Result.fail("库存不足");
        }
            //6.提交订单
            VoucherOrder voucherOrder = new VoucherOrder();
            //封装订单信息ID
            long orderId = redisIdWorker.nextId("order");
            voucherOrder.setId(orderId);
            //封装用户ID
            //Long userId = UserHolder.getUser().getId();
            voucherOrder.setUserId(userId);
            //封装优惠券ID
            voucherOrder.setVoucherId(voucherId);
            //保存回数据库
            save(voucherOrder);
            //7.返回结果
            return Result.ok(orderId);

    }
}
