package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.dto.ScrollResult;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Blog;
import com.hmdp.entity.Follow;
import com.hmdp.entity.User;
import com.hmdp.mapper.BlogMapper;
import com.hmdp.service.IBlogService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.service.IFollowService;
import com.hmdp.service.IUserService;
import com.hmdp.utils.SystemConstants;
import com.hmdp.utils.UserHolder;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.BLOG_LIKED_KEY;
import static com.hmdp.utils.RedisConstants.FEED_KEY;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class BlogServiceImpl extends ServiceImpl<BlogMapper, Blog> implements IBlogService {

    @Resource
    private IUserService userService;

    @Resource
    private IFollowService followService;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Override
    public Result queryHotBlog(Integer current) {
        // 根据用户查询
        Page<Blog> page = query()
                .orderByDesc("liked")
                .page(new Page<>(current, SystemConstants.MAX_PAGE_SIZE));
        // 获取当前页数据
        List<Blog> records = page.getRecords();
        // 查询用户
        records.forEach(blog ->{
            this.queryBlogUser(blog);
            this.isBlogLiked(blog);
        });
        return Result.ok(records);
    }

    @Override
    public Result queryBlogById(Long id) {
        Blog blog = getById(id);
        if(blog == null){
            return Result.fail("博客不存在");
        }
        queryBlogUser(blog);
        //查询点赞
        isBlogLiked(blog);
        return Result.ok(blog);
    }

    private void isBlogLiked(Blog blog) {
        //1. 获取当前用户信息
        UserDTO user = UserHolder.getUser();
        //当用户未登录时，就不判断了，直接return结束逻辑
        if (user == null) {
            return;
        }
        Long userId = user.getId();
        //2. 判断当前用户是否点赞
        String key = BLOG_LIKED_KEY + blog.getId();
        Double score = stringRedisTemplate.opsForZSet().score(key, userId.toString());
        blog.setIsLike(score != null);
    }

    @Override
    public Result likeBlog(Long id) {
        //获取当前登录用户
        Long userId = UserHolder.getUser().getId();
        String key="Blog:liked:"+id;
        Double score = stringRedisTemplate.opsForZSet().score(key, userId.toString());
        //判断是否点赞
        if(score == null){
            //如果未点过赞，则可以点赞
            //数据库点赞数+1
            boolean isSuccess = update().setSql("liked = liked + 1").eq("id", id).update();
            //保存用户到redis的set集合
            if(isSuccess){
                stringRedisTemplate.opsForZSet().add(key, userId.toString(), System.currentTimeMillis());
            }
        }
        else {
            //如果点过赞，则取消点赞
            //数据库点赞数-1
            boolean isSuccess = update().setSql("liked = liked - 1").eq("id", id).update();
            //把用户从redis的set集合中移除
            if(isSuccess){
                stringRedisTemplate.opsForZSet().remove(key, userId.toString());
            }
        }
        return Result.ok();
    }

    @Override
    public Result queryBlogLikes(Long id) {
        String key = BLOG_LIKED_KEY + id;
        //zrange key 0 4  查询zset中前5个元素
        Set<String> top5 = stringRedisTemplate.opsForZSet().range(key, 0, 4);
        //如果是空的(可能没人点赞)，直接返回一个空集合
        if (top5 == null || top5.isEmpty()) {
            return Result.ok(Collections.emptyList());
        }
        List<Long> ids = top5.stream().map(Long::valueOf).collect(Collectors.toList());
        //将ids使用`,`拼接，SQL语句查询出来的结果并不是按照我们期望的方式进行排
        //所以我们需要用order by field来指定排序方式，期望的排序方式就是按照查询出来的id进行排序
        String idsStr = StrUtil.join(",", ids);
        //select * from tb_user where id in (ids[0], ids[1] ...) order by field(id, ids[0], ids[1] ...)
        List<UserDTO> userDTOS = userService.query().in("id", ids)
                .last("order by field(id," + idsStr + ")")
                .list().stream()
                .map(user -> BeanUtil.copyProperties(user, UserDTO.class))
                .collect(Collectors.toList());
        return Result.ok(userDTOS);
    }


//    @Override
//    public Result queryBlogLikes(Long id) {
//        String key=BLOG_LIKED_KEY+id;
//        //查询前五名
//        Set<String> top5 = stringRedisTemplate.opsForZSet().reverseRange(key, 0, 4);
//        if(top5 == null || top5.isEmpty()){
//            return Result.ok(Collections.emptyList());
//        }
//        //解析用户ID
//        List<Long> ids = top5.stream().map(Long::valueOf).collect(Collectors.toList());
//        //根据用户ID查询用户信息
//        List<UserDTO> userDTOS = userService.listByIds(ids)
//                .stream()
//                .map(user -> BeanUtil.copyProperties(user, UserDTO.class))
//                .collect(Collectors.toList());
//
//        return Result.ok(userDTOS);
//    }

    private void queryBlogUser(Blog blog) {
        Long userId = blog.getUserId();
        User user = userService.getById(userId);
        blog.setName(user.getNickName());
        blog.setIcon(user.getIcon());
    }

    @Override
    public Result saveBlog(Blog blog) {
        // 获取登录用户
        UserDTO user = UserHolder.getUser();
        blog.setUserId(user.getId());
        // 保存探店博文
        boolean isSuccess = save(blog);
        if (!isSuccess) {
            return Result.fail("新增笔记失败");
        }
        //查询笔记作者的所有粉丝
        List<Follow> follows = followService.query().eq("follow_user_id", user.getId()).list();
        //把笔记的ID推送给所有粉丝
        for(Follow follow:follows){
            //获取粉丝ID
            Long userId = follow.getUserId();
            //推送消息
            String key =FEED_KEY+ userId;
            stringRedisTemplate.opsForZSet().add(key,blog.getId().toString(),System.currentTimeMillis());
        }
        // 返回id
        return Result.ok(blog.getId());
    }

    @Override
    public Result queryBlogOfFollow(Long max, Integer offset) {
        //获取当前登录用户
        Long userId = UserHolder.getUser().getId();
        //查询收件箱
        String key =FEED_KEY+ userId;
        Set<ZSetOperations.TypedTuple<String>> typedTuples = stringRedisTemplate.opsForZSet()
                .reverseRangeByScoreWithScores(key, 0, max, offset, 2);
        if(typedTuples == null || typedTuples.isEmpty()){
            return Result.ok();
        }
        List<Long> ids = new ArrayList<>(typedTuples.size());
        long minTime=0;
        int os=1;
        //解析数据，blogId,时间戳，offset
        for (ZSetOperations.TypedTuple<String> tuple:typedTuples) {
            //获取Id
            String idStr = tuple.getValue();
            ids.add(Long.valueOf(idStr));
            //获取时间戳
            Long time = tuple.getScore().longValue();
            if(minTime==time){
                os++;
            }
            else {
                minTime=time;
                os=1;
            }
        }
        //根据Id查询blog
        String idStr=StrUtil.join(",",ids);
        List<Blog> blogs = query().in("id", ids).last("order by field(id," + idStr + ")").list();
        for (Blog blog : blogs) {
            queryBlogUser(blog);
            //查询点赞
            isBlogLiked(blog);
        }
        //封装并返回
        ScrollResult r = new ScrollResult();
        r.setList(blogs);
        r.setOffset(os);
        r.setMinTime(minTime);
        return Result.ok(r);
    }
}
