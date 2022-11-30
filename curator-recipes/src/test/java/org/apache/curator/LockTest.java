package org.apache.curator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;

/**
 * @author Saint
 * @CreateTime 2022-11-29
 */
public class LockTest {
    public static void main(String[] args) {
        //重试策略,定义初试时间3s,重试3次
        ExponentialBackoffRetry exponentialBackoffRetry = new ExponentialBackoffRetry(3000, 3);

        //初始化客户端
        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString("127.0.0.1:2181")
                .sessionTimeoutMs(3000)
                .connectionTimeoutMs(3000)
                .retryPolicy(exponentialBackoffRetry)
                .build();
        // start()开始连接，没有此会报错
        client.start();
        //利用zookeeper的类似于文件系统的特性进行加锁  第二个参数指定锁的路径
        InterProcessMutex interProcessMutex = new InterProcessMutex(client, "/lock");

        try {
            //加锁
            interProcessMutex.acquire();
            System.out.println(Thread.currentThread().getName() + "获取锁成功");
            Thread.sleep(60_000);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                //释放锁
                interProcessMutex.release();
                System.out.println(Thread.currentThread().getName() + "释放锁成功");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
