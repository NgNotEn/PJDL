package joining;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

public class ThreadPool {
    public static final int THREAD = 1;
    
    public static ExecutorService executorService;
    public static ForkJoinPool sortService;
    

    /**
     * 初始化线程池
     */
    public static void init() {
 
        executorService = Executors.newFixedThreadPool(THREAD);
        sortService = new ForkJoinPool(THREAD);
        
        // 添加关闭钩子，确保程序退出时线程池能正确关闭
        Runtime.getRuntime().addShutdownHook(new Thread(ThreadPool::shutdown));
    }

    /**
     * 关闭所有线程池
     */
    public static void shutdown() {
        
        try {
            // 关闭普通线程池
            if (executorService != null) {
                executorService.shutdown();
                if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
                    executorService.shutdownNow();
                }
            }
            
            // 关闭ForkJoinPool
            if (sortService != null) {
                sortService.shutdown();
                if (!sortService.awaitTermination(5, TimeUnit.SECONDS)) {
                    sortService.shutdownNow();
                }
            }
            
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            // 强制关闭
            if (executorService != null) {
                executorService.shutdownNow();
            }
            if (sortService != null) {
                sortService.shutdownNow();
            }
        } finally {
            executorService = null;
            sortService = null;
        }
    }


}