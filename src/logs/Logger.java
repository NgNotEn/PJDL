package logs;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class Logger {

    private static final Logger INSTANCE = new Logger();
    // 使用非阻塞的高并发队列，绝不拖慢你的算法速度
    private final Queue<String> logBuffer = new ConcurrentLinkedQueue<>();
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    private Logger() {}

    public static Logger getInstance() {
        return INSTANCE;
    }

    // ==========================================
    // 1. 系统级控制
    // ==========================================

    /**
     * 每次查询开始前调用，清空旧日志
     */
    public void reset() {
        logBuffer.clear();
    }

    /**
     * 获取所有日志 (供 WebServer 读取并发送给前端)
     */
    public List<String> getLogs() {
        return new ArrayList<>(logBuffer);
    }

    // ==========================================
    // 2. 核心业务接口 (请在你的算法中调用这些)
    // ==========================================

    /**
     * 记录大阶段切换
     * 例如: "Partitioning", "Dynamic Grouping", "Parallel Join"
     */
    public void logPhase(String phaseName) {
        // 格式: TIME | PHASE | ThreadName | Content
        append("PHASE", phaseName);
    }

    /**
     * 记录分组策略详情 (这是展示算法智能性的关键)
     * 例如: varName="n_nationkey", details="Found 2 connected components"
     */
    public void logGroup(String varName, String details) {
        append("GROUP", "Var [" + varName + "]: " + details);
    }

    /**
     * [关键] 记录 Worker 开始执行任务 (用于画甘特图起点)
     * @param taskId 任务唯一标识 (比如分区ID)
     * @param info 额外信息 (比如 "Range 0-5000")
     */
    public void logWorkerStart(int taskId, String info) {
        // 格式: ... | WORKER_START | ... | TaskID | Info
        append("WORKER_START", "Task-" + taskId + " | " + info);
    }

    /**
     * [关键] 记录 Worker 结束任务 (用于画甘特图终点)
     * @param taskId 任务唯一标识 (必须与 Start 的 ID 对应)
     * @param resultCount 这一波查到了多少条数据
     */
    public void logWorkerEnd(int taskId, int resultCount) {
        append("WORKER_END", "Task-" + taskId + " | Result: " + resultCount);
    }

    /**
     * 记录普通信息
     */
    public void info(String msg) {
        append("INFO", msg);
    }
    
    /**
     * 记录错误
     */
    public void error(String msg) {
        append("ERROR", msg);
    }

    // ==========================================
    // 3. 内部格式化实现
    // ==========================================
    
    // 生成格式: [时间] | [类型] | [线程名] | [内容]
    // 这种格式前端解析起来非常快
    private void append(String type, String content) {
        StringBuilder sb = new StringBuilder();
        sb.append("[").append(LocalDateTime.now().format(TIME_FORMATTER)).append("]");
        sb.append(" | ").append(type);
        // sb.append(" | ").append(Thread.currentThread().getName());
        sb.append(" | ").append(content);
        
        logBuffer.add(sb.toString());
    }
}