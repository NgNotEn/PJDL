package util;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveTask;

public class CartesianProduct {

    // 使用更合适的线程池配置
    private static final ForkJoinPool FORK_JOIN_POOL = new ForkJoinPool(
            Math.min(Runtime.getRuntime().availableProcessors(), 8));

    // 调整并行阈值
    private static final int PARALLEL_THRESHOLD = 7000;
    private static final int CARTESIAN_THRESHOLD = 1000;

    // 添加状态跟踪字段
    private List<List<int[]>> nonEmptyGroups;   // 非空组
    private int[] groupSizes;                   // 每个组的大小 有效值数量，同组内表的有效值数量相同
    private int totalLength;
    private long totalCombinations;             // 组合数
    private int batchSize;
    private long currentIndex;
    private boolean hasMoreResults;

    /**
     * 构造函数 - 根据输入组初始化并计算分组大小
     */
    public CartesianProduct(List<List<int[]>> joinListInGroups) {
        initialize(joinListInGroups);
    }

    /**
     * 初始化计算参数和分组信息
     * @joinListInGroups 每个int[]，都是同组的一个索引组合，这个索引组合内分别是相对应的表内相同值（有效值）的下标
     */
    private void initialize(List<List<int[]>> joinListInGroups) {
        if (joinListInGroups == null || joinListInGroups.isEmpty()) {
            hasMoreResults = false;
            return;
        }

        // 过滤空组
        nonEmptyGroups = new ArrayList<>();
        for (List<int[]> group : joinListInGroups) {
            if (group != null && !group.isEmpty())
                nonEmptyGroups.add(group);
        }
        // 存在空组直接说明交集为空
        if (nonEmptyGroups.size() != joinListInGroups.size()) {
            hasMoreResults = false;
            return;
        }

        if (nonEmptyGroups.size() == 1) {
            // 单组数据特殊处理， 交集即自身
            totalCombinations = nonEmptyGroups.get(0).size();
            batchSize = (int) totalCombinations;
            hasMoreResults = true;
            currentIndex = 0;
            return;
        }

        // 计算组合数量和验证
        totalCombinations = 1;                          // 组合数是组间索引组合的数量的乘积
        groupSizes = new int[nonEmptyGroups.size()];    // 每个组的大小
        totalLength = 0;

        for (int i = 0; i < nonEmptyGroups.size(); i++) {
            groupSizes[i] = nonEmptyGroups.get(i).size();   // 每个组的索引组数量
            totalCombinations *= groupSizes[i];
            totalLength += nonEmptyGroups.get(i).get(0).length; // 每个组的索引组的长度（表数量）的加和 ==> 每个笛卡尔积组合的长度
        }

        // 计算每个结果项的内存占用
        long memoryPerItem = (totalLength * 4L + 24L); // 每个int 4字节 + 数组对象开销

        // 根据内存限制计算批处理大小
        long maxMemoryBytes = Runtime.getRuntime().maxMemory() / 10;
        batchSize = (int) Math.min(
                totalCombinations,
                Math.max(1, maxMemoryBytes / memoryPerItem));

        currentIndex = 0;       // 已处理的范围
        hasMoreResults = totalCombinations > 0;
    }

    /**
     * 获取下一批笛卡尔积结果
     */
    public int[][] nextResult() {
        if (!hasMoreResults) {
            return new int[0][];
        }

        // 处理单组特殊情况
        if (nonEmptyGroups.size() == 1) {
            hasMoreResults = false;
            return convertSingleGroup(nonEmptyGroups.get(0));
        }

        // 计算当前批次大小
        long remaining = totalCombinations - currentIndex;
        int currentBatchSize = (int) Math.min(batchSize, remaining);

        int[][] result;
        if (currentBatchSize > PARALLEL_THRESHOLD) {
            // 并行计算
            result = FORK_JOIN_POOL.invoke(new CartesianProductTask(
                    nonEmptyGroups, groupSizes, totalLength,
                    currentIndex, currentIndex + currentBatchSize));
        } else {
            // 顺序计算
            result = computeCartesianSequentially(
                    nonEmptyGroups, groupSizes, totalLength,
                    currentIndex, currentBatchSize);
        }

        // 更新状态
        currentIndex += currentBatchSize;
        if (currentIndex >= totalCombinations) {
            hasMoreResults = false;
        }

        return result;
    }

    /**
     * 检查是否还有更多结果可获取
     */
    public boolean hasMoreResults() {
        return hasMoreResults;
    }

    /**
     * 获取总组合数
     */
    public long getTotalCombinations() {
        return totalCombinations;
    }

    private int[][] convertSingleGroup(List<int[]> singleGroup) {
        int[][] result = new int[singleGroup.size()][];
        for (int i = 0; i < singleGroup.size(); i++)
            result[i] = singleGroup.get(i).clone();
        return result;
    }

    private int[][] computeCartesianSequentially(List<List<int[]>> groups,
            int[] groupSizes, int totalLength, long startIndex, int count) {
        
        // 初始化结果数组
        int[][] result = new int[count][];

        // 预计算偏移量和元素大小
        int[] offsets = new int[groups.size()];
        int[] elementSizes = new int[groups.size()];

        int currentOffset = 0;
    

        for (int i = 0; i < groups.size(); i++) {
            elementSizes[i] = groups.get(i).get(0).length;
            offsets[i] = currentOffset;
            currentOffset += elementSizes[i];
        }

        // 使用更高效的索引计算
        for (int i = 0; i < count; i++) {
            int[] merged = new int[totalLength];
            long temp = startIndex + i;

            for (int j = 0; j < groups.size(); j++) {
                int index = (int) (temp % groupSizes[j]);
                temp /= groupSizes[j];

                int[] selectedArray = groups.get(j).get(index);
                System.arraycopy(selectedArray, 0, merged, offsets[j], elementSizes[j]);
            }

            result[i] = merged;
        }

        return result;
    }

    // 优化后的并行任务类，修改为支持long类型索引
    private class CartesianProductTask extends RecursiveTask<int[][]> {
        private final List<List<int[]>> groups;
        private final int[] groupSizes;
        private final int totalLength;
        private final long start;
        private final long end;

        public CartesianProductTask(List<List<int[]>> groups, int[] groupSizes,
                int totalLength, long start, long end) {
            this.groups = groups;
            this.groupSizes = groupSizes;
            this.totalLength = totalLength;
            this.start = start;
            this.end = end;
        }

        @Override
        protected int[][] compute() {
            if (end - start <= CARTESIAN_THRESHOLD)
                return computeDirectly();

            long mid = start + (end - start) / 2;
            CartesianProductTask leftTask = new CartesianProductTask(
                    groups, groupSizes, totalLength, start, mid);
            CartesianProductTask rightTask = new CartesianProductTask(
                    groups, groupSizes, totalLength, mid, end);

            leftTask.fork();
            int[][] rightResult = rightTask.compute();
            int[][] leftResult = leftTask.join();

            // 合并结果
            int[][] result = new int[leftResult.length + rightResult.length][];
            System.arraycopy(leftResult, 0, result, 0, leftResult.length);
            System.arraycopy(rightResult, 0, result, leftResult.length, rightResult.length);
            return result;
        }

        private int[][] computeDirectly() {
            int resultLength = (int) (end - start);
            int[][] result = new int[resultLength][];

            // 预计算偏移量
            int[] offsets = new int[groups.size()];
            int[] elementSizes = new int[groups.size()];

            int currentOffset = 0;
            for (int i = 0; i < groups.size(); i++) {
                elementSizes[i] = groups.get(i).get(0).length;
                offsets[i] = currentOffset;
                currentOffset += elementSizes[i];
            }

            // 计算totalLength应该是所有组数组长度的和
            int totalLength = currentOffset;

            // 计算结果
            for (long idx = start; idx < end; idx++) {
                int[] merged = new int[totalLength];
                long temp = idx;

                for (int j = 0; j < groups.size(); j++) {
                    int index = (int) (temp % groupSizes[j]);
                    temp /= groupSizes[j];

                    int[] selectedArray = groups.get(j).get(index);
                    System.arraycopy(selectedArray, 0, merged, offsets[j], elementSizes[j]);
                }

                result[(int) (idx - start)] = merged;
            }

            return result;
        }
    }
}