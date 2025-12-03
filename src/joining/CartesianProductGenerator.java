package joining;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * 笛卡尔积生成器
 * 输入: List<int[]>，每个int[]代表一个维度的所有可能值
 * 输出: 所有可能的组合（笛卡尔积）
 */
public class CartesianProductGenerator implements Iterable<int[]>, Iterator<int[]> {
    private final List<int[]> dimensions;
    private final int[] currentIndices;
    private final int[] currentCombination;
    private final int totalCombinations;
    private int remainingCombinations;
    private boolean hasNext;

    /**
     * 构造函数
     * @param dimensions List<int[]>，每个数组代表一个维度的所有可能值
     */
    public CartesianProductGenerator(List<int[]> dimensions) {
        if (dimensions == null || dimensions.isEmpty()) {
            throw new IllegalArgumentException("Dimensions cannot be null or empty");
        }
        
        this.dimensions = dimensions;
        this.currentIndices = new int[dimensions.size()];
        this.currentCombination = new int[dimensions.size()];
        
        // 计算总组合数
        int total = 1;
        for (int[] dimension : dimensions) {
            if (dimension == null || dimension.length == 0) {
                throw new IllegalArgumentException("Each dimension must have at least one value");
            }
            total *= dimension.length;
        }
        this.totalCombinations = total;
        this.remainingCombinations = total;
        this.hasNext = (total > 0);
        
        // 初始化第一个组合
        if (hasNext) {
            updateCurrentCombination();
        }
    }

    /**
     * 获取总组合数
     */
    public int getTotalCombinations() {
        return totalCombinations;
    }

    /**
     * 获取剩余组合数
     */
    public int getRemainingCombinations() {
        return remainingCombinations;
    }

    @Override
    public boolean hasNext() {
        return hasNext;
    }

    @Override
    public int[] next() {
        if (!hasNext) {
            throw new NoSuchElementException("No more combinations available");
        }
        
        // 复制当前组合（避免返回内部数组引用）
        int[] result = Arrays.copyOf(currentCombination, currentCombination.length);
        
        // 生成下一个组合
        generateNext();
        
        return result;
    }

    /**
     * 生成下一个组合
     */
    private void generateNext() {
        remainingCombinations--;
        
        if (remainingCombinations == 0) {
            hasNext = false;
            return;
        }
        
        // 从最后一个维度开始递增
        for (int i = dimensions.size() - 1; i >= 0; i--) {
            if (++currentIndices[i] < dimensions.get(i).length) {
                // 当前维度可以递增，不需要进位
                updateCurrentCombination();
                return;
            } else {
                // 当前维度归零，继续处理前一个维度
                currentIndices[i] = 0;
            }
        }
        
        // 如果所有维度都已归零，说明没有更多组合
        hasNext = false;
    }

    /**
     * 根据当前索引更新组合值
     */
    private void updateCurrentCombination() {
        for (int i = 0; i < dimensions.size(); i++) {
            currentCombination[i] = dimensions.get(i)[currentIndices[i]];
        }
    }

    @Override
    public Iterator<int[]> iterator() {
        return this;
    }

    /**
     * 不支持remove操作
     */
    @Override
    public void remove() {
        throw new UnsupportedOperationException("Remove operation is not supported");
    }

    /**
     * 静态工具方法：一次性生成所有笛卡尔积组合
     */
    public static List<int[]> generateAll(List<int[]> dimensions) {
        List<int[]> results = new ArrayList<>();
        CartesianProductGenerator generator = new CartesianProductGenerator(dimensions);
        
        while (generator.hasNext()) {
            results.add(generator.next());
        }
        
        return results;
    }
}