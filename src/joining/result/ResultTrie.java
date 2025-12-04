package joining.result;

import java.util.Arrays;
import java.util.BitSet;
import java.util.Set;

/* 
 * 作用于Trie，与表无关
 * 存储每次连接的结果，
 * 每构造一层都应该回溯剪枝
*/


public class ResultTrie {

    public final int aliasID;       // 表id
    public final int maxLevel;      // 最大深度
    public int curMaxLevel;         // 当前实际构建的深度

    // 分层存储的核心数据结构
    public int[][] levels;      // 每层所有的节点（引用trie，浅拷贝），初始加入是经过第i层的连接结果，但是后续需要剪枝会把剪掉的节点值设置为 INT.MAX
    public int[][] ranges;      // 存储父子节点关系 [start, end) 对
    public int[][] levelRanges; // 剪枝后的有效区域 [start,end))， 并非值块的有效区域，而是整体的有效区域，其中允许存在被删除的结点，可能只是用来缩小遍历范围的

    // 层遍历状态管理
    public int curLevel;        // 当前层
    public int[] positions;     // 记录了在每个层级上当前遍历的位置
    private int[] upperBounds;  // 每层的遍历上界
    
    // 添加缓存变量减少数组访问
    private int[] currentLevelArray;    // levels[curLevel]的缓存
    private int currentUpperBound;      // upperBounds[curLevel]的缓存

    public BitSet status;

    public ResultTrie(int aliasID, int maxLevel) {
        this.aliasID = aliasID;
        this.maxLevel = maxLevel;
        levels = new int[maxLevel + 1][];       // 初始化层->值数组
        ranges = new int[maxLevel + 1][];       // 初始化层
        levelRanges = new int[maxLevel + 1][2];
        positions = new int[maxLevel + 1];
        upperBounds = new int[maxLevel + 1];

        curMaxLevel = 0;                    // 设置根层
        levels[0] = new int[] { -1 };       // 设置根节点的值为 -1
        /*
         * 根节点的子节点范围初始为[-1,-1)
         * 
         * 对于ranges[i]表示的是该层有效的结点在下层的子节点分组，每一层的构建都代表着连接结果的加入，一般来说是全部有效的
         * 但随着后续的结点的构建，是需要不断剪枝的
         * 而每次构建的新层都是连接后的答案，所以我们直接通过下一层的有效值的父节点的值来具体的构造本层结点的有效范围，
         * 所以在构造本层时，将有效值块标为无效，在最后一层时可以再标为有效
         * 
        */
        ranges[0] = new int[] { -1, -1 };  
        levelRanges[0][0] = 0;
        levelRanges[0][1] = 1;
        status = new BitSet(1);
        status.set(0);
        
        // 初始化缓存
        updateLevelCache();                 // 更新缓存变量
    }
    
    // 更新当前层缓存
    private final void updateLevelCache() {
        if (curLevel <= curMaxLevel && levels[curLevel] != null) {
            currentLevelArray = levels[curLevel];
            currentUpperBound = upperBounds[curLevel];
        } else {
            currentLevelArray = null;
            currentUpperBound = 0;
        }
    }

    // 添加一层  只把有效的 join key（status=1）加入新层
    public void addLayer(int[] currentValues, int[] parentIndexes, int size) {
        curMaxLevel++;
        status = new BitSet(size); // 初始化状态位图
        // 引用新层有效节点值
        levels[curMaxLevel] = currentValues;

        // 初始化新层的有效范围（初始全部）
        levelRanges[curMaxLevel][0] = 0;
        levelRanges[curMaxLevel][1] = size;

        // 为最后一层和非最后一层分开处理
        // 更新最后一层的值和范围
        if (curMaxLevel == maxLevel) {
            // 获取父层范围数组
            int[] parentRanges = ranges[curMaxLevel - 1];   // 引用父层有效值块
            for (int i = 0; i < size; i++) {
                // 更新父节点的范围
                int parentIndex = parentIndexes[i]; // 重映射，第i个值的父结点在上一层的range中的实际索引
                int rangeIndex = parentIndex << 1;  // 准备获取父节点值块范围
                // 非最后层处理时，会为本层(会在下一层被使用所以是父层)设置为全-1
                if (parentRanges[rangeIndex] == -1) // 设置子节点起始
                    parentRanges[rangeIndex] = i;
                parentRanges[rangeIndex + 1] = i + 1;
            }
            return;
        }

        // 处理非最后一层 - 优化范围数组初始化
        int[] newRanges = new int[size << 1];
        Arrays.fill(newRanges, -1); // 批量初始化
        ranges[curMaxLevel] = newRanges;
        int[] parentRanges = ranges[curMaxLevel - 1];

        for (int i = 0; i < size; i++) {
            // 更新父节点的范围
            int parentIndex = parentIndexes[i];
            int rangeIndex = parentIndex << 1;
            if (parentRanges[rangeIndex] == -1)
                parentRanges[rangeIndex] = i;
            parentRanges[rangeIndex + 1] = i + 1;
        }
    }

    /**
     * 优化剪枝操作 - 减少重复计算和数组访问
     */
    public void pruneUpward(Set<Integer> needToStore, int pruneLevel) {
        // 确保pruneLevel是有效的
        if (pruneLevel <= 0 || pruneLevel > curMaxLevel)
            return;

        // 获取pruneLevel层的值和范围
        int[] pruneLevelValues = levels[pruneLevel];
        int start = levelRanges[pruneLevel][0];
        int end = levelRanges[pruneLevel][1];

        // 记录新的有效范围
        int newFirstValid = Integer.MAX_VALUE;      // 设置Integer.MAX_VALUE标记删除
        int newLastValidPlus1 = -1;
        boolean anyNodeDeleted = false;

        // 优化：使用位运算和减少条件判断
        for (int i = start; i < end; i++) {
            int value = pruneLevelValues[i];
            // 跳过已删除的节点
            if (value == Integer.MAX_VALUE) 
                continue;

            // 判断当前节点是否需要保留
            if (!needToStore.contains(value)) {
                // 将不需要保留的节点标记为删除
                pruneLevelValues[i] = Integer.MAX_VALUE;
                anyNodeDeleted = true;
            } else {
                // 保留此节点，更新有效范围
                newFirstValid = Math.min(newFirstValid, i);
                newLastValidPlus1 = Math.max(newLastValidPlus1, i + 1);
            }
        }

        // 更新pruneLevel层的有效范围
        if (newFirstValid != Integer.MAX_VALUE) {
            levelRanges[pruneLevel][0] = newFirstValid;
            levelRanges[pruneLevel][1] = newLastValidPlus1;
        } else {
            // 这一层没有有效节点
            levelRanges[pruneLevel][0] = 0;
            levelRanges[pruneLevel][1] = 0;
        }

        // 如果有节点被删除，且不是最顶层，则更新父层节点
        if (anyNodeDeleted && pruneLevel > 0)
            updateParentNodes(pruneLevel);
    }

    /**
     * 优化父节点更新 - 减少重复计算
     */
    private void updateParentNodes(int childLevel) {
        int parentLevel = childLevel - 1;
        if (parentLevel <= 0)
            return;

        int[] parentValues = levels[parentLevel];
        int[] parentRanges = ranges[parentLevel];
        int[] childValues = levels[childLevel];

        int parentStart = levelRanges[parentLevel][0];
        int parentEnd = levelRanges[parentLevel][1];

        int newFirstValid = Integer.MAX_VALUE;
        int newLastValidPlus1 = -1;
        boolean parentChanged = false;

        for (int i = parentStart; i < parentEnd; i++) {
            if (parentValues[i] == Integer.MAX_VALUE)
                continue; // 跳过已删除的节点

            int rangeIdx = i << 1;
            int childStart = parentRanges[rangeIdx];
            int childEnd = parentRanges[rangeIdx + 1];

            if (childStart == -1) {
                // 没有子节点，此节点保留   ？？？？？？
                newFirstValid = Math.min(newFirstValid, i);
                newLastValidPlus1 = Math.max(newLastValidPlus1, i + 1);
                continue;
            }

            // 检查是否有有效子节点 - 优化循环
            boolean hasValidChild = false;
            for (int j = childStart; j < childEnd && !hasValidChild; j++) {
                hasValidChild = (childValues[j] != Integer.MAX_VALUE);
            }

            if (!hasValidChild) {
                // 没有有效子节点，删除父节点
                parentValues[i] = Integer.MAX_VALUE;
                parentChanged = true;
            } else {
                // 有有效子节点，更新父节点的有效范围
                newFirstValid = Math.min(newFirstValid, i);
                newLastValidPlus1 = Math.max(newLastValidPlus1, i + 1);
            }
        }

        // 更新父层的有效范围
        if (newFirstValid != Integer.MAX_VALUE) {
            levelRanges[parentLevel][0] = newFirstValid;
            levelRanges[parentLevel][1] = newLastValidPlus1;
        } else {
            levelRanges[parentLevel][0] = 0;
            levelRanges[parentLevel][1] = 0;
        }

        // 如果父层有节点被删除，递归向上更新
        if (parentChanged && parentLevel > 0)
            updateParentNodes(parentLevel);
    }

    // 优化open方法 - 减少数组访问
    public final void open() {
        if (curLevel > curMaxLevel)
            return;

        int currentPosition = positions[curLevel];
        int nextLevel = curLevel + 1;

        // 获取子节点范围
        int rangeIdx = currentPosition << 1;
        int[] childRanges = ranges[curLevel];
        int childStart = childRanges[rangeIdx];
        int childEnd = childRanges[rangeIdx + 1];

        if (childStart != -1) {
            positions[nextLevel] = childStart;
            upperBounds[nextLevel] = childEnd;

            // 优化：跳过删除节点的查找
            int[] nextLevelValues = levels[nextLevel];
            int pos = positions[nextLevel];
            while (pos < childEnd && nextLevelValues[pos] == Integer.MAX_VALUE) {
                pos++;
            }
            positions[nextLevel] = pos;
        } else {
            positions[nextLevel] = 0;
            upperBounds[nextLevel] = -1;
        }

        curLevel = nextLevel;
        updateLevelCache(); // 更新缓存
    }

    // 返回上一层
    public final void up() {
        if (curLevel > 0) {
            curLevel--;
            updateLevelCache(); // 更新缓存
        }
    }

    // 在 reset() 中设置 upperBounds
    public void reset() {
        System.arraycopy(levelRanges[0], 0, levelRanges[0], 0, 0); // 预热数组访问
        for (int i = 0; i <= curMaxLevel; i++) {
            positions[i] = levelRanges[i][0];
            upperBounds[i] = levelRanges[i][1]; 
        }
        curLevel = 0;
        updateLevelCache();
    }

    // 优化key方法 - 使用缓存
    public final int key() {
        if (curLevel > curMaxLevel || currentLevelArray == null)
            return Integer.MAX_VALUE;
        
        int pos = positions[curLevel];
        return pos < currentLevelArray.length ? currentLevelArray[pos] : Integer.MAX_VALUE;
    }

    // 优化seek方法 - 减少数组访问次数
    public final void seek(int seekKey) {
        if (currentLevelArray == null || positions[curLevel] >= currentUpperBound) {
            positions[curLevel] = currentUpperBound;
            return;
        }

        int validStart = positions[curLevel];
        int validEnd = currentUpperBound;

        // 快速检查边界情况
        if (seekKey > currentLevelArray[validEnd - 1]) {
            positions[curLevel] = validEnd;
            return;
        }

        int lb = validStart;
        int ub = validEnd;

        // 优化二分查找
        while (lb < ub) {
            int mid = lb + ((ub - lb) >>> 1); // 避免溢出
            int midKey = currentLevelArray[mid];

            if (midKey == Integer.MAX_VALUE) {
                // 处理删除节点的情况
                int left = mid - 1;
                while (left >= lb && currentLevelArray[left] == Integer.MAX_VALUE)
                    left--;
                int right = mid + 1;
                while (right < ub && currentLevelArray[right] == Integer.MAX_VALUE)
                    right++;
                if (left >= lb) {
                    ub = left + 1;
                } else if (right < ub) {
                    lb = right;
                } else {
                    break;
                }
                continue;
            }

            if (midKey < seekKey) {
                lb = mid + 1;
            } else if (midKey > seekKey) {
                ub = mid;
            } else {
                positions[curLevel] = mid;
                return;
            }
        }

        positions[curLevel] = lb;
        if (positions[curLevel] >= validEnd) {
            positions[curLevel] = validEnd;
            return;
        }
        
        // 优化跳过删除节点
        int pos = positions[curLevel];
        while (pos < validEnd && currentLevelArray[pos] == Integer.MAX_VALUE) {
            pos++;
        }
        positions[curLevel] = pos;
    }

    // 优化next方法 - 使用缓存
    public final void next() {
        int pos = positions[curLevel] + 1;
        if (pos >= currentUpperBound) {
            positions[curLevel] = currentUpperBound;
            return;
        }
        
        // 优化跳过删除节点
        while (pos < currentUpperBound && currentLevelArray[pos] == Integer.MAX_VALUE) {
            pos++;
        }
        positions[curLevel] = pos;
    }

    // 优化atEnd判断 - 使用缓存
    public final boolean atEnd() {
        return curLevel > curMaxLevel || positions[curLevel] >= currentUpperBound;
    }

    public int getLastLevelIndex() {
        int index =  curLevel < 0 || currentLevelArray == null ? -1 : positions[curLevel];
        if(index != -1) status.set(index);
        return index;
    }
}