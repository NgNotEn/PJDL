package joining;

import java.util.Arrays;


// 负责在单个连接任务中执行Leapfrog Join算法（针对一组Trie的当前层）
public class LeapFrogJoin {
    private int[][] arrays;         // 每个Trie当前层的值数组
    private int[][] arrayStatus;    // 有效标记
    private int[][] arrayBounds;    // 每个Trie当前层的值范围（在值数组中的起始和结束索引）
    private int arrayCnt;           // Trie的数量 - 迭代器数量
    private int[] currentPositions; // 每个迭代器的当前位置
    Trie[] inputTries;

    // 添加emptyResult字段避免重复计算
    private boolean emptyResult = false;

    // Estimated initial result size to minimize array resizing overhead
    // private static final int INITIAL_RESULT_SIZE = 64; // 增大初始大小

    // 预分配临时数组避免重复创建
    private int[] tempIndices;
    private int[] tempFirstValues;

    // 缓存数组长度减少字段访问
    private final int[] arrayLengths;

    /**
     * Constructor with optimized iterator sorting and early validation, 初始化，根据每个Trie的当前层数据，按第一个值排序
     * @param arrayIdxes 每个Trie的当前层的某个有效父节点的索引->可以获得所有Trie在当前层的在相同父路径下的所有节点值
     * @param inputTries 与当前连接变量相关的所有Trie
     */
    public LeapFrogJoin(int[] arrayIdxes, Trie[] inputTries) {
        this.arrayCnt = inputTries.length;
        this.inputTries = inputTries;

        // Early exit for empty input arrays
        if (arrayCnt == 0) {
            emptyResult = true;
            this.arrayLengths = new int[0];
            return;
        }

        this.arrays = new int[arrayCnt][];
        this.arrayBounds = new int[arrayCnt][];
        this.arrayStatus = new int[arrayCnt][];
        this.currentPositions = new int[arrayCnt];
        this.arrayLengths = new int[arrayCnt];

        // 预分配临时数组
        this.tempIndices = new int[arrayCnt];
        this.tempFirstValues = new int[arrayCnt];   // 迭代器首个值

        // Validate bounds and collect first values for sorting
        boolean hasEmptyBound = false;  // 空有效界限，交集为空
        for (int i = 0; i < arrayCnt; i++) {
            Trie trie = inputTries[i];

            // 在此处拓展trie的下一层

            int[] arrayBound = trie.boundsOfArray(arrayIdxes[i]);   // 获取父节点值块范围，也即本路径下的本层有效结点值范围

            // Early detection of empty bounds
            if (arrayBound[0] >= arrayBound[1]) {       // 对应路径没有任何值-交集为空，退出
                hasEmptyBound = true;
                break;
            }

            // Store first value for sorting optimization
            tempFirstValues[i] = trie.curLevelValues.data[arrayBound[0]];
            tempIndices[i] = i;

            // Store references to avoid repeated access
            arrays[i] = trie.curLevelValues.data;
            arrayBounds[i] = arrayBound;
            arrayStatus[i] = trie.curLevelValueStatus.data;
            arrayLengths[i] = arrayBound[1] - arrayBound[0]; // 缓存长度
        }

        if (hasEmptyBound) {
            emptyResult = true;
            return;
        }

        // Sort iterators by first value only if multiple iterators exist
        if (arrayCnt > 1) {
            sortIteratorsByFirstValue(tempFirstValues, tempIndices);

            // Rearrange arrays according to sorted order
            int[][] tempArrays = new int[arrayCnt][];
            int[][] tempBounds = new int[arrayCnt][];
            int[][] tempStatus = new int[arrayCnt][];
            int[] tempLengths = new int[arrayCnt];

            System.arraycopy(arrays, 0, tempArrays, 0, arrayCnt);
            System.arraycopy(arrayBounds, 0, tempBounds, 0, arrayCnt);
            System.arraycopy(arrayStatus, 0, tempStatus, 0, arrayCnt);
            System.arraycopy(arrayLengths, 0, tempLengths, 0, arrayCnt);
            
            // 索引重分配
            for (int i = 0; i < arrayCnt; i++) {
                int srcIdx = tempIndices[i];    // 排序后的索引
                arrays[i] = tempArrays[srcIdx];
                arrayBounds[i] = tempBounds[srcIdx];
                arrayStatus[i] = tempStatus[srcIdx];
                arrayLengths[i] = tempLengths[srcIdx];
            }
        }
    }

    /**
     * Optimized sorting for small arrays using insertion sort with special cases
     */
    private void sortIteratorsByFirstValue(int[] firstValues, int[] indices) {
        // Special optimization for common small cases
        switch (arrayCnt) {
            case 2:
                if (firstValues[indices[0]] > firstValues[indices[1]]) {
                    int temp = indices[0];
                    indices[0] = indices[1];
                    indices[1] = temp;
                }
                return;
            case 3:
                // 优化三元素排序
                int idx0 = indices[0], idx1 = indices[1], idx2 = indices[2];
                int val0 = firstValues[idx0], val1 = firstValues[idx1], val2 = firstValues[idx2];

                if (val0 > val1) {
                    // 交换0和1
                    indices[0] = idx1;
                    indices[1] = idx0;
                    int tempVal = val0;
                    val0 = val1;
                    val1 = tempVal;
                    int tempIdx = idx0;
                    idx0 = idx1;
                    idx1 = tempIdx;
                }
                if (val1 > val2) {
                    // 交换1和2
                    indices[1] = idx2;
                    indices[2] = idx1;
                    if (val0 > val2) {
                        // 交换0和1
                        indices[0] = indices[1];
                        indices[1] = idx0;
                    }
                }
                return;
            default:
                // General case: insertion sort for larger arrays
                for (int i = 1; i < arrayCnt; i++) {
                    int value = firstValues[indices[i]];
                    int valueIndex = indices[i];
                    int j = i - 1;

                    // Shift elements greater than current value
                    while (j >= 0 && firstValues[indices[j]] > value) {
                        indices[j + 1] = indices[j];
                        j--;
                    }
                    indices[j + 1] = valueIndex;
                }
        }
    }

    public void execute() {
        // 提前检查空结果
        if (emptyResult)
            // return new int[0];
            return;

        // Single iterator optimization - avoid unnecessary complexity - 单表情况
        if (arrayCnt == 1) {
            int[] bound = arrayBounds[0];
            // int size = bound[1] - bound[0];

            // Mark all values as processed
            if (arrayStatus[0] != null) {
                int[] status = arrayStatus[0];
                Arrays.fill(status, bound[0], bound[1], 1); // 使用Arrays.fill优化
            }

            // Direct copy for better performance
            // int[] result = new int[size];
            // System.arraycopy(arrays[0], bound[0], result, 0, size);
            // return result;
            return;
        }

        // 初始化位置
        for (int i = 0; i < arrayCnt; i++) {
            currentPositions[i] = arrayBounds[i][0];
        }

        // Smart initial sizing based on smallest array
        int minSize = arrayLengths[0];
        for (int i = 1; i < arrayCnt; i++) {
            minSize = Math.min(minSize, arrayLengths[i]);
        }
        // int initialSize = Math.min(INITIAL_RESULT_SIZE, minSize);
        // int[] resultBuffer = new int[initialSize];
        // int resultSize = 0;

        // Cache frequently accessed values for performance
        int listIndex = 0;
        int maxIndex = arrayCnt - 1;

        // 缓存常用数组引用
        final int[][] localArrays = arrays;
        final int[][] localBounds = arrayBounds;
        final int[][] localStatus = arrayStatus;
        final int[] localPositions = currentPositions;

        int max = localArrays[maxIndex][localPositions[maxIndex]];

        mainLoop: while (true) {
            // Get current value with minimal array access
            final int[] currentArray = localArrays[listIndex];
            final int currentPosition = localPositions[listIndex];
            final int value = currentArray[currentPosition];

            if (value == max) {
                // // Expand result buffer when needed (使用更大的增长因子)
                // if (resultSize == resultBuffer.length) {
                // int newSize = resultBuffer.length << 1; // 位运算替代乘法
                // resultBuffer = Arrays.copyOf(resultBuffer, newSize);
                // }
                // resultBuffer[resultSize++] = value;

                // Mark matching values across all iterators - 优化循环
                for (int i = 0; i < arrayCnt; i++) {
                    final int pos = localPositions[i];
                    final int[] status = localStatus[i];
                    if (status != null)
                        status[pos] = 1;
                }

                // Advance to next potential match
                seek(listIndex, max + 1);
            } else {
                // Seek to current maximum value
                seek(listIndex, max);
            }

            // Check for end condition  越界检查
            if (localPositions[listIndex] >= localBounds[listIndex][1]) {
                break mainLoop;
            }

            // Update maximum value and advance iterator index
            max = localArrays[listIndex][localPositions[listIndex]];
            listIndex = listIndex + 1;
            if (listIndex >= arrayCnt) { // 避免取模运算
                listIndex = 0;
            }
        }

        // Return appropriately sized result array
        // return resultSize < resultBuffer.length ? Arrays.copyOf(resultBuffer,
        // resultSize) : resultBuffer;
    }

    /**
     * 进一步优化的seek操作
     */
    private void seek(int listIndex, int value) {
        final int[] data = arrays[listIndex];
        int pos = currentPositions[listIndex];
        final int end = arrayBounds[listIndex][1];

        // Fast path: already at or beyond target value
        if (pos >= end)
            return;

        int currentValue = data[pos];
        if (currentValue >= value)
            return;

        // 对于小范围使用线性搜索，阈值调整为8
        int range = end - pos;
        if (range <= 8) {
            do {
                pos++;
                if (pos >= end)
                    break;
                currentValue = data[pos];
            } while (currentValue < value);
            currentPositions[listIndex] = pos;
            return;
        }

        // Binary search for larger ranges - 优化版本
        int left = pos + 1;
        int right = end - 1;

        while (left <= right) {
            int middle = (left + right) >>> 1; // Unsigned shift for performance
            int middleValue = data[middle];

            if (middleValue < value) {
                left = middle + 1;
            } else {
                right = middle - 1;
            }
        }

        currentPositions[listIndex] = left;
    }
}