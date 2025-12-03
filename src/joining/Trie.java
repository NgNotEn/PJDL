package joining;

import java.util.*;

import data.ColumnData;
import data.DoubleData;
import data.IntData;
import util.Pair;
import util.IntArray;
import joining.result.ResultTrie;
public class Trie {

    // public static Map<List<ColumnRef>, int[]> OrderCache;
    public ResultTrie rt;
    public final int aliasID;               // 表别名ID
    public final String table;              // 表名
    public final int cardinality;           // 获取表的基数（行数）

    // 层状态管理
    public final int maxLevel;  // 参与连接的列数：也即最大层数;  但有效层是从0开始的（层下标），也就是说如果curLevel == maxLevel，说明已经不是最后一层了
    public int curLevel = -1;  // 当前层, 根节点在-1层
    private int[] curValues;    // 当前列的原始数据（转换为int用于比较）
    public int[] tupleIdx;
    public IntArray curLevelValues; 
    public IntArray curLevelValueParentIdx; 
    public IntArray curLevelValueStatus;
    public IntArray curLevelArrayBounds;
    public IntArray preLevelValueBounds;
    public IntArray curLevelValueBounds;

    // 奇偶轮用以实现切换层范围,避免拷贝消耗，被pre/curLevelValueBounds引用
    public IntArray valueBounds1;           
    public IntArray valueBounds2;
    private int curCol = 0;
    private List<int[]> trieCols;

    // public Trie(String alias, int aliasID, QueryInfo query, Context context, List<Set<ColumnRef>> globalVarOrder) throws Exception {
    public Trie(BaseTrie bt, Pair<Integer, Integer> domain) throws Exception {

        this.aliasID = bt.aliasID;
        curLevel = -1;  // 根节点
        table = bt.table;
        cardinality = domain.getSecond() - domain.getFirst();
        trieCols = bt.trieCols;
        maxLevel = trieCols.size();
        

        // 初始化层级管理器
        curLevelValues = new IntArray(cardinality / 2, cardinality);
        curLevelValueParentIdx = new IntArray(cardinality / 2, cardinality);
        curLevelValueStatus = new IntArray(cardinality / 2, cardinality);

        curLevelArrayBounds = new IntArray(cardinality / 2, cardinality * 2);
        valueBounds1 = new IntArray(cardinality, cardinality * 2);
        valueBounds2 = new IntArray(cardinality, cardinality * 2);

        curLevelValues.add(-1);             // 添加虚拟根节点值 -1
        curLevelValueStatus.add(1);   // 将虚拟根节点值设为有效

        valueBounds1.add(0);
        valueBounds1.add(cardinality);

        final int[] tupleOrder = bt.tupleOrder;
        tupleIdx = Arrays.copyOfRange(tupleOrder, domain.getFirst(), domain.getSecond());
        // tupleIdx = new int[cardinality];
        // for (int i = 0; i < cardinality; i++) {
        //     tupleIdx[i] = tupleOrder[domain.getFirst() + i];
        // }
    }

    int[] boundsOfArray(int arrayIdx) {
        int baseIdx = arrayIdx << 1; // Use bit shift
        return new int[] {
                curLevelArrayBounds.data[baseIdx],
                curLevelArrayBounds.data[baseIdx + 1]
        };
    }
    // 处理最后一层（连接列处理完毕），准备结果输出。
    public void lastLevel() {
        curLevel++;
        final boolean isEvenLevel = (curLevel & 1) == 0;
        preLevelValueBounds = isEvenLevel ? valueBounds1 : valueBounds2;
        curLevelValueBounds = isEvenLevel ? valueBounds2 : valueBounds1;

        final int[] statusData = curLevelValueStatus.data;
        final int[] boundsData = preLevelValueBounds.data;
        final int statusSize = curLevelValueStatus.size;

        for (int i = 0; i < statusSize; i++) {
            if (statusData[i] != 1)
                continue;
            
            // 查看有效值的值块范围
            final int baseIdx = i << 1;
            curLevelValueBounds.add(boundsData[baseIdx]);
            curLevelValueBounds.add(boundsData[baseIdx + 1]); 
        }

        // System.out.println("Trie " + aliasID + " last level with " + (curLevelValueBounds.size / 2) + " value blocks.");
        curLevel--;
    }
    // 进入下一层（初始化值）。根据是否在缓存（OrderCache）中有排序结果，选择调用processWithSort或processWithoutSort。
    public void nextLevel() {
        curLevel++;
        if (curLevel == maxLevel)
            return;
        // 转换为 int[] 用于比较
        curValues = trieCols.get(curCol++);

        final boolean isEvenLevel = (curLevel & 1) == 0;    // even level?
        preLevelValueBounds = isEvenLevel ? valueBounds1 : valueBounds2;    // 上一层的约束行
        curLevelValueBounds = isEvenLevel ? valueBounds2 : valueBounds1;    // 预处理的该层约束行


         // 重置状态数组
        curLevelValueParentIdx.size = 0;
        curLevelValues.size = 0;
        curLevelArrayBounds.size = 0;

        processWithSort();

        curLevelValueStatus.rangeSetValue(0, curLevelValues.size, 0);   // 重置值状态为0（初始状态），经过连接后再标红
        preLevelValueBounds.size = 0;

    }

    private void processWithoutSort() {

        int[] boundsData = preLevelValueBounds.data;
        final int[] values = curValues;
        final int[] indices = tupleIdx;

        // 根结点处理
        if (curLevel == 0) {
            final int arrayLB = boundsData[0];
            final int arrayUB = boundsData[1];

            curLevelArrayBounds.add(curLevelValues.size);

            int currentStart = arrayLB;
            int currentValue = values[indices[currentStart]];
            for (int j = arrayLB + 1; j < arrayUB; j++) {
                int value = values[indices[j]];
                if (value != currentValue) {
                    curLevelValues.add(currentValue);
                    curLevelValueBounds.add(currentStart);
                    curLevelValueBounds.add(j);
                    currentStart = j;
                    currentValue = value;
                }
            }
            curLevelValues.add(currentValue);
            curLevelValueBounds.add(currentStart);
            curLevelValueBounds.add(arrayUB);

            curLevelArrayBounds.add(curLevelValues.size);

            curLevelValueParentIdx.rangeSetValue(0, curLevelValues.size, 0);
        } else {
            IntArray preLevelValueStatus = curLevelValueStatus;
            int[] statusData = preLevelValueStatus.data;
            int size = preLevelValueStatus.size;
            BitSet status = rt.status; // 获取当前层的状态位图

            int fatherIdx = 0;
            for (int i = 0; i < size; i++) {
                if (statusData[i] != 1)
                    continue;

                curLevelArrayBounds.add(curLevelValues.size);

                if(status.get(fatherIdx)) {
                    final int baseIdx = i << 1; // Use bit shift
                    final int arrayLB = boundsData[baseIdx];
                    final int arrayUB = boundsData[baseIdx + 1];

                    int currentStart = arrayLB;
                    int currentValue = values[indices[currentStart]];
                    for (int j = arrayLB + 1; j < arrayUB; j++) {
                        int value = values[indices[j]];
                        if (value != currentValue) {
                            curLevelValues.add(currentValue);
                            curLevelValueParentIdx.add(fatherIdx);
                            curLevelValueBounds.add(currentStart);
                            curLevelValueBounds.add(j);
                            currentStart = j;
                            currentValue = value;
                        }
                    }
                    curLevelValues.add(currentValue);
                    curLevelValueParentIdx.add(fatherIdx);
                    curLevelValueBounds.add(currentStart);
                    curLevelValueBounds.add(arrayUB);
                }

                curLevelArrayBounds.add(curLevelValues.size);

                fatherIdx++;
            }
        }
    }

    private void processWithSort() {

        final BitSet status = rt.status; // 获取当前层的状态位图

        // 获取上一层的状态数据和边界数据
        IntArray preLevelValueStatus = curLevelValueStatus;     // 获取上一层的有效标记，路径标记
        int[] statusData = preLevelValueStatus.data;            // 引用
        int[] boundsData = preLevelValueBounds.data;            // 上一层有效值块的索引范围，此处在进入nextLevel时刻发生了切换
        int size = preLevelValueStatus.size;


        // 以下操作可以看作父层剪枝
        int fatherIdx = 0;
        for (int i = 0; i < size; i++) {
            // 未标红的路径不再记录（剪枝）
            if (statusData[i] != 1)
                continue;

            // 记录标红的路径
            curLevelArrayBounds.add(curLevelValues.size);
            if(status.get(fatherIdx)) {
                final int baseIdx = i << 1; // Use bit shift
                final int arrayLB = boundsData[baseIdx];
                final int arrayUB = boundsData[baseIdx + 1];
                processBoundInternal(fatherIdx, arrayLB, arrayUB);  // 组内排序
            }
            curLevelArrayBounds.add(curLevelValues.size);
            fatherIdx++;
        }
    }


    // 将每条有效路径下的该层结点进行排序
    private void processBoundInternal(int fatherIdx, int arrayLB, int arrayUB) {
        final int[] values = curValues;
        final int[] indices = tupleIdx;
        // final int length = arrayUB - arrayLB;

        // if (length <= 1) {
        //     // 处理单个元素的情况
        //     curLevelValues.add(values[indices[arrayLB]]);
        //     curLevelValueParentIdx.add(fatherIdx);
        //     curLevelValueBounds.add(arrayLB);
        //     curLevelValueBounds.add(arrayUB);
        //     return;
        // }

        // // 检查是否有序
        // boolean isSorted = true;
        // int prevValue = values[indices[arrayLB]];
        // for (int j = arrayLB + 1; j < arrayUB; j++) {
        //     int value = values[indices[j]];
        //     if (value < prevValue) {
        //         isSorted = false;
        //         break;
        //     }
        //     prevValue = value;
        // }

        // if (!isSorted) {
        //     // 优化排序逻辑
        //     if(length <= 32) {
        //         // 插入排序
        //         insertionSort(arrayLB, arrayUB, tupleIdx, values);
        //     }
        //     // if (length < 1 << 16) {
                
        //     //     QuickSort.sort(arrayLB, arrayUB - 1, tupleIdx, values);
        //     // } 
        //     else {
        //         int[] sorted = Arrays.stream(indices, arrayLB, arrayUB)
        //             .parallel()
        //             .boxed()
        //             .sorted(Comparator.comparingInt(idx -> values[idx]))
        //             .mapToInt(Integer::intValue)
        //             .toArray();

        //         System.arraycopy(sorted, 0, tupleIdx, arrayLB, sorted.length);
        //     }
        // }

        // 提取唯一值
        int currentStart = arrayLB;
        int currentValue = values[indices[currentStart]];
        for (int j = arrayLB + 1; j < arrayUB; j++) {
            int value = values[indices[j]];
            if (value != currentValue) {
                curLevelValues.add(currentValue);
                curLevelValueParentIdx.add(fatherIdx);
                curLevelValueBounds.add(currentStart);
                curLevelValueBounds.add(j);
                currentStart = j;
                currentValue = value;
            }
        }
        // 添加最后一个值
        curLevelValues.add(currentValue);
        curLevelValueParentIdx.add(fatherIdx);
        curLevelValueBounds.add(currentStart);
        curLevelValueBounds.add(arrayUB);
    }

    private void insertionSort(int start, int end, int[] indices, int[] values) {
        // 实际排序范围：[start, end-1]
        for (int i = start + 1; i < end; i++) {
            int currentIndex = indices[i];
            int currentValue = values[currentIndex];
            int j = i - 1;

            // 将大于当前元素的元素向后移动
            while (j >= start && values[indices[j]] > currentValue) {
                indices[j + 1] = indices[j];
                j--;
            }

            // 插入当前元素到正确位置
            indices[j + 1] = currentIndex;
        }
    }
}