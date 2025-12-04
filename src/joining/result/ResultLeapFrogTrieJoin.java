package joining.result;

import joining.Trie;

import java.util.*;

public class ResultLeapFrogTrieJoin {

    // 每个变量对应的 Trie 数组
    private final ResultTrie[][] trieByVar;

    // 当前变量ID
    private int curVariableID = 0;

    // 变量总数
    private final int nrVars;

    // 所有 Trie 的数组引用
    private final ResultTrie[] allTries;

    // 存储所有连接结果 - 预分配大小以减少扩展操作
    private final ArrayList<int[]> joinResults;

    public int[] printResult;

    // 用于存储连接信息的帧数组 - 直接使用数组而不是ArrayList
    private final JoinFrame[] joinFrames;

    // 是否在上一轮迭代中进行了回溯
    private boolean backtracked = false;

    // 结果缓冲池 - 避免频繁创建新数组
    private final int[][] resultPool;
    private int resultPoolIndex = 0;
    private final int RESULT_POOL_SIZE = 2048; // 增大池大小

    // 缓存常用变量减少数组访问
    private final int allTriesLength;

    // 预分配临时数组，避免重复创建
    private final ResultTrie[] tempTrieArray;
    private final int[] tempKeyArray;

    public ResultLeapFrogTrieJoin(ResultTrie[][] trieByVar, ResultTrie[] resultTries) {
        this.trieByVar = trieByVar;
        this.nrVars = trieByVar.length;
        this.allTries = resultTries;
        this.allTriesLength = resultTries.length;

        // 预分配结果缓冲区，避免动态扩容
        this.joinResults = new ArrayList<>(16384); // 增大初始容量

        // 初始化对象池
        this.resultPool = new int[RESULT_POOL_SIZE][allTriesLength];

        // 预分配临时数组
        int maxTrieCount = 0;
        for (int i = 0; i < nrVars; i++)
            maxTrieCount = Math.max(maxTrieCount, trieByVar[i].length);
        this.tempTrieArray = new ResultTrie[maxTrieCount];
        this.tempKeyArray = new int[maxTrieCount];

        // 初始化 joinFrames 数组
        this.joinFrames = new JoinFrame[nrVars];
        for (int i = 0; i < nrVars; i++)
            this.joinFrames[i] = new JoinFrame();

        printResult = new int[nrVars];
    }

    public List<int[]> execute() {
        // 清空之前的结果
        joinResults.clear();
        resultPoolIndex = 0;

        // 重置所有 Trie - 展开循环减少数组边界检查
        ResultTrie[] tries = allTries;
        int triesLen = allTriesLength;
        for (int i = 0; i < triesLen; i++)
            tries[i].reset();

        curVariableID = 0;
        backtracked = false;

        // 主循环
        while (curVariableID >= 0) {
            // 如果已经处理完所有变量，收集结果并回溯
            if (curVariableID >= nrVars) {  // 处理到 nrVars-1
                collectResult();
                curVariableID--; // 内联 backtrack()
                backtracked = true;
                continue;
            }

            JoinFrame joinFrame = joinFrames[curVariableID];    // 当前层的各个迭代器状态

            if (backtracked) {
                if (!handleBacktrack(joinFrame))
                    continue;
            } else {
                if (!setupNextVariable(joinFrame))
                    continue;
            }

            // 执行 LeapFrog Join
            leapfrogJoin(joinFrame);
        }

        return joinResults; // 每棵树的连接结果下标  { [], [], [] , ...}
    }

    private void collectResult() {

        // System.out.println(Arrays.toString(printResult));

        // 复制结果到临时缓冲区
        ResultTrie[] tries = allTries;

        int[] result;

        // 使用对象池
        if (resultPoolIndex < RESULT_POOL_SIZE)
            result = resultPool[resultPoolIndex++];
        else
            result = new int[allTriesLength];

        for (int i = 0; i < allTriesLength; i++) {
            int idx = tries[i].getLastLevelIndex();
            // otries[i].validIdx.add(idx);  // 标记有效
            result[i] = idx;
        }

        joinResults.add(result);    
    }

    private boolean handleBacktrack(JoinFrame joinFrame) {
        backtracked = false;

        ResultTrie[] curTries = joinFrame.curTries;
        ResultTrie minTrie = curTries[joinFrame.minTriePos];
        minTrie.next();

        if (minTrie.atEnd()) {
            int nrCurTries = joinFrame.nrCurTries;
            for (int i = 0; i < nrCurTries; i++)
                curTries[i].up();

            curVariableID--; // 内联 backtrack()
            backtracked = true;
            return false;
        }

        // 优化取模运算
        int nextPos = joinFrame.minTriePos + 1;
        joinFrame.minTriePos = nextPos >= joinFrame.nrCurTries ? 0 : nextPos;
        return true;
    }

    private boolean setupNextVariable(JoinFrame joinFrame) {
        ResultTrie[] curTries = trieByVar[curVariableID];
        joinFrame.curTries = curTries;
        int nrCurTries = curTries.length;
        joinFrame.nrCurTries = nrCurTries;

        // 打开所有相关的 Trie
        for (int i = 0; i < nrCurTries; i++)
            curTries[i].open();

        // 检查是否有任何 Trie 已经到达末尾
        for (int i = 0; i < nrCurTries; i++) {
            if (curTries[i].atEnd()) {
                for (int j = 0; j < nrCurTries; j++)
                    curTries[j].up();

                curVariableID--; // 内联 backtrack()
                backtracked = true;
                return false;
            }
        }

        // 使用更高效的排序算法
        if (nrCurTries <= 1) {
            // 单个元素无需排序
        } else if (nrCurTries == 2) {
            // 两个元素直接比较交换
            if (curTries[0].key() > curTries[1].key()) {
                ResultTrie temp = curTries[0];
                curTries[0] = curTries[1];
                curTries[1] = temp;
            }
        } else if (nrCurTries <= 8) {
            // 小数组使用插入排序
            insertionSort(curTries, nrCurTries);
        } else {
            // 大数组使用优化的快速排序
            optimizedSort(curTries, nrCurTries);
        }

        joinFrame.minTriePos = 0;
        return true;
    }

    // 针对小数组优化的插入排序
    private void insertionSort(ResultTrie[] tries, int length) {
        for (int i = 1; i < length; i++) {
            ResultTrie key = tries[i];
            int keyValue = key.key();
            int j = i - 1;

            while (j >= 0 && tries[j].key() > keyValue) {
                tries[j + 1] = tries[j];
                j--;
            }
            tries[j + 1] = key;
        }
    }

    // 优化的排序算法 - 减少key()调用次数
    private void optimizedSort(ResultTrie[] tries, int length) {
        // 缓存key值避免重复计算
        for (int i = 0; i < length; i++) {
            tempTrieArray[i] = tries[i];
            tempKeyArray[i] = tries[i].key();
        }

        // 使用key数组进行排序
        quickSortWithKeys(tempTrieArray, tempKeyArray, 0, length - 1);

        // 复制回原数组
        System.arraycopy(tempTrieArray, 0, tries, 0, length);
    }

    private void quickSortWithKeys(ResultTrie[] tries, int[] keys, int low, int high) {
        if (low < high) {
            int pi = partitionWithKeys(tries, keys, low, high);
            quickSortWithKeys(tries, keys, low, pi - 1);
            quickSortWithKeys(tries, keys, pi + 1, high);
        }
    }

    private int partitionWithKeys(ResultTrie[] tries, int[] keys, int low, int high) {
        int pivot = keys[high];
        int i = low - 1;

        for (int j = low; j < high; j++) {
            if (keys[j] <= pivot) {
                i++;
                // 交换tries和keys
                ResultTrie tempTrie = tries[i];
                tries[i] = tries[j];
                tries[j] = tempTrie;

                int tempKey = keys[i];
                keys[i] = keys[j];
                keys[j] = tempKey;
            }
        }

        // 交换pivot
        ResultTrie tempTrie = tries[i + 1];
        tries[i + 1] = tries[high];
        tries[high] = tempTrie;

        int tempKey = keys[i + 1];
        keys[i + 1] = keys[high];
        keys[high] = tempKey;

        return i + 1;
    }

    private void leapfrogJoin(JoinFrame joinFrame) {
        ResultTrie[] curTries = joinFrame.curTries;
        int nrCurTries = joinFrame.nrCurTries;

        while (true) {
            // 优化maxTriePos计算
            int maxTriePos = joinFrame.minTriePos == 0 ? nrCurTries - 1 : joinFrame.minTriePos - 1;
            ResultTrie maxTrie = curTries[maxTriePos];
            int maxKey = maxTrie.key();

            ResultTrie minTrie = curTries[joinFrame.minTriePos];
            int minKey = minTrie.key();

            if (minKey == maxKey) {
                printResult[curVariableID] = minKey;
                curVariableID++; // 内联 advance()
                break;
            } else {
                minTrie.seek(maxKey);

                if (minTrie.atEnd()) {
                    for (int i = 0; i < nrCurTries; i++)
                        curTries[i].up();

                    curVariableID--; // 内联 backtrack()
                    backtracked = true;
                    break;
                } else {
                    // 优化取模运算
                    int nextPos = joinFrame.minTriePos + 1;
                    joinFrame.minTriePos = nextPos >= nrCurTries ? 0 : nextPos;
                }
            }
        }
    }

    private static final class JoinFrame {
        ResultTrie[] curTries = null;
        int nrCurTries = -1;
        int minTriePos = -1;
    }
}