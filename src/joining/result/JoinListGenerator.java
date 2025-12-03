package joining.result;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.stream.Collectors;

import joining.Trie;
import util.CartesianProduct;


public class JoinListGenerator {

    private int varCount;
    public ResultTrie[] resultTries;             // 连接结果数组
    public ResultTrie[][] trieByVar;             // 关联variable
    public Map<Integer, Integer>[] varID2Level;  // variable id -> level
    private boolean[] trieAdded;                 // 表是否已添加到当前连接层级
    private CartesianProduct curCPGenerator;     // 笛卡尔积生成器

    @SuppressWarnings("unchecked")
    public JoinListGenerator(Trie[] tries, int totalVarCount) {
        // 初始化 ResultTrie 和 Trie 数组
        resultTries = new ResultTrie[tries.length];

        for (Trie trie : tries) {
            resultTries[trie.aliasID] = new ResultTrie(trie.aliasID, trie.maxLevel);
            trie.rt = resultTries[trie.aliasID]; // 关联 Trie 和 ResultTrie
        }
            
        
        // 初始化状态记录
        varID2Level = new HashMap[tries.length];
        for (int i = 0; i < varID2Level.length; i++)
            varID2Level[i] = new HashMap<>();

        trieAdded = new boolean[tries.length]; 
        trieByVar = new ResultTrie[totalVarCount][];     // 按连接层级组织的表数组
        varCount = 0;
    }


    // 推进一层 join（即加入一个等价类），为本层涉及的每个表的 ResultTrie 添加新层
    public void addVariable(Trie[] curTries) {
        // 初始化当前等价类的表数组
        trieByVar[varCount] = new ResultTrie[curTries.length];  
        // 遍历本层涉及的所有 Trie（表），为每个表的 ResultTrie 添加新层（addLayer）。
        for (int trieIdx = 0; trieIdx < curTries.length; trieIdx++) {
            Trie trie = curTries[trieIdx];
            int aliasID = trie.aliasID;
            ResultTrie resultTrie = resultTries[aliasID];
            trieByVar[varCount][trieIdx] = resultTrie;
            trieAdded[aliasID] = true;

            int maxSize = trie.curLevelValueStatus.size;
            int[] statusData = trie.curLevelValueStatus.data;
            int[] valuesData = trie.curLevelValues.data;
            int[] parentIdxData = trie.curLevelValueParentIdx.data;

            // 预计算有效元素数量
            // 只把有效的 join key（status=1）加入新层
            int validCount = 0;
            for (int i = 0; i < maxSize; i++) {
                validCount += statusData[i]; // 利用1和0的特性
            }

            if (validCount == 0) {
                resultTrie.addLayer(new int[0], new int[0], 0);    // 添加新层级到ResultTrie
                varID2Level[resultTrie.aliasID].put(varCount, resultTrie.curMaxLevel);
                continue;
            }

            int[] filteredValues = new int[validCount];
            int[] filteredValueParentIdx = new int[validCount];

            // 使用 HashSet 预分配大小
            HashSet<Integer> needToStore = new HashSet<>(validCount);
            int[] curLevel = resultTrie.levels[resultTrie.curMaxLevel];

            // long start = System.nanoTime();
            // 记录父节点索引，便于后续剪枝和结果回溯
            int idx = 0;
            for (int i = 0; i < maxSize; i++) {
                if (statusData[i] == 1) {
                    filteredValues[idx] = valuesData[i];
                    // filteredValues[idx] = trie.visit(i);
                    int parentIdx = parentIdxData[i];
                    filteredValueParentIdx[idx] = parentIdx;

                    if (parentIdx < curLevel.length) {
                        int parentValue = curLevel[parentIdx];
                        if (parentValue != -1 && parentValue != Integer.MAX_VALUE)
                            needToStore.add(parentValue);
                    }
                    idx++;
                }
            }
            // System.out.println("filter cost: " + (System.nanoTime() - start) /
            // 1000000);

            // System.out.println(Arrays.toString(filteredValues));
            resultTrie.addLayer(filteredValues, filteredValueParentIdx, validCount);
            varID2Level[resultTrie.aliasID].put(varCount, resultTrie.curMaxLevel);
            
            int needToPrune = findPruningVar(trie.aliasID);
            if (needToPrune != -1) {
                for (ResultTrie rt : trieByVar[needToPrune])
                    rt.pruneUpward(needToStore, varID2Level[rt.aliasID].get(needToPrune));
            }
        }

        // printDebugInfo();

        varCount++;
    }

    // 将调试打印提取为单独方法
    private void printDebugInfo() {
        for (ResultTrie resultTrie : resultTries) {
            System.out.println("-----------------------------------------------------");
            System.out.println("ResultTrie aliasID: " + resultTrie.aliasID);
            for (int level = 0; level <= resultTrie.curMaxLevel; level++) {
                System.out.println("Level  " + level + ": " + Arrays.toString(resultTrie.levels[level]));
                System.out.println("Ranges " + level + ": " + Arrays.toString(resultTrie.ranges[level]));
            }
            for (int level = 0; level <= resultTrie.curMaxLevel; level++) {
                System.out.println("LevelRange  " + level + ": " + Arrays.toString(resultTrie.levelRanges[level]));
            }
        }
    }

    private int findPruningVar(int aliasID) {
        for (int varID = varCount - 1; varID >= 0; varID--) {
            ResultTrie[] tries = trieByVar[varID];
            for (int i = 0; i < tries.length; i++)
                if (tries[i].aliasID == aliasID)
                    return varID;
        }
        return -1;
    }

    public List<int[]> genResult() {
        ResultLeapFrogTrieJoin rlfrj = new ResultLeapFrogTrieJoin(trieByVar, resultTries);
        return rlfrj.execute();
    }




    // 根据当前所有等价类的全局分组（aliasGrouping），为每个分组生成 join 任务列表
    public void genJoinList(List<TreeSet<Integer>> aliasGrouping) {
        final int groupCount = aliasGrouping.size();        // 提取组数
        List<List<int[]>> joinListInGroups = new ArrayList<>(groupCount);

        // for (TreeSet<Integer> group : aliasGrouping) {
        //     List<int[]> joinListInGroup = processGroup(group);// 处理分组, 组内连接
        //     joinListInGroups.add(joinListInGroup);
        // }
        joinListInGroups = aliasGrouping.parallelStream()
                                .map(this::processGroup)
                                .collect(Collectors.toList());

        curCPGenerator = new CartesianProduct(joinListInGroups);// 生成当前的变量的笛卡尔积
    }

    public int[][] nextGroupOfJoinList() {
        if (curCPGenerator.hasMoreResults())
            return curCPGenerator.nextResult();
        return new int[0][];
    }

    // 提取组处理逻辑为单独方法 // 为一个分组生成 join 任务。
    private List<int[]> processGroup(TreeSet<Integer> group) {
        List<int[]> joinListInGroup = new ArrayList<>();        // list[i] means  in current level all tries' valid data's row index, not after join

        // 检查组内所有 Trie 是否都已添加到当前 join 层级
        boolean allAdded = true;
        for (int id : group) {
            if (!trieAdded[id]) {
                allAdded = false;
                break;
            }
        }
        // 如果有未添加的，直接返回空任务（占位）
        if (!allAdded) {
            int[] list = new int[group.size()];
            joinListInGroup.add(list);
            return joinListInGroup;
        }

        // 组内单个trie的情况   枚举该 Trie 的所有结果
        if (group.size() == 1) {
            return processSingleTrie(group.first());
        }

        // 多个trie的情况
        return processMultipleTries(group);
    }

    private List<int[]> processSingleTrie(int trieId) {
        List<int[]> joinListInGroup = new ArrayList<>();
        ResultTrie trie = resultTries[trieId];
        BitSet status = trie.status;
        int[] level = trie.levels[trie.curMaxLevel];    // 引用ResultTrie上一次（本次连接未完成）的连接层

        // 预分配空间
        joinListInGroup = new ArrayList<>(level.length);

        for (int i = 0; i < level.length; i++) {
            if (level[i] != Integer.MAX_VALUE) {
                joinListInGroup.add(new int[] { i });   // 将所有父节点加入（实际上，是路径）
                status.set(i);
            }
                
        }

        return joinListInGroup;
    }

    private List<int[]> processMultipleTries(TreeSet<Integer> group) {
        // 记录组内的ResultTrie数组
        ResultTrie[] allTrie = new ResultTrie[group.size()];
        int offset = 0;
        for (int i = 0; i < resultTries.length; i++) {
            ResultTrie rtrie = resultTries[i];
            if (group.contains(rtrie.aliasID)) {
                allTrie[offset] = rtrie;
                offset++; 
            }
        }

        // 构建组内变量的trie数组
        ResultTrie[][] groupTrieByVar = buildGroupTrieByVar(group);
        // 基于我们已经连接好的这些条件，在本组内部的这些表之间，有哪些行组合是满足所有历史连接条件的？返回 curVID-1 层的索引，也即预连接层的值块范围
        ResultLeapFrogTrieJoin rlfrj = new ResultLeapFrogTrieJoin(groupTrieByVar, allTrie);
        return rlfrj.execute(); // 执行连接
    }

    private ResultTrie[][] buildGroupTrieByVar(TreeSet<Integer> group) {
        // 先计算需要的数组大小
        int varGroupCount = 0;
        for (int varId = 0; varId < varCount; varId++) {
            boolean hasTrieInGroup = false;
            for (ResultTrie trie : trieByVar[varId]) {
                if (group.contains(trie.aliasID)) {
                    hasTrieInGroup = true;
                    break;
                }
            }
            if (hasTrieInGroup)
                varGroupCount++;
        }

        ResultTrie[][] groupTrieByVar = new ResultTrie[varGroupCount][];
        int varGroupIdx = 0;

        for (int varId = 0; varId < varCount; varId++) {
            // 计算该变量在组内的trie数量
            int trieCount = 0;
            for (ResultTrie trie : trieByVar[varId]) {
                if (group.contains(trie.aliasID))
                    trieCount++;
            }

            if (trieCount > 0) {
                ResultTrie[] trieOfVar = new ResultTrie[trieCount];
                int trieIdx = 0;
                for (ResultTrie trie : trieByVar[varId]) {
                    if (group.contains(trie.aliasID))
                        trieOfVar[trieIdx++] = trie;
                }
                groupTrieByVar[varGroupIdx++] = trieOfVar;  // for per var , compute how many tries in group that hava it
                /*
                 *  var_1       var_2       ...
                 *  trie_1      trie_x      ...
                 *  Trie_2      trie_y      ...
                 *   ...          ...       ...
                */
            }
        }

        return groupTrieByVar;
    }
}