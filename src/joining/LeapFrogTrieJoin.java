package joining;

import data.IntData;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import joining.result.JoinListGenerator;
import query.ColumnRef;

public class LeapFrogTrieJoin {

    private final int totalVarCount; // 变量总数
    private Trie[] tries;   // 所有表的Trie数组
    
    // 方便通过 var 找 tries
    private Trie[][] trieByVar;
    private Map<Integer, String> aliasID2Col;   // 列名映射​​。用于在执行时，根据表的ID查找当前需要处理的列名。
    private HashMap<String, Integer> alias2ID;  // 别名映射​​。将表别名（String）映射到一个唯一的整数ID，方便内部通过ID快速访问。
    private boolean[][] trieMatrix; // 连通性矩阵​​。一个二维布尔矩阵，trieMatrix[i][j]为 true表示第 i个变量（等价类）涉及别名ID为 j的表。用于跟踪和计算连通性。
    private int varCount;   // 前已添加的变量数

    private JoinListGenerator generator; // 连接列表生成器（JoinListGenerator），用于生成连接任务。



    AggregateData[] aggregateDatas;
    Map<Integer, List<Integer>> aggregateInfo;



    // 优化后的构造函数,初始化所有Trie（并行创建）
    public LeapFrogTrieJoin(Trie[] tries) throws Exception {
        this.aggregateDatas = JoinProcessor.aggregateDatas;
        this.aggregateInfo = JoinProcessor.aggregateInfo;
        
        // 获取变量总数
        totalVarCount = TrieManager.totalVarCount;
        // 创建别名到ID（索引）的映射
        final int aliasCnt = TrieManager.aliasCnt;
        alias2ID = TrieManager.alias2ID;
        // 预先分配所有数据结构
        varCount = 0;   // ==> varID
        trieMatrix = new boolean[totalVarCount][aliasCnt]; //  triebyvar的标记
        trieByVar = new Trie[totalVarCount][]; // triebyvar


        // 初始化所有Tries
        this.tries = tries;
        generator = new JoinListGenerator(tries, totalVarCount);        // 初始化结果生成器（直接使用 Trie 合并结构）
    }

    // 计算当前所有已加入等价类后，哪些表已经通过等价类连通，哪些还没连通，并分组
    public List<TreeSet<Integer>> addVariable(Set<ColumnRef> nextVar) {
        if (varCount >= totalVarCount) {
            throw new IllegalStateException("Cannot add more variables than totalVarCount: " + totalVarCount);
        }

        ColumnRef[] columns = nextVar.toArray(new ColumnRef[0]);
        Arrays.sort(columns, Comparator.comparingInt(col -> alias2ID.get(col.aliasName)));  // sort by aliasID

        aliasID2Col = new HashMap<>();

        // 获得关联表
        Trie[] curTries = new Trie[columns.length];
        for (int i = 0; i < columns.length; i++) {
            int id = alias2ID.get(columns[i].aliasName);
            aliasID2Col.put(id, columns[i].columnName);
            curTries[i] = tries[id];
            trieMatrix[varCount][id] = true;    // 标记这些表被访问了
        }


        // 存储关联表
        trieByVar[varCount] = curTries;

        // 更新varID    (next var)
        varCount++;

        List<TreeSet<Integer>> aliasGrouping = new ArrayList<>();        // Groups
        updateLevelAliasGrouping(aliasGrouping);
        return aliasGrouping;

    }


    // 并查集 将​​通过会被当前变量连通起来的表​​分组
    private static class UnionFind {
        private final int[] parent;
        private final int[] rank;

        public UnionFind(int size) {
            parent = new int[size];
            rank = new int[size];
            for (int i = 0; i < size; i++) {
                parent[i] = i;
                rank[i] = 0;
            }
        }

        public int find(int x) {
            if (parent[x] != x)
                parent[x] = find(parent[x]); // Path compression
            return parent[x];
        }

        public void union(int x, int y) {
            int rootX = find(x);
            int rootY = find(y);

            if (rootX != rootY) {
                // 未连通
                if (rank[rootX] < rank[rootY]) {
                    parent[rootX] = rootY;
                } else if (rank[rootX] > rank[rootY]) {
                    parent[rootY] = rootX;
                } else {
                    parent[rootY] = rootX;
                    rank[rootX]++;
                }
            }
        }
    }

    private void updateLevelAliasGrouping(List<TreeSet<Integer>> aliasGrouping) {
        int curVarId = varCount - 1;    // 前面操作中 varCount++

        Set<Integer> colSet = new HashSet<>();
        for (int c = 0; c < alias2ID.size(); c++) {
            if (trieMatrix[curVarId][c])
                colSet.add(c);  // 获取相关表
        }



        Set<Integer> allCols = new HashSet<>(colSet);   // 保存副本

        // 使用Union-Find高效合并当前(已加入)变量相关的相交集—————— 连通的层， 分为一组，不同组不存在相关约束，同组内各表必定有同约束的路径
        UnionFind unionFind = new UnionFind(curVarId);  /// size为当前加入的层数目
        Map<Integer, List<Integer>> columnToRows = new HashMap<>(); // 构建表-层映射

        // 对当前行中的每列(表)，找出它在之前行的位置
        for (int c : allCols) {
            List<Integer> rows = new ArrayList<>();
            for (int preVarId = curVarId - 1; preVarId >= 0; preVarId--) {
                if (trieMatrix[preVarId][c])
                    rows.add(preVarId); // 记录表c出现的历史层
            }
            if (!rows.isEmpty()) {  // 如果表c有历史层， 没有历史层的不处理
                columnToRows.put(c, rows);  // 添加表-层映射————（一个表有哪些历史层）
                // 合并所有相关行到同一连接组件
                for (int i = 1; i < rows.size(); i++)   // 有历史层的可以尝试连通
                    unionFind.union(rows.get(0), rows.get(i));
            }
        }
        
        // FIX: 确保在同一历史层共同出现的表被连通
        // 这样它们在后续层会被分到同一组，保持共同的父路径约束
        for (Map.Entry<Integer, List<Integer>> entry1 : columnToRows.entrySet()) {
            for (Map.Entry<Integer, List<Integer>> entry2 : columnToRows.entrySet()) {
                if (entry1.getKey() >= entry2.getKey()) continue; // 避免重复和自比较
                
                List<Integer> rows1 = entry1.getValue();
                List<Integer> rows2 = entry2.getValue();
                
                // 找到它们共同出现的层
                for (int row1 : rows1) {
                    if (rows2.contains(row1)) {
                        // 两个表在 row1 层共同出现，连通它们
                        if (!rows1.isEmpty() && !rows2.isEmpty()) {
                            unionFind.union(rows1.get(0), rows2.get(0));
                        }
                        break; // 只需连通一次
                    }
                }
            }
        }

        // 基于连接组件创建分组
        Map<Integer, Set<Integer>> componentToColumns = new HashMap<>();
        // 提取连通分量
        for (Map.Entry<Integer, List<Integer>> entry : columnToRows.entrySet()) {
            int column = entry.getKey();
            List<Integer> rows = entry.getValue();

            if (!rows.isEmpty()) {
                int root = unionFind.find(rows.get(0));
                if (!componentToColumns.containsKey(root))
                    componentToColumns.put(root, new HashSet<>());
                componentToColumns.get(root).add(column);
            }
        }

        // 创建分组--（以前连通，并且当前层也连通的表）  这里感觉是不是多余了？
        for (Set<Integer> colGroup : componentToColumns.values()) { // 对每个连通分量（包含表）
            TreeSet<Integer> finalColGroup = new TreeSet<>();
            for (int column : colGroup) {
                // 只添加也存在于当前层的表（重检查防止多线程竞争导致的脏数据？）
                if (trieMatrix[curVarId][column])
                    finalColGroup.add(column);
            }   
            if (!finalColGroup.isEmpty()) {
                allCols.removeAll(finalColGroup);   // 从剩余表中移除
                aliasGrouping.add(finalColGroup);
            }
        }

        // 将剩下的表（历史层不连通的表）设为一组
        if (!allCols.isEmpty())
            aliasGrouping.add(new TreeSet<>(allCols));

    }

    public long nextLevelCost = 0;
    public long innerJoinCost = 0;
    public long outterJoinCost = 0;
    public long addVariableCost = 0;
    public long initeTriesCost = 0;


    public void executeJoin(Set<ColumnRef> var) {
        int varID = varCount;

        // 根据每个表的历史层的连通性将与var相关的表进行分组，没有历史层的表组合为一组
        List<TreeSet<Integer>> aliasGrouping = addVariable(var);

        Trie[] groupedCurTries = new Trie[trieByVar[varID].length];
        int offset = 0;
        for (TreeSet<Integer> group : aliasGrouping) {
            for (int aliasID : group) {
                groupedCurTries[offset++] = tries[aliasID]; // 获得相关的Trie
            }

        }
        long innerJoinStart = System.currentTimeMillis();

        generator.genJoinList(aliasGrouping);     // 为每个分组生成连接计划， 进行了组内连接（Trie）

        innerJoinCost += (System.currentTimeMillis() - innerJoinStart);

        long nextLevelStart = System.currentTimeMillis();



        // CompletableFuture<?>[] futures = new CompletableFuture[groupedCurTries.length];
        for (int i = 0; i < groupedCurTries.length; i++) {
            final Trie trie = groupedCurTries[i];
            // futures[i] = CompletableFuture.runAsync(() -> {
                trie.nextLevel();      // 构造每个Trie的下一层（本层）
            // }); 
        }
        // for (Future<?> future : futures) {
        //     try {
        //         future.get();
        //     } catch (InterruptedException | ExecutionException e) {
        //         e.printStackTrace();
        //     }
        // }



        nextLevelCost += (System.currentTimeMillis() - nextLevelStart);

        
        long outterJoinStart = System.currentTimeMillis();

        int[][] joinLists = generator.nextGroupOfJoinList();


        while (joinLists.length != 0) {

            paraleleExecute(joinLists, groupedCurTries);    // 利用 ResultTrie进行 笛卡尔积验证

            joinLists = generator.nextGroupOfJoinList();

        }
        
        outterJoinCost += (System.currentTimeMillis() - outterJoinStart);

        long addVariableStart = System.currentTimeMillis();

        generator.addVariable(trieByVar[varID]);    // 在 ResultTrie 中推进一层，加入有效值后，回溯剪枝


        addVariableCost += (System.currentTimeMillis() - addVariableStart);
    }

    private void paraleleExecute(int[][] joinLists, Trie[] groupedCurTries) {
         for (int[] joinList: joinLists) {
            LeapFrogJoin join = new LeapFrogJoin(joinList, groupedCurTries);
            join.execute();
        }
    }

    public int[] genResultTuple() {
        List<int[]> tuples = generator.genResult();

        final int cnt = tries.length;
        
        for (int i = 0; i < cnt; i++) {
            tries[i].lastLevel();
        }

        int[] joinResult = new int[this.aggregateDatas.length];
        Arrays.fill(joinResult, -1);
        
    
        for (final int[] tuple : tuples) {
            List<List<Integer>> dimensions = new ArrayList<>(cnt);
    
            for (int tid = 0; tid < cnt; tid++) {
                final Trie trie = tries[tid];
                if (trie.curLevel == -1) {
                    List<Integer> result = new ArrayList<>();
                    result.add(-1);
                    dimensions.add(result);
                } else {
                    int baseIdx = tuple[tid] << 1;
                    int start = trie.curLevelValueBounds.data[baseIdx];
                    int end = trie.curLevelValueBounds.data[baseIdx + 1];
                    final int[] sourceIndices = trie.tupleIdx;
                    List<Integer> result = new ArrayList<>();

                    for (int i = start; i < end; i++) {
                        result.add(sourceIndices[i]);
                    }
                    dimensions.add(result);
                }
            }
            getAggregateResult(dimensions, joinResult);
        }
        return joinResult;
    }

    void getAggregateResult(List<List<Integer>> results, int[] joinResult) {
        int aliasCtr = 0;
        for (List<Integer> rids: results) {
            if (aggregateInfo.containsKey(aliasCtr)) {
                for (int aggregateColumnCtr : aggregateInfo.get(aliasCtr)) {
                    IntData columnData = (IntData) this.aggregateDatas[aggregateColumnCtr].columnData;
                    for (int row : rids) {
                        int value = columnData.data[row];
                        if (joinResult[aggregateColumnCtr] != -1) {
                            if ((value < joinResult[aggregateColumnCtr] && aggregateDatas[aggregateColumnCtr].isMin)
                                    || (value > joinResult[aggregateColumnCtr]
                                            && !aggregateDatas[aggregateColumnCtr].isMin))
                                joinResult[aggregateColumnCtr] = value;
                        } else {
                            joinResult[aggregateColumnCtr] = value;
                        }
                    }
                }
            }
            aliasCtr++;
        }
    }
}
