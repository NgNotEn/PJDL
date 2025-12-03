package joining;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;
import preprocessing.Context;
import query.ColumnRef;
import query.QueryInfo;
import util.Pair;

public class TrieManager {
    public static int totalVarCount;
    public static int aliasCnt;
    public static HashMap<String, Integer> alias2ID;

    private BaseTrie[] tries;

    // 使用普通变量替代原子变量（串行版本）
    private int totalCombination;
    private int[] currentIndices;  // 普通数组替代AtomicIntegerArray
    private Pair<Integer, Integer>[][] domains_by_basetire;
    private final int[] blocksPerTrie; // 存储每个trie的分块数
    private int maxCard;

    public TrieManager(QueryInfo query, Context context, List<Set<ColumnRef>> globalVarOrder) {
        // 获取变量总数
        totalVarCount = query.equiJoinAttribute.size();
        // 创建别名到ID（索引）的映射
        aliasCnt = query.aliases.length;
        alias2ID = new HashMap<>(aliasCnt * 4 / 3);
        for (int id = 0; id < aliasCnt; id++)
            alias2ID.put(query.aliases[id], id);

        // 串行初始化BaseTries
        tries = new BaseTrie[aliasCnt];
        List<Map.Entry<String, Integer>> entries = new ArrayList<>(alias2ID.entrySet());

        for (int i = 0; i < entries.size(); i++) {
            Map.Entry<String, Integer> entry = entries.get(i);
            String alias = entry.getKey();
            int id = entry.getValue();
            try {
                tries[id] = new BaseTrie(alias, id, query, context, globalVarOrder);
            } catch (Exception e) {
                System.err.println("Error initializing Trie for alias: " + alias + ", id: " + id);
                e.printStackTrace();
                throw new RuntimeException("Failed to initialize Trie for alias: " + alias, e);
            }
        }
        
        // Verify all tries are initialized
        for (int i = 0; i < aliasCnt; i++) {
            if (tries[i] == null) {
                throw new RuntimeException("Trie initialization failed for alias index: " + i);
            }
        }
        
        // 找到 cardinality 最大的 trie
        this.maxCard = tries[0].cardinality;
        for (int i = 1; i < aliasCnt; i++) {
            if (tries[i].cardinality > maxCard) {
                maxCard = tries[i].cardinality;
            }
        }
        
        // 根据最大表大小确定目标组合数和切割方案
        // 只切割前三大表，根据数据规模选择合适的组合数
        int targetCombinations;
        int[] topThreeSplits; // [最大表, 第二大表, 第三大表]
        
        if (maxCard > 50_000_000) {
            // 亿行级别: 512 = 32 × 8 × 2
            targetCombinations = 512;
            topThreeSplits = new int[]{32, 8, 2};
        } else if (maxCard > 10_000_000) {
            // 千万级别: 256 = 16 × 8 × 2
            targetCombinations = 256;
            topThreeSplits = new int[]{16, 8, 2};
        } else if (maxCard > 1_000_000) {
            // 百万级别: 128 = 8 × 8 × 2
            targetCombinations = 128;
            topThreeSplits = new int[]{8, 8, 2};
        } else if (maxCard > 100_000) {
            // 十万级别: 64 = 8 × 4 × 2
            targetCombinations = 64;
            topThreeSplits = new int[]{8, 4, 2};
        } else {
            // 小表: 32 = 4 × 4 × 2
            targetCombinations = 32;
            topThreeSplits = new int[]{4, 4, 2};
        }

        // 初始化普通变量（串行版本）
        currentIndices = new int[aliasCnt];
        domains_by_basetire = new Pair[aliasCnt][];
        blocksPerTrie = new int[aliasCnt];
        
        // 计算切割方案
        int[] splits = calculateOptimalSplits(tries, targetCombinations, topThreeSplits);
        
        int totalComb = 1;
        for(int i = 0; i < aliasCnt; i++) {
            int blocks = splits[i];
            if (blocks == 1) {
                // 不切割，保持完整 (Trie会用end-start计算cardinality，所以end要等于cardinality)
                domains_by_basetire[i] = new Pair[] { new Pair<>(0, tries[i].cardinality) };
            } else {
                // 均匀切割成 blocks 份
                domains_by_basetire[i] = split_trie_uniform(tries[i], blocks);
            }
            
            blocksPerTrie[i] = domains_by_basetire[i].length;
            if (blocksPerTrie[i] > 0) {
                totalComb *= blocksPerTrie[i];
            } else {
                totalComb = 0;
                break;
            }
        }
        
        totalCombination = totalComb;
    }
    
    /**
     * 计算最优切割方案
     * 只切割前三大表，使用预定义的分割比例
     * @param tries 所有表的BaseTrie数组
     * @param targetCombinations 目标组合数
     * @param topThreeSplits 前三大表的分割方案 [最大表, 第二大, 第三大]
     */
    private int[] calculateOptimalSplits(BaseTrie[] tries, int targetCombinations, int[] topThreeSplits) {
        int n = tries.length;
        int[] splits = new int[n];
        
        // 按 cardinality 降序排序的索引
        Integer[] sortedIndices = IntStream.range(0, n)
            .boxed()
            .sorted((i, j) -> Integer.compare(tries[j].cardinality, tries[i].cardinality))
            .toArray(Integer[]::new);
        
        // 初始化：所有表都不切（1份）
        for (int i = 0; i < n; i++) {
            splits[i] = 1;
        }
        
        // 如果表数少于3个，调整策略
        if (n < 3) {
            if (n == 1) {
                // 只有1个表：最大表分割数 × 第二大分割数
                splits[sortedIndices[0]] = topThreeSplits[0] * topThreeSplits[1];
            } else if (n == 2) {
                // 只有2个表：最大表翻倍，第二大保持
                splits[sortedIndices[0]] = topThreeSplits[0] * 2;
                splits[sortedIndices[1]] = topThreeSplits[1];
            }
        } else {
            // 3个或以上表：使用标准方案
            for (int rank = 0; rank < Math.min(3, n); rank++) {
                int idx = sortedIndices[rank];
                int card = tries[idx].cardinality;
                
                // 应用最小块大小约束（每块至少10行）
                int maxAllowed = Math.max(1, card / 10);
                splits[idx] = Math.min(topThreeSplits[rank], maxAllowed);
            }
        }
        
        // 计算实际组合数
        int actualCombinations = 1;
        for (int split : splits) {
            actualCombinations *= split;
        }
        
        System.out.println("Target combinations: " + targetCombinations + 
                           ", Actual combinations: " + actualCombinations);
        
        return splits;
    }
    
    /**
     * 均匀切割表成指定块数
     * 保证每个块至少有1行,避免空块或负数域
     */
    @SuppressWarnings("unchecked")
    private Pair<Integer, Integer>[] split_trie_uniform(BaseTrie trie, int blocks) {
        int card = trie.cardinality;
        if (card == 0 || blocks <= 0) {
            return new Pair[0];
        }
        
        if (blocks == 1) {
            // Trie用end-start计算cardinality，所以end=card而不是card-1
            return new Pair[] { new Pair<>(0, card) };
        }
        
        // 如果表太小,切不了那么多块,减少块数
        if (card < blocks) {
            blocks = card; // 每个块至少1行
        }
        
        final int finalBlocks = blocks;
        return IntStream.range(0, finalBlocks)
            .mapToObj(i -> {
                int start = (int)((long)i * card / finalBlocks);
                int end = (int)((long)(i + 1) * card / finalBlocks); // Trie用end-start，不需要-1
                if (i == finalBlocks - 1) end = card; // 最后一块到cardinality
                
                // 确保 start < end (Trie会用end-start计算，至少为1)
                if (start >= end) {
                    end = start + 1;
                }
                
                return new Pair<>(start, end);
            })
            .toArray(Pair[]::new);
    }

    public boolean has_next() {
        return totalCombination > 0;
    }


    private synchronized int[] genNext() {
        if (totalCombination <= 0) {
            return null;
        }
        
        // 递减总数
        totalCombination--;
        int[] result =  currentIndices.clone();
        // 更新索引（从最后一个开始递增）
        for (int i = aliasCnt - 1; i >= 0; i--) {
            currentIndices[i]++;
            
            if (currentIndices[i] < blocksPerTrie[i]) {
                break;
            } else {
                // 当前维度归零，继续处理前一个维度
                currentIndices[i] = 0;
            }
        }
        
        return result;
    }

    public Trie[] next_tries() {

        // 获取当前状态的快照
        int[] currentSnapshot = genNext();
        if(currentSnapshot == null) return null;
        
        // 创建Trie数组
        Pair<Integer, Integer>[] temp_domains = new Pair[aliasCnt];
        Trie[] _tries = new Trie[aliasCnt];
        
        // 获取对应的域范围
        for (int i = 0; i < aliasCnt; i++) {
            int index = currentSnapshot[i];
            // 添加边界检查
            if (index < 0 || index >= domains_by_basetire[i].length) {
                System.err.println("Invalid index for trie " + i + ": " + index + 
                    " (max: " + (domains_by_basetire[i].length - 1) + ")");
                // 使用第一个有效域
                index = 0;
            }
            temp_domains[i] = domains_by_basetire[i][index];
        }
        
        // 创建 Trie 实例
        for (int i = 0; i < aliasCnt; i++) {
            try {
                _tries[i] = new Trie(tries[i], temp_domains[i]);
            } catch (Exception e) {
                System.err.println("Failed to create Trie for index: " + i);
                System.err.println("Domain: " + temp_domains[i]);
                e.printStackTrace();
                throw new RuntimeException("Failed to create Trie for index: " + i, e);
            }
        }
        
        return _tries;
    }
    
    public long getTotalCombinations() {
        return totalCombination;
    }
    
}




// // 非并发版本-图数据库
// package joining;
// import java.util.ArrayList;
// import java.util.HashMap;
// import java.util.List;
// import java.util.Map;
// import java.util.Set;
// import java.util.concurrent.CompletableFuture;

// import preprocessing.Context;
// import query.ColumnRef;
// import query.QueryInfo;
// import util.Pair;

// public class TrieManager {
//     public static int totalVarCount;
//     public static int aliasCnt;
//     public static HashMap<String, Integer> alias2ID;

//     private BaseTrie[] tries;

//     // 使用普通变量替代原子变量
//     private int totalCombination;
//     private int[] currentIndices;
//     private Pair<Integer, Integer>[][] domains_by_basetire;
//     private final int[] blocksPerTrie; // 存储每个trie的分块数

//     public TrieManager(QueryInfo query, Context context, List<Set<ColumnRef>> globalVarOrder) {
//         // 获取变量总数
//         totalVarCount = query.equiJoinAttribute.size();
//         // 创建别名到ID（索引）的映射
//         aliasCnt = query.aliases.length;
//         alias2ID = new HashMap<>(aliasCnt * 4 / 3);
//         for (int id = 0; id < aliasCnt; id++)
//             alias2ID.put(query.aliases[id], id);

//         // init BaseTries
//         tries = new BaseTrie[aliasCnt];

//         System.out.println("Table number: " + aliasCnt);

//         List<Map.Entry<String, Integer>> entries = new ArrayList<>(alias2ID.entrySet());

//         CompletableFuture<?>[] futures = new CompletableFuture[entries.size()];
//         for (int i = 0; i < entries.size(); i++) {
//             final Map.Entry<String, Integer> entry = entries.get(i);
//             futures[i] = CompletableFuture.runAsync(() -> {
//                 String alias = entry.getKey();
//                 int id = entry.getValue();
//                 try {
//                     tries[id] = new BaseTrie(alias, id, query, context, globalVarOrder);
//                 } catch (Exception e) {
//                     System.err.println("Error initializing Trie for alias: " + alias + ", id: " + id);
//                     e.printStackTrace();
//                     throw new RuntimeException("Failed to initialize Trie for alias: " + alias, e);
//                 }
//             });
//         }

//         try {
//             CompletableFuture.allOf(futures).join();
//         } catch (Exception e) {
//             e.printStackTrace();
//             throw new RuntimeException("Failed to initialize tries", e);
//         }
        
//         // Verify all tries are initialized
//         for (int i = 0; i < aliasCnt; i++) {
//             if (tries[i] == null) {
//                 throw new RuntimeException("Trie initialization failed for alias index: " + i);
//             }
//         }
        

//         // 初始化普通变量
//         currentIndices = new int[aliasCnt];
//         domains_by_basetire = new Pair[aliasCnt][];
//         blocksPerTrie = new int[aliasCnt];
        
//         int totalComb = 1;
//         for(int i = 0; i < aliasCnt; i++) {
//             domains_by_basetire[i] = split_trie(tries[i], i);
//             blocksPerTrie[i] = domains_by_basetire[i].length;
//             if (blocksPerTrie[i] > 0) {
//                 totalComb *= blocksPerTrie[i];
//             } else {
//                 totalComb = 0;
//                 break;
//             }
//         }
        
//         totalCombination = totalComb;
//     }
    
//     private Pair<Integer, Integer>[] split_trie(BaseTrie trie, int idx) {
//         int card = trie.cardinality;
//         if (card == 0) return new Pair[0];
//         if (idx > 0) return new Pair[]{new Pair<>(0, card)};

//         int targetBlocks = 2048;
//         targetBlocks = Math.min(targetBlocks, card);
        
//         int blockSize = (card + targetBlocks - 1) / targetBlocks; // 向上取整
        
//         List<Pair<Integer, Integer>> blocks = new ArrayList<>();
//         for (int i = 0; i < card; i += blockSize) {
//             int end = Math.min(i + blockSize, card);
//             blocks.add(new Pair<>(i, end));
//             if (blocks.size() >= targetBlocks) break;
//         }
    
//         return blocks.toArray(new Pair[0]);
//     }



//     private synchronized int[] genNext() {
//         if (totalCombination <= 0) {
//             return null;
//         }
        
//         // 递减总数
//         totalCombination--;
            // int[] result = currentIndices.clone();
//         // 更新索引（类似数字递增）
//         for (int i = aliasCnt - 1; i >= 0; i--) {
//             int nextIndex = currentIndices[i] + 1;
            
//             if (nextIndex < blocksPerTrie[i]) {
//                 // 当前维度可以递增，不需要进位
//                 currentIndices[i] = nextIndex;
//                 break;
//             } else {
//                 // 当前维度归零，继续处理前一个维度
//                 currentIndices[i] = 0;
//             }
//         }
//         return result.clone();
//     }

//     public Trie[] next_tries() {
//         // 生成下一个，获取当前状态
//         int[] currentSnapshot = genNext();
//         if (currentSnapshot == null) {
//             return null;
//         }
        
//         // 创建Trie数组
//         Pair<Integer, Integer>[] temp_domains = new Pair[aliasCnt];
        
        
//         // 获取对应的域范围
//         for (int i = 0; i < aliasCnt; i++) {
//             int index = currentSnapshot[i];
//             // 添加边界检查
//             if (index < 0 || index >= domains_by_basetire[i].length) {
//                 System.err.println("Invalid index for trie " + i + ": " + index + 
//                     " (max: " + (domains_by_basetire[i].length - 1) + ")");
//                 // 使用第一个有效域
//                 index = 0;
//             }
//             temp_domains[i] = domains_by_basetire[i][index]; 
//         }
        
//         // 创建 Trie 实例
//         Trie[] _tries = new Trie[aliasCnt];
//         // 使用并行流创建 Trie 实例
//         try {
//             java.util.stream.IntStream.range(0, aliasCnt)
//                 .parallel()
//                 .forEach(i -> {
//                     try {
//                         _tries[i] = new Trie(tries[i], temp_domains[i]);
//                     } catch (Exception e) {
//                         System.err.println("Failed to create Trie for index: " + i);
//                         System.err.println("Domain: " + temp_domains[i]);
//                         e.printStackTrace();
//                         throw new RuntimeException("Failed to create Trie for index: " + i, e);
//                     }
//                 });
//         } catch (Exception e) {
//             throw new RuntimeException("Failed to create tries in parallel", e);
//         }
//         // for (int i = 0; i < aliasCnt; i++) {
//         //     try {
//         //         _tries[i] = new Trie(tries[i], temp_domains[i]);
//         //     } catch (Exception e) {
//         //         System.err.println("Failed to create Trie for index: " + i);
//         //         System.err.println("Domain: " + temp_domains[i]);
//         //         e.printStackTrace();
//         //         throw new RuntimeException("Failed to create Trie for index: " + i, e);
//         //     }
//         // }
        
//         return _tries;
//     }
    
//     public long getTotalCombinations() {
//         return totalCombination;
//     }
    
// }