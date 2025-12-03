package joining;
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

    private int totalCombination;
    private int[] currentIndices;
    private Pair<Integer, Integer>[][] domains_by_basetire;
    private final int[] blocksPerTrie; // 存储每个trie的分块数

    public TrieManager(QueryInfo query, Context context, List<Set<ColumnRef>> globalVarOrder) {
        // 获取变量总数
        totalVarCount = query.equiJoinAttribute.size();
        // 创建别名到ID（索引）的映射
        aliasCnt = query.aliases.length;
        alias2ID = new HashMap<>(aliasCnt * 4 / 3);
        for (int id = 0; id < aliasCnt; id++)
            alias2ID.put(query.aliases[id], id);

        // int maxCardIndex = 0;
        // int maxCard = 0;
        tries = new BaseTrie[aliasCnt];
        for (Map.Entry<String, Integer> entry : alias2ID.entrySet()) {
            String alias = entry.getKey();
            int id = entry.getValue();
            try {
                tries[id] = new BaseTrie(alias, id, query, context, globalVarOrder);
                // if(tries[id].cardinality > maxCard) {
                //     maxCard = tries[id].cardinality;
                //     maxCardIndex = id;
                // }
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

        currentIndices = new int[aliasCnt]; // 所有索引初始为0
        domains_by_basetire = new Pair[aliasCnt][];
        blocksPerTrie = new int[aliasCnt];
        
        totalCombination = 1;
        for(int i = 0; i < aliasCnt; i++) {
            domains_by_basetire[i] = split_trie(tries[i]);
            blocksPerTrie[i] = domains_by_basetire[i].length;
            if (blocksPerTrie[i] > 0) {
                totalCombination *= blocksPerTrie[i];
            } else {
                totalCombination = 0;
                break;
            }
        }
    }
    
    public static int SPLIT_LENGTH = 1 << 22;
    @SuppressWarnings("unchecked")
    private Pair<Integer, Integer>[] split_trie(BaseTrie trie) {

        // int card = trie.cardinality;
        // if (card == 0) {
        //     return new Pair[0];
        // }
        
        // if (trie.isTopTrie) {
        //     int blocks = Math.min(1, card);
        //     int baseSize = card / blocks; // 每块的基础大小
        //     int remainder = card % blocks; // 余数，需要分配到前remainder块中
            
        //     return IntStream.range(0, blocks)
        //         .mapToObj(i -> {
        //             int start = i * baseSize + Math.min(i, remainder);
        //             int blockSize = baseSize + (i < remainder ? 1 : 0);
        //             int end = start + blockSize;
        //             return new Pair<>(start, end);
        //         })
        //         .toArray(Pair[]::new);
        // } else {
        //     // 否则不切，返回整个Trie作为一个块
        //     return new Pair[] { new Pair<>(0, card) };
        // }


        int card = trie.cardinality;
        if (card == 0) {
            return new Pair[0];
        }
        int blocks = (card + SPLIT_LENGTH - 1) >> 22;
        return IntStream.range(0, blocks)
            .mapToObj(i -> {
                int start = i * SPLIT_LENGTH;
                int end = Math.min((i + 1) * SPLIT_LENGTH, card);
                return new Pair<>(start, end);
            })
            .toArray(Pair[]::new);
    }

    public boolean has_next() {
        return totalCombination > 0;
    }

    private boolean genNext() {
        if (totalCombination <= 0) {
            return false;
        }
        
        // 直接更新索引，无需CAS操作
        for (int i = aliasCnt - 1; i >= 0; i--) {
            int nextIndex = currentIndices[i] + 1;
            
            if (nextIndex < blocksPerTrie[i]) {
                // 当前维度可以递增，不需要进位
                currentIndices[i] = nextIndex;
                totalCombination--; // 减少剩余组合数
                return true;
            } else {
                // 当前维度归零，继续处理前一个维度
                currentIndices[i] = 0;
            }
        }
        
        totalCombination--; // 处理最后一个组合
        return totalCombination >= 0;
    }

    public Trie[] next_tries() {
        if (!genNext()) {
            return null; // 没有更多组合
        }
        
        // 创建Trie数组
        Pair<Integer, Integer>[] temp_domains = new Pair[aliasCnt];
        Trie[] _tries = new Trie[aliasCnt];
        
        // 获取对应的域范围
        for (int i = 0; i < aliasCnt; i++) {
            int index = currentIndices[i];
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

