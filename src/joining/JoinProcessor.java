package joining;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

import buffer.BufferManager;
import catalog.CatalogManager;
import data.ColumnData;
import data.DoubleData;
import data.IntData;
import expressions.ExpressionInfo;
import predicate.NonEquiNode;
import preprocessing.Context;
import query.ColumnRef;
import query.QueryInfo;


public class JoinProcessor implements Callable<List<int[]>>{

    // 生成一个attribute order
    public static List<Set<ColumnRef>> planGenerator(QueryInfo query, Context context) throws Exception {
        BitSet preStatus = new BitSet(query.aliases.length);
        Map<String, Integer> s2i = query.aliasToIndex;

        // 创建一个包含原始Set<ColumnRef>和评分的列表
        List<Map.Entry<Set<ColumnRef>, Double>> scoredEqClasses = new ArrayList<>();

        // 收集所有基数用于归一化
        List<Double> allCardinalities = new ArrayList<>();  // all min cardinalities
        List<Double> allEqClassSizes = new ArrayList<>();   // all eqClass sizes

        // 为每个等价类计算最小基数
        Map<Set<ColumnRef>, Double> eqClassToMinCardinality = new HashMap<>();
        for (Set<ColumnRef> eqClass : query.equiJoinAttribute) {
            double minCardinality = Double.MAX_VALUE;

            // 找出等价类中基数最小的列
            for (ColumnRef colRef : eqClass) {
                String table = context.aliasToFiltered.get(colRef.aliasName);
                ColumnData colData = BufferManager.getData(new ColumnRef(table, colRef.columnName));
                if(colData instanceof IntData) {
                    IntData intData = (IntData) colData;
                    minCardinality = Math.min(minCardinality, intData.cardinality);
                }
                else if(colData instanceof DoubleData) {
                    DoubleData doubleData = (DoubleData) colData;
                    minCardinality = Math.min(minCardinality, doubleData.cardinality);
                }
            }

            eqClassToMinCardinality.put(eqClass, minCardinality);
            allCardinalities.add(minCardinality);
            allEqClassSizes.add((double) eqClass.size());
        }


        // 对基数进行对数变换以减少差距
        List<Double> logCardinalities = new ArrayList<>();
        for (double cardinality : allCardinalities) {
            logCardinalities.add(Math.log(cardinality + 1)); // +1避免log(0)
        }

        // 计算归一化参数
        double minLogCard = Collections.min(logCardinalities);
        double maxLogCard = Collections.max(logCardinalities);
        double minSize = Collections.min(allEqClassSizes);
        double maxSize = Collections.max(allEqClassSizes);

        // 避免除零
        double logCardRange = (maxLogCard - minLogCard) == 0 ? 1 : (maxLogCard - minLogCard);
        double sizeRange = (maxSize - minSize) == 0 ? 1 : (maxSize - minSize);

        for (Set<ColumnRef> eqClass : query.equiJoinAttribute) {
            double minCardinality = eqClassToMinCardinality.get(eqClass);
            double logCardinality = Math.log(minCardinality + 1);

            // 归一化到[0,1]范围
            double normalizedLogCard = (logCardinality - minLogCard) / logCardRange;
            double normalizedSize = (eqClass.size() - minSize) / sizeRange;

            // 计算综合分数（基数越小越好，等价类大小越小越好）
            // 使用加权平均，基数权重更高
            double score = 0.7 * (1 - normalizedLogCard) + 0.3 * (1 - normalizedSize);

            scoredEqClasses.add(new AbstractMap.SimpleEntry<>(eqClass, score));
        }

        // 根据分数排序（分数大的优先，因为我们用的是1-normalized_value）
        Collections.sort(scoredEqClasses, Comparator.comparing(Map.Entry<Set<ColumnRef>, Double>::getValue).reversed());

        // 按连通性约束重新排序
        List<Set<ColumnRef>> plan = new ArrayList<>();  // 最终的等价类顺序
        Set<String> usedAliases = new HashSet<>();  // 已使用的aliasName集合
        List<Set<ColumnRef>> remaining = new ArrayList<>(); // 剩余未排序的等价类

        for (Map.Entry<Set<ColumnRef>, Double> entry : scoredEqClasses) {
            remaining.add(entry.getKey());  // 初始化剩余等价类列表
        }


        // 选择第一个等价类（分数最高的）
        if (!remaining.isEmpty()) {
            Set<ColumnRef> first = remaining.remove(0);
            plan.add(first);    // 添加到计划中
            for (ColumnRef colRef : first) {
                usedAliases.add(colRef.aliasName);
                preStatus.set(s2i.get(colRef.aliasName));
            }
        }


        // 按连通性和出现次数优先级选择后续等价类   ​​强制确保每一步连接都发生在一个新等价类和已有表集合之间​​
        while (!remaining.isEmpty()) {
            List<Set<ColumnRef>> candidates = new ArrayList<>();    // a eqClass include some colRef with same colRef.aliasName
            List<Set<ColumnRef>> best_candidates = new ArrayList<>();
            BitSet temp = new BitSet(preStatus.length());
            // 找出所有与已选择等价类连通的候选
            for (Set<ColumnRef> eqClass : remaining) {
                boolean isConnected = false;
                for (ColumnRef colRef : eqClass) {
                    if (usedAliases.contains(colRef.aliasName)) {
                        isConnected = true;
                        break;
                    }
                }
                if (isConnected) {
                    temp.clear();
                    for (ColumnRef colRef : eqClass) {
                        temp.set(s2i.get(colRef.aliasName));
                    }
                    if(temp.intersects(preStatus))  best_candidates.add(eqClass);
                    candidates.add(eqClass);
                }
            }

            if(!best_candidates.isEmpty()) {
                candidates = best_candidates;
            }

            if (candidates.isEmpty()) {
                // 如果没有连通的候选，选择分数最高的（保持原有排序）
                candidates.add(remaining.get(0));
            }

            // 按出现次数优先级排序候选者
            // 统计每个aliasName在所有等价类中的出现次数
            Map<String, Integer> aliasCount = new HashMap<>();
            for (Set<ColumnRef> eqClass : query.equiJoinAttribute) {
                for (ColumnRef colRef : eqClass) {
                    aliasCount.put(colRef.aliasName, aliasCount.getOrDefault(colRef.aliasName, 0) + 1);
                }
            }

            // 对候选者按照包含的aliasName出现次数进行排序
            Collections.sort(candidates, (eqClass1, eqClass2) -> {
                // 计算等价类中aliasName的最小出现次数
                int minCount1 = Integer.MAX_VALUE;
                int minCount2 = Integer.MAX_VALUE;

                for (ColumnRef colRef : eqClass1) {
                    minCount1 = Math.min(minCount1, aliasCount.get(colRef.aliasName));
                }
                for (ColumnRef colRef : eqClass2) {
                    minCount2 = Math.min(minCount2, aliasCount.get(colRef.aliasName));
                }

                // 优先选择包含出现次数少的alias的等价类
                int result = Integer.compare(minCount1, minCount2);
                if (result != 0)
                    return result;

                // 如果出现次数相同，按原有评分排序
                return Double.compare(eqClassToMinCardinality.get(eqClass2), eqClassToMinCardinality.get(eqClass1));
            });

            // 选择最佳候选
            Set<ColumnRef> selected = candidates.get(0);

            
            preStatus.clear();
            for(ColumnRef col: selected) {
                preStatus.set(s2i.get(col.aliasName));
            }


            plan.add(selected);
            remaining.remove(selected);

            // 更新已使用的aliases
            for (ColumnRef colRef : selected) {
                usedAliases.add(colRef.aliasName);
            }
        }

        return plan;
    }




    /*=========================================连接===========================================*/ 
    public static TrieManager manager;
    static QueryInfo query;
    static Context context;
    static List<Set<ColumnRef>> varOrder;

    int threadId;
    
    JoinProcessor(int threadId, QueryInfo query, Context context, List<Set<ColumnRef>> varOrder) {
        this.threadId = threadId;
        static_init(query, context, varOrder);
    }

    static void static_init(QueryInfo _query, Context _context, List<Set<ColumnRef>> _varOrder) {
        query = _query;
        context = _context;
        varOrder = _varOrder;
    }

    @Override
    public List<int[]> call() throws Exception {
        List<int[]> joinResults = new ArrayList<>();
        while(true) {
            Trie[] tries =  manager.next_tries();
            if(tries == null) break;
            LeapFrogTrieJoin lftj = new LeapFrogTrieJoin(tries);
            for (Set<ColumnRef> var : varOrder) {
                lftj.executeJoin(var);
            }
            joinResults.addAll(handleNonEquiPredicates(lftj.genResultTuple()));
        }

        return joinResults;
    }


    public List<int[]> handleNonEquiPredicates(List<int[]> currentResult) {
        List<int[]> joinResult = new ArrayList<>();
        if (currentResult.size() > 0) {
            // we have non equality predicates here.
            if (query.nonEquiJoinPreds.size() > 0) {
                // number of non equality join predicates
                int nrNonEquivalentPredict = query.nonEquiJoinNodes.size();
                List<ExpressionInfo> nonEquiExpressions = query.nonEquiJoinPreds;
                // map non equality predicate id to table id
                HashMap<Integer, Integer> nonEquiTablesToAliasIndex = new HashMap<>();
                HashMap<Integer, Integer> nonEquiCards = new HashMap<>();
                Set<Integer> availableTables = new HashSet<>();

                for (int tid = 0; tid < query.nrJoined; tid++) {
                    availableTables.add(tid);
                    for (int eid = 0; eid < nrNonEquivalentPredict; eid++) {
                        if (!nonEquiTablesToAliasIndex.containsKey(eid) && availableTables.containsAll(
                                nonEquiExpressions.get(eid).aliasIdxMentioned)) {
                            nonEquiTablesToAliasIndex.put(eid, tid);
                            String alias = query.aliases[tid];
                            String table = context.aliasToFiltered.get(alias);
                            nonEquiCards.put(eid, CatalogManager.getCardinality(table));
                        }
                    }
                }

                Set<Integer> validateRowIds = new HashSet<>(currentResult.size());
                for (int rid = 0; rid < currentResult.size(); rid++) {
                    validateRowIds.add(rid);
                }

                for (int nonEquiPredictId = 0; nonEquiPredictId < nrNonEquivalentPredict; nonEquiPredictId++) {
                    int processTableId = nonEquiTablesToAliasIndex.get(nonEquiPredictId);
                    int cardinality = nonEquiCards.get(nonEquiPredictId);
                    NonEquiNode nonEquiNode = query.nonEquiJoinNodes.get(nonEquiPredictId);
                    Set<Integer> currentValidateRowIds = new HashSet<>();
                    for (int rid : validateRowIds) {
                        int[] result = currentResult.get(rid);
                        if (result[processTableId] == -1) {
                            // non equality condition is not mentioned before, should be exist or non-exist in subqueries
                            // Create a copy to avoid modifying the original array during evaluation
                            int[] testResult = Arrays.copyOf(result, result.length);
                            testResult[processTableId] = 0;
                            while (testResult[processTableId] < cardinality) {
                                if (nonEquiNode.evaluate(testResult, processTableId, cardinality)) {
                                    // satisfy condition - update original with found value
                                    result[processTableId] = testResult[processTableId];
                                    currentValidateRowIds.add(rid);
                                    break;
                                } else {
                                    testResult[processTableId]++;
                                }
                            }
                        } else {
                            // test directly
                            if (nonEquiNode.evaluate(result, processTableId, cardinality)) {
                                // satisfy condition
                                currentValidateRowIds.add(rid);
                            }
                        }
                    }
                    validateRowIds = currentValidateRowIds;
                }

                for (int validateRowId : validateRowIds) {
                    joinResult.add(currentResult.get(validateRowId));
                }
            } else {
                // apply cartesian
                boolean applyCartesianProduct = false;
                int cartesianProductTableIndex = -1;
                for (int tid = 0; tid < currentResult.get(0).length; tid++) {
                    int rid = currentResult.get(0)[tid];
                    if (rid == -1) {
                        applyCartesianProduct = true;
                        cartesianProductTableIndex = tid;
                        break;
                    }
                }
                if (applyCartesianProduct) {
                    // add more results here
                    String alias = query.aliases[cartesianProductTableIndex];
                    String table = context.aliasToFiltered.get(alias);
                    int card = CatalogManager.getCardinality(table);
                    for (int[] result : currentResult) {
                        for (int i = 0; i < card; i++) {
                            int[] newResult = Arrays.copyOf(result, result.length);
                            newResult[cartesianProductTableIndex] = i;
                            joinResult.add(newResult);
                        }
                    }
                } else {
                    joinResult.addAll(currentResult);
                }
            }
        }
        return joinResult;
    }
}