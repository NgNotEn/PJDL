package joining;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import catalog.CatalogManager;
import config.JoinConfig;
import config.NamingConfig;
import config.PreConfig;
import joining.parallel.threads.ThreadPool;
import joining.result.ResultTuple;
import operators.Materialize;
import preprocessing.Context;
import query.ColumnRef;
import query.QueryInfo;
import statistics.JoinStats;

/**
 * This variant of the join processor parallelize
 * data processing in joining.parallel via multiple
 * threads.
 */
public class ParallelJoinProcessor {

    /**
     * The list of final result tuples.
     */
    public static ResultTuple[] results;

    /**
     * Executes the join phase and stores result in relation.
     * Also updates mapping from query column references to
     * database columns.
     *
     * @param query   query to process
     * @param context query execution context
     */
    public static void process(QueryInfo query,
                               Context context) throws Exception {
        long startMillis = System.currentTimeMillis();
        // there is no predicate to evaluate in join phase.
        
        // 如果等价类和非等价类谓词都为空，并且开启了过滤选项，则直接将过滤后的表作为连接结果
        if (query.equiJoinPreds.size() == 0 && query.nonEquiJoinPreds.size() == 0 && PreConfig.FILTER) {
            String targetRelName = NamingConfig.JOINED_NAME;
            Materialize.executeFromExistingTable(query.colsForPostProcessing,
                    context.columnMapping, targetRelName);
            // Measure execution time for join phase
            JoinStats.subExeTime.add(0L);
            // Update processing context
            context.columnMapping.clear();
            for (ColumnRef postCol : query.colsForPostProcessing) {
                String newColName = postCol.aliasName + "." + postCol.columnName;
                ColumnRef newRef = new ColumnRef(targetRelName, newColName);
                context.columnMapping.put(postCol, newRef);
            }
            // Store number of join result tuples
            int skinnerJoinCard = CatalogManager.getCardinality(targetRelName);
            JoinStats.skinnerJoinCards.add(skinnerJoinCard);
            System.out.println("Join card: " + skinnerJoinCard + "\tJoin time:" + Arrays.toString(JoinStats.subExeTime.toArray()));
        } 
        // 如果等价类非空
        else if (query.equiJoinPreds.size() > 0) {
            // Join condition, apply leapfrog triejoin
            List<ResultTuple> resultTuples = new ArrayList<>();
            long startJoinMillis = System.currentTimeMillis();

            /****************************************
             *                                      *
             *                                      *
             *          change , functions          *
             *                                      *
             *                                      *    
             ****************************************/

            // ===================== join sta =====================
            List<Set<ColumnRef>> varOrder = JoinProcessor.planGenerator(query, context);  // 生成连接顺序
            // 生成 任务生成器
            TrieManager manager = new TrieManager(query, context, varOrder);
            JoinProcessor.manager = manager;


            List<JoinProcessor> tasks = new ArrayList<>();
            long totalCombinations = manager.getTotalCombinations();
            System.out.println("Total combinations to process: " + totalCombinations);
            System.out.println("Number of threads: " + JoinConfig.NTHREAD);
            System.out.println("Expected batches per thread: " + (totalCombinations / JoinConfig.NTHREAD));
            System.out.println("Starting join...");
            
            // join tasks
            for (int i = 0; i < JoinConfig.NTHREAD; i++) {
                tasks.add(new JoinProcessor(i, query, context, varOrder));
            }

            List<Future<List<int[]>>> evaluateResults = ThreadPool.executorService.invokeAll(tasks);


            for (Future<List<int[]>> futureResult : evaluateResults) {
                List<int[]> joinResults = futureResult.get();
                if (joinResults.size() > 0) {
                    for (int[] result : joinResults) {
                        resultTuples.add(new ResultTuple(result));
                    }
                }

            }

            // ===================== join end =====================

            // // Initialize UCT join order search tree
            // StaticLFTJCollections.init(query, context);
            // HypercubeManager.init(StaticLFTJCollections.joinValueBound, JoinConfig.INITCUBE);
            // ParallelUctNodeLFTJ root = new ParallelUctNodeLFTJ(0, query, true, JoinConfig.NTHREAD);
            // List<AsyncParallelJoinTask> tasks = new ArrayList<>();
            // System.out.println("start join");
            // System.out.println("start cube number:" + HypercubeManager.hypercubes.size());
            
            // // join tasks
            // for (int i = 0; i < JoinConfig.NTHREAD; i++) {
            //     tasks.add(new AsyncParallelJoinTask(query, context, root, i));
            // }

            // List<Future<ParallelJoinResult>> evaluateResults = ThreadPool.executorService.invokeAll(tasks);
            // for (Future<ParallelJoinResult> futureResult : evaluateResults) {
            //     ParallelJoinResult joinResult = futureResult.get();
            //     if (joinResult.result.size() > 0) {
            //         for (int[] result : joinResult.result) {
            //             resultTuples.add(new ResultTuple(result));
            //         }
            //     }
            // }

            long joinTime = (System.currentTimeMillis() - startJoinMillis);
            System.out.println("WCOJ time:" + joinTime);
            System.out.println("Finish Parallel Join!");
            System.out.println("Materializing join result with " + resultTuples.size() + " tuples ...");

            JoinStats.exeTime += joinTime;
            JoinStats.subExeTime.add(joinTime);

            // Materialize result table
            long materializeStart = System.currentTimeMillis();
            String targetRelName = NamingConfig.JOINED_NAME;
            Materialize.execute(resultTuples, query.aliasToIndex,
                    query.colsForPostProcessing,
                    context.columnMapping, targetRelName);
//            // Update processing context
            context.columnMapping.clear();
            for (ColumnRef postCol : query.colsForPostProcessing) {
                String newColName = postCol.aliasName + "." + postCol.columnName;
                ColumnRef newRef = new ColumnRef(targetRelName, newColName);
                context.columnMapping.put(postCol, newRef);
            }
            long materializeEnd = System.currentTimeMillis();
            JoinStats.subMateriazed.add(materializeEnd - materializeStart);
            // Store number of join result tuples
            int skinnerJoinCard = resultTuples.size();
            JoinStats.skinnerJoinCards.add(skinnerJoinCard);
            System.out.println("Join card: " + skinnerJoinCard + "\tJoin time:" + Arrays.toString(JoinStats.subExeTime.toArray()));

        } else if (query.nonEquiJoinPreds.size() > 0) {
            // only contain non equality predicates
            long startJoinMillis = System.currentTimeMillis();
            // only support two tables here
            int cardinality1 = CatalogManager.getCardinality(context.aliasToFiltered.get(query.aliases[0]));
            int cardinality2 = CatalogManager.getCardinality(context.aliasToFiltered.get(query.aliases[1]));
            // parallel
            List<ResultTuple> resultTuples = IntStream.range(0, cardinality1).parallel().mapToObj(i -> {
                List<ResultTuple> currentResultTuples = new ArrayList<>();
                int[] rowIds = new int[]{i, 0};
                while (rowIds[1] < cardinality2) {
                    if (query.nonEquiJoinNodes.get(0).evaluate(rowIds, 1, cardinality2)) {
                        // satisfy condition
                        currentResultTuples.add(new ResultTuple(rowIds));
                    }
                    rowIds[1]++;
                }
                return currentResultTuples;
            }).flatMap(List::stream).collect(Collectors.toList());

            long joinTime = (System.currentTimeMillis() - startJoinMillis);
            JoinStats.exeTime += joinTime;
            JoinStats.subExeTime.add(joinTime);

            long materializeStart = System.currentTimeMillis();
            String targetRelName = NamingConfig.JOINED_NAME;
            Materialize.execute(resultTuples, query.aliasToIndex,
                    query.colsForPostProcessing,
                    context.columnMapping, targetRelName);
            context.columnMapping.clear();
            for (ColumnRef postCol : query.colsForPostProcessing) {
                String newColName = postCol.aliasName + "." + postCol.columnName;
                ColumnRef newRef = new ColumnRef(targetRelName, newColName);
                context.columnMapping.put(postCol, newRef);
            }
            long materializeEnd = System.currentTimeMillis();
            JoinStats.subMateriazed.add(materializeEnd - materializeStart);
            // Store number of join result tuples
            int skinnerJoinCard = resultTuples.size();
            JoinStats.skinnerJoinCards.add(skinnerJoinCard);
            System.out.println("Join card: " + skinnerJoinCard + "\tJoin time:" + Arrays.toString(JoinStats.subExeTime.toArray()));
        }
        // Measure execution time for join phase
        JoinStats.joinMillis += System.currentTimeMillis() - startMillis;
    }


}
