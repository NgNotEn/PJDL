package joining;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.IntStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import buffer.BufferManager;
import catalog.CatalogManager;
import data.ColumnData;
import data.DoubleData;
import data.IntData;
import preprocessing.Context;
import query.ColumnRef;
import query.QueryInfo;

public class BaseTrie {
    public final int aliasID;               // 表别名ID
    public final String table;              // 表名
    public final int cardinality;           // 获取表的基数（行数）

    public final int maxLevel;  // 参与连接的列数：也即最大层数;  但有效层是从0开始的（层下标），也就是说如果curLevel == maxLevel，说明已经不是最后一层了
    public List<int[]> trieCols;
    public List<ColumnData> trieRefCols;


    public int[] tupleOrder;
    public static Map<List<ColumnRef>, int[]> orderCache;

    public boolean isTopTrie = false;

    public BaseTrie(String alias, int aliasID, QueryInfo query, Context context, List<Set<ColumnRef>> globalVarOrder) throws Exception {
        this.aliasID = aliasID;
        table = context.aliasToFiltered.get(alias);
        cardinality = CatalogManager.getCardinality(table); // 表基数是所有列的数量
        trieRefCols = new ArrayList<>();
        List<ColumnRef> order = new ArrayList<>();
        // 初始化实际数据
        int turns = 0;
        for (Set<ColumnRef> eqClass : globalVarOrder) {
            for (ColumnRef colRef : eqClass) {      
                if (colRef.aliasName.equals(alias)) {   // 只存储参与了等值连接的列，不存储其他列
                    if(turns == 0) isTopTrie = true;
                    String colName = colRef.columnName;
                    ColumnRef bufferRef = new ColumnRef(table, colName);
                    order.add(bufferRef);
                    ColumnData colData = BufferManager.getData(bufferRef);
                    trieRefCols.add(colData);
                }
                turns++;
            }
        }
        maxLevel = trieRefCols.size();

        boolean notFiltered = !context.aliasToFiltered.get(alias).contains(".");

        if(notFiltered && orderCache.containsKey(order)) {
            tupleOrder = orderCache.get(order);
            System.out.println("Cache Hit!");
            System.out.println(context.aliasToFiltered.get(alias));
        } else {
            
            CompletableFuture<int[]> future = CompletableFuture.supplyAsync(() -> 
                IntStream.range(0, cardinality)
                        .boxed()
                        .parallel()
                        .sorted(new Comparator<Integer>() {
                            @Override
                            public int compare(Integer row1, Integer row2) {
                                for (ColumnData colData : trieRefCols) {
                                    int cmp = compareColumnValues(colData, row1, row2);
                                    if (cmp != 0) {
                                        return cmp;
                                    }
                                }
                                return 0;
                            }
                        })
                        .mapToInt(i -> i)
                        .toArray()
                        , 
                ThreadPool.sortService
            );

            try {
                tupleOrder = future.get();  // 获取结果
            } catch (InterruptedException | ExecutionException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("排序任务执行失败", e);
            }

            if(notFiltered) {
                orderCache.put(order, tupleOrder);
            }
        }


        trieCols = new ArrayList<>(maxLevel);
        for(ColumnData cd: trieRefCols) {
            trieCols.add(getIntValues(cd));
        }
    }

    private static int compareColumnValues(ColumnData colData, int row1, int row2) {
        if (colData instanceof IntData) {
            IntData intData = (IntData) colData;
            return Integer.compare(intData.data[row1], intData.data[row2]);
        } else if (colData instanceof DoubleData) {
            DoubleData doubleData = (DoubleData) colData;
            return Double.compare(doubleData.data[row1], doubleData.data[row2]);
        } else {
            throw new IllegalArgumentException("Unsupported column data type: " + colData.getClass().getName());
        }
    }

    public Integer compareTuples(int row1, int row2) {
        for (ColumnData colData : trieRefCols) {
            int cmp = compareColumnValues(colData, row1, row2);
            if (cmp != 0) {
                return cmp;
            }
        }
        return 0;
    }

    private int[] getIntValues(ColumnData colData) {
        if (colData instanceof IntData) {
            return ((IntData) colData).data;
        } else if (colData instanceof DoubleData) {
            // Convert DoubleData to int for comparison
            double[] doubleData = ((DoubleData) colData).data;
            int[] intData = new int[doubleData.length];
            for (int i = 0; i < doubleData.length; i++) {
                intData[i] = (int) doubleData[i];
            }
            return intData;
        } else {
            throw new IllegalArgumentException("Unsupported column data type: " + colData.getClass().getName());
        }
    }
}
