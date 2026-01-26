package joining;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.ArrayDeque;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;

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

    

    public BaseTrie(String alias, int aliasID, QueryInfo query, Context context, List<Set<ColumnRef>> globalVarOrder) throws Exception {
        this.aliasID = aliasID;
        table = context.aliasToFiltered.get(alias);
        cardinality = CatalogManager.getCardinality(table); // 表基数是所有列的数量
        trieRefCols = new ArrayList<>();
        List<ColumnRef> order = new ArrayList<>();
        // 初始化实际数据
        for (Set<ColumnRef> eqClass : globalVarOrder) {
            for (ColumnRef colRef : eqClass) {      
                if (colRef.aliasName.equals(alias)) {   // 只存储参与了等值连接的列，不存储其他列
                    String colName = colRef.columnName;
                    ColumnRef bufferRef = new ColumnRef(table, colName);
                    order.add(bufferRef);
                    ColumnData colData = BufferManager.getData(bufferRef);
                    trieRefCols.add(colData);
                }
            }
        }
        maxLevel = trieRefCols.size();

        boolean notFiltered = !context.aliasToFiltered.get(alias).contains(".");

        if(orderCache == null) {
            orderCache = new ConcurrentHashMap<>();
        }

        if(notFiltered && orderCache.containsKey(order)) {
            tupleOrder = orderCache.get(order);
            System.out.println("Cache Hit!");
        } else {
            tupleOrder = buildTupleOrder();
            if(notFiltered) {
                orderCache.put(order, tupleOrder);
            }
        }


        trieCols = new ArrayList<>(maxLevel);
        for(ColumnData cd: trieRefCols) {
            trieCols.add(getIntValues(cd));
        }
    }

    private int[] buildTupleOrder() {
        int[] order = IntStream.range(0, cardinality).toArray();
        if(cardinality > 20000) {
            ForkJoinPool.commonPool().invoke(new ParallelQuickSort(order, 0, cardinality - 1, this));
        } else {
            // Custom iterative quicksort on primitive int[] to avoid boxing allocations
            ArrayDeque<int[]> stack = new ArrayDeque<>();
            stack.push(new int[]{0, cardinality - 1});
            while(!stack.isEmpty()) {
                int[] range = stack.pop();
                int l = range[0];
                int r = range[1];
                if(l >= r) continue;
                int pivotIdx = order[(l + r) >>> 1];
                int i = l;
                int j = r;
                while(i <= j) {
                    while(compareTuple(order[i], pivotIdx) < 0) i++;
                    while(compareTuple(order[j], pivotIdx) > 0) j--;
                    if(i <= j) {
                        int tmp = order[i];
                        order[i] = order[j];
                        order[j] = tmp;
                        i++; j--;
                    }
                }
                if(l < j) stack.push(new int[]{l, j});
                if(i < r) stack.push(new int[]{i, r});
            }
        }
        return order;
    }

    // Parallel quicksort to restore speed on large tables without boxing
    private static class ParallelQuickSort extends RecursiveAction {
        private static final int THRESHOLD = 20000;
        private final int[] arr;
        private final int left;
        private final int right;
        private final BaseTrie trie;

        ParallelQuickSort(int[] arr, int left, int right, BaseTrie trie) {
            this.arr = arr;
            this.left = left;
            this.right = right;
            this.trie = trie;
        }

        @Override
        protected void compute() {
            if (right - left < THRESHOLD) {
                quickSortIter(arr, left, right, trie);
                return;
            }
            int i = left;
            int j = right;
            int pivot = arr[(left + right) >>> 1];
            while (i <= j) {
                while (trie.compareTuple(arr[i], pivot) < 0) i++;
                while (trie.compareTuple(arr[j], pivot) > 0) j--;
                if (i <= j) {
                    int tmp = arr[i];
                    arr[i] = arr[j];
                    arr[j] = tmp;
                    i++; j--;
                }
            }
            ParallelQuickSort leftTask = null;
            ParallelQuickSort rightTask = null;
            if (left < j) leftTask = new ParallelQuickSort(arr, left, j, trie);
            if (i < right) rightTask = new ParallelQuickSort(arr, i, right, trie);
            if (leftTask != null && rightTask != null) {
                invokeAll(leftTask, rightTask);
            } else if (leftTask != null) {
                leftTask.invoke();
            } else if (rightTask != null) {
                rightTask.invoke();
            }
        }

        private static void quickSortIter(int[] order, int l, int r, BaseTrie trie) {
            ArrayDeque<int[]> stack = new ArrayDeque<>();
            stack.push(new int[]{l, r});
            while(!stack.isEmpty()) {
                int[] range = stack.pop();
                int left = range[0];
                int right = range[1];
                if(left >= right) continue;
                int pivotIdx = order[(left + right) >>> 1];
                int i = left;
                int j = right;
                while(i <= j) {
                    while(trie.compareTuple(order[i], pivotIdx) < 0) i++;
                    while(trie.compareTuple(order[j], pivotIdx) > 0) j--;
                    if(i <= j) {
                        int tmp = order[i];
                        order[i] = order[j];
                        order[j] = tmp;
                        i++; j--;
                    }
                }
                if(left < j) stack.push(new int[]{left, j});
                if(i < right) stack.push(new int[]{i, right});
            }
        }
    }

    private int compareTuple(int row1, int row2) {
        for (ColumnData colData : trieRefCols) {
            int cmp = compareColumnValues(colData, row1, row2);
            if (cmp != 0) {
                return cmp;
            }
        }
        return 0;
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
