package indexing;

import buffer.BufferManager;
import catalog.CatalogManager;
import catalog.info.DbInfo;
import config.IndexingMode;
import data.ColumnData;
import data.DoubleData;
import data.IntData;
import java.util.*;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import joining.BaseTrie;
import query.ColumnRef;

/**
 * Features utility functions for creating indexes.
 *
 * @author immanueltrummer
 */
public class Indexer {
    /**
     * Create an index on the specified column.
     *
     * @param colRef create index on this column
     */
    public static void index(ColumnRef colRef) throws Exception {
        // Check if index already exists
        if (!BufferManager.colToIndex.containsKey(colRef)) {
            ColumnData data = BufferManager.getData(colRef);
            if (data instanceof IntData) {
                IntData intData = (IntData) data;
                IntIndex index = new IntIndex(intData);
                BufferManager.colToIndex.put(colRef, index);
            } else if (data instanceof DoubleData) {
                DoubleData doubleData = (DoubleData) data;
                DoubleIndex index = new DoubleIndex(doubleData);
                BufferManager.colToIndex.put(colRef, index);
            }
        }
    }

    /**
     * Creates an index for each key/foreign key column.
     *
     * @param mode determines on which columns to create indices
     * @throws Exception
     */
    public static void indexAll(IndexingMode mode) throws Exception {
        System.out.println("Indexing all key columns ...");
        long startMillis = System.currentTimeMillis();

        // 收集所有需要索引的列引用
        List<ColumnRef> columnsToIndex = CatalogManager.currentDB.nameToTable.values()
                .parallelStream()
                .flatMap(tableInfo -> tableInfo.nameToCol.values().parallelStream()
                        .filter(columnInfo -> mode.equals(IndexingMode.ALL) ||
                                (mode.equals(IndexingMode.ONLY_KEYS) &&
                                        (columnInfo.isPrimary || columnInfo.isForeign)))
                        .map(columnInfo -> new ColumnRef(tableInfo.name, columnInfo.name)))
                .collect(Collectors.toList());

        // 使用自定义线程池进行并行索引
        int numThreads = Math.min(columnsToIndex.size(), Runtime.getRuntime().availableProcessors());
        ForkJoinPool customThreadPool = new ForkJoinPool(numThreads);

        try {
            customThreadPool.submit(() -> columnsToIndex.parallelStream().forEach(colRef -> {
                try {
                    System.out.println("Indexing " + colRef + " ...");
                    index(colRef);
                } catch (Exception e) {
                    System.err.println("Error indexing " + colRef);
                    e.printStackTrace();
                }
            })).get();
        } finally {
            customThreadPool.shutdown();
        }

        long totalMillis = System.currentTimeMillis() - startMillis;
        System.out.println("Indexing took " + totalMillis + " ms.");
    }

    public static void buildSortIndices(DbInfo dbInfo) throws Exception {
        System.out.println("Build sorted indices ...");
        long startMillis = System.currentTimeMillis();
        BaseTrie.orderCache = new HashMap<>();



        //  // TEst1数据

        // // r表
        // int r_card = CatalogManager.getCardinality("r");
        // ColumnRef columnRef1 = new ColumnRef("r", "a");
        // buildSortIndices(Arrays.asList(columnRef1), r_card);
        // // s表
        // int s_card = CatalogManager.getCardinality("s");
        // ColumnRef columnRef2 = new ColumnRef("s", "a");
        // buildSortIndices(Arrays.asList(columnRef2), s_card);
        // // t表
        // int t_card = CatalogManager.getCardinality("t");
        // ColumnRef columnRef3 = new ColumnRef("t", "a");
        // buildSortIndices(Arrays.asList(columnRef3), t_card);
        // // u表
        // int u_card = CatalogManager.getCardinality("u");
        // ColumnRef columnRef4 = new ColumnRef("u", "b");
        // buildSortIndices(Arrays.asList(columnRef4), u_card);






        // cast_info
        int ci_card = CatalogManager.getCardinality("cast_info");
        ColumnRef columnRef1 = new ColumnRef("cast_info", "person_id");
        ColumnRef columnRef2 = new ColumnRef("cast_info", "movie_id");
        ColumnRef columnRef3 = new ColumnRef("cast_info", "person_role_id");
        ColumnRef columnRef4 = new ColumnRef("cast_info", "role_id");
        // build following sorted indices on cast_info      14种
        buildSortIndices(Arrays.asList(columnRef1, columnRef2), ci_card);
        buildSortIndices(Arrays.asList(columnRef2, columnRef1), ci_card);
        buildSortIndices(Arrays.asList(columnRef1, columnRef2, columnRef4), ci_card);
        buildSortIndices(Arrays.asList(columnRef1, columnRef4, columnRef2), ci_card);
        buildSortIndices(Arrays.asList(columnRef2, columnRef1, columnRef4), ci_card);
        buildSortIndices(Arrays.asList(columnRef2, columnRef4, columnRef1), ci_card);
        buildSortIndices(Arrays.asList(columnRef4, columnRef1, columnRef2), ci_card);
        buildSortIndices(Arrays.asList(columnRef4, columnRef2, columnRef1), ci_card);
        buildSortIndices(Arrays.asList(columnRef1, columnRef2, columnRef3), ci_card);
        buildSortIndices(Arrays.asList(columnRef1, columnRef3, columnRef2), ci_card);
        buildSortIndices(Arrays.asList(columnRef2, columnRef1, columnRef3), ci_card);
        buildSortIndices(Arrays.asList(columnRef2, columnRef3, columnRef1), ci_card);
        buildSortIndices(Arrays.asList(columnRef3, columnRef1, columnRef2), ci_card);
        buildSortIndices(Arrays.asList(columnRef3, columnRef2, columnRef1), ci_card);

        
        // build following sorted indices on movie_info
        int mi_card = CatalogManager.getCardinality("movie_info");
        ColumnRef columnRef5 = new ColumnRef("movie_info", "movie_id");
        ColumnRef columnRef6 = new ColumnRef("movie_info", "info_type_id");
        buildSortIndices(Arrays.asList(columnRef5, columnRef6), mi_card);
        buildSortIndices(Arrays.asList(columnRef6, columnRef5), mi_card);


        // build following sorted indices on title
        int t_card = CatalogManager.getCardinality("title");
        ColumnRef columnRef7 = new ColumnRef("title", "id");
        buildSortIndices(Collections.singletonList(columnRef7), t_card);


        // build following sorted indices on name
        int n_card = CatalogManager.getCardinality("name");
        ColumnRef columnRef8 = new ColumnRef("name", "id");
        buildSortIndices(Collections.singletonList(columnRef8), n_card);


        // build following sorted indices on movie_info_idx
        int mii_card = CatalogManager.getCardinality("movie_info_idx");
        ColumnRef columnRef9 = new ColumnRef("movie_info_idx", "movie_id");
        ColumnRef columnRef10 = new ColumnRef("movie_info_idx", "info_type_id");
        buildSortIndices(Arrays.asList(columnRef9, columnRef10), mii_card);
        buildSortIndices(Arrays.asList(columnRef10, columnRef9), mii_card);

        int mc_card = CatalogManager.getCardinality("movie_companies");
        ColumnRef columnRef11 = new ColumnRef("movie_companies", "company_type_id");
        buildSortIndices(Arrays.asList(columnRef11), mc_card);

        ColumnRef columnRef12 = new ColumnRef("movie_companies", "company_id");
        buildSortIndices(Arrays.asList(columnRef12), mc_card);

        int mk_card = CatalogManager.getCardinality("movie_keyword");
        ColumnRef columnRef13 = new ColumnRef("movie_keyword", "keyword_id");
        buildSortIndices(Arrays.asList(columnRef13), mk_card);

        long totalMillis = System.currentTimeMillis() - startMillis;
        System.out.println("Indexing took " + totalMillis + " ms.");
    }

    public static void buildSortIndices(List<ColumnRef> columnRefs, int card) {
        List<ColumnData> trieCols = new ArrayList<>();
        List<ColumnRef> trieRefs = new ArrayList<>();
        for (ColumnRef columnRef : columnRefs) {
            try {
                trieCols.add(BufferManager.getData(columnRef));
                trieRefs.add(new ColumnRef(columnRef.aliasName, columnRef.columnName));
            } catch (Exception e) {
                System.err.println("Error sort indexing " + columnRef);
                e.printStackTrace();
            }
        }
        Integer[] indices = IntStream.range(0, card).boxed().toArray(Integer[]::new);
        Arrays.parallelSort(indices, (row1, row2) -> {
            for (int i = 0; i < trieCols.size(); i++) {
                ColumnData colData = trieCols.get(i);
                int cmp = colData.compareRows(row1, row2);
                if (cmp == 2) {
                    boolean row1null = colData.isNull.get(row1);
                    boolean row2null = colData.isNull.get(row2);
                    if (row1null != row2null) {
                        return row1null ? -1 : 1;
                    }
                } else if (cmp != 0) {
                    return cmp;
                }
            }
            return 0;
        });
        int[] tupleOrder = Arrays.stream(indices).mapToInt(Integer::intValue).toArray();

        System.out.println("add OrderCache: " + trieRefs);
        for (int i = 0; i < trieRefs.size(); i++) {
            List<ColumnRef> prefix = trieRefs.subList(0, i + 1);
            BaseTrie.orderCache.put(new ArrayList<>(prefix), tupleOrder);
        }
    }

}
