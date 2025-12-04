package benchmark;import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;


import buffer.BufferManager;
import catalog.CatalogManager;
import config.GeneralConfig;
import config.StartupConfig;
import diskio.PathUtil;
import expressions.aggregates.AggInfo;
import expressions.aggregates.SQLaggFunction;
import indexing.Indexer;
import joining.AggregateData;
import joining.JoinProcessor;
import joining.ThreadPool;
import joining.TrieManager;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import preprocessing.Context;
import preprocessing.Preprocessor;
import query.ColumnRef;
import query.QueryInfo;
import types.SQLtype;

/**
 * Benchmarks pre-, join, and post-processing stage and compares
 * the output sizes against the sizes of results produced by
 * Postgres.
 *
 * @author immanueltrummer
 */
public class BenchAndVerify {

	/**
	 * Parses queries in all '.sql' files that are found
	 * in given directory and returns mapping from file
	 * names to queries.
	 *
	 * @param dirPath path to directory to read queries from
	 * @return ordered mapping from file names to queries
	 * @throws Exception
	 */
	public static Map<String, PlainSelect> readAllQueries(
			String dirPath) throws Exception {
		Map<String, PlainSelect> nameToQuery = new TreeMap<String, PlainSelect>();

		File dir = new File(dirPath);
		System.out.println("Reading SQL files from directory: " + dirPath);
		// System.out.println("Reading SQL file: " + dirPath);

		for (File file : dir.listFiles()) {
			if (file.getName().endsWith(".sql")) {
				String sql = new String(Files.readAllBytes(file.toPath()));
				// System.out.println(sql);
				Statement sqlStatement = CCJSqlParserUtil.parse(sql);
				Select select = (Select) sqlStatement;
				PlainSelect plainSelect = (PlainSelect) select.getSelectBody();
				nameToQuery.put(file.getName(), plainSelect);
			}
		}

		// String sql = new String(Files.readAllBytes(dir.toPath()));
		// System.out.println(sql);
		// Statement sqlStatement = CCJSqlParserUtil.parse(sql);
		// Select select = (Select) sqlStatement;
		// PlainSelect plainSelect = (PlainSelect) select.getSelectBody();
		// nameToQuery.put(dir.getName(), plainSelect);

		return nameToQuery;
	}

	/**
	 * Processes all queries in given directory.
	 *
	 * @param args first argument is Skinner DB directory,
	 *             second argument is query directory
	 *             third argument is Postgres database name
	 *             fourth argument is Postgres user name
	 *             fifth argument is Postgres user password
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {

		// Initialize database
		String SkinnerDbDir = args[0];
		String queryDir = args[1];
		PathUtil.initSchemaPaths(SkinnerDbDir);
		CatalogManager.loadDB(PathUtil.schemaPath);
		PathUtil.initDataPaths(CatalogManager.currentDB);
		System.out.println("Loading data ...");
		GeneralConfig.inMemory = true;
		BufferManager.loadDB();
		System.out.println("Data loaded.");
		Indexer.buildSortIndices(CatalogManager.currentDB);
		Indexer.indexAll(StartupConfig.INDEX_CRITERIA);
		// Read all queries from files
		Map<String, PlainSelect> nameToQuery = BenchAndVerify.readAllQueries(queryDir);



/*  ----------------------------  differ ----------------------------*/


		long qcnt = 0;
		long avgcost = 0;
		long maxcost = 0;
		long mincost = Integer.MAX_VALUE;
		long totalJoinCost = 0;

		ThreadPool.init();


		for (Entry<String, PlainSelect> entry : nameToQuery.entrySet()) {
			System.out.println("---------- " + entry.getKey() + " ----------");

			long start = System.currentTimeMillis();
			QueryInfo query = new QueryInfo(entry.getValue(),
					false, -1, -1, null);
			
			// 预处理
			Context preSummary = Preprocessor.process(query);
			// 连接

			

			long joinStart = System.currentTimeMillis();

			List<Set<ColumnRef>> varOrder = JoinProcessor.planGenerator(query, preSummary);  // 生成连接顺序
			try {
				int[] joinResult = before_join(query, preSummary, varOrder);
				System.out.print("result: ");
				for(int i = 0; i < joinResult.length; i++) {
				System.out.print(joinResult[i] + " ");
			}
			System.out.println();
			} catch (Exception e) {
				e.printStackTrace();
			}
			

			
			long joinEnd = System.currentTimeMillis();

			long preCost = (joinStart - start);
			long joinCost = (joinEnd - joinStart);
			long totalCost = (joinEnd - start);

			System.out.println("join cost: " + joinCost + " ms.");
			System.out.println();
			System.out.println();


			totalJoinCost += joinCost;
			qcnt++;
			avgcost += (totalCost - avgcost) / qcnt;
			maxcost = Math.max(maxcost, totalCost);
			mincost = Math.min(mincost, totalCost);

			


			// Clean up
			BufferManager.unloadTempData();
			CatalogManager.removeTempTables();
		}

		ThreadPool.shutdown();

		System.out.println("========== Summary ==========");
		System.out.println("- queryCnt: " + qcnt);
		System.out.println("- avgCost: " + avgcost + " ms");
		System.out.println("- maxCost: " + maxcost + " ms");
		System.out.println("- minCost: " + mincost + " ms");
		System.out.println("- totalcost: " + qcnt*avgcost + " ms");
		System.out.println("- avgJoinCost: " + totalJoinCost/qcnt + " ms");
		System.out.println("- totalJoinCost: " + totalJoinCost + " ms");

	}

	

	private static int[] before_join(QueryInfo query, Context context, List<Set<ColumnRef>> varOrder) throws Exception {
		int aggregateNum = query.aggregates.size();
        AggregateData[] aggregateDatas = new AggregateData[aggregateNum];
        SQLtype[] sqLtypes = new SQLtype[aggregateNum];
        Map<Integer, List<Integer>> aggregateInfo = new HashMap<>();

        int cnt = 0;
        for (AggInfo aggregate : query.aggregates) {
            AggregateData aggregateData = new AggregateData(); 
            aggregateData.isMin = (aggregate.aggFunction == SQLaggFunction.MIN);    // MIN or not MIN
            ColumnRef columnRef = aggregate.aggInput.columnsMentioned.iterator().next();
            ColumnRef filterColumnRef = new ColumnRef(context.aliasToFiltered.get(columnRef.aliasName),
                    columnRef.columnName);
            aggregateData.columnData = BufferManager.getData(filterColumnRef);
            aggregateData.tid = aggregate.aggInput.aliasIdxMentioned.iterator().next();
            aggregateDatas[cnt] = aggregateData;
            aggregateInfo.putIfAbsent(aggregateData.tid, new ArrayList<>());
            aggregateInfo.get(aggregateData.tid).add(cnt);
            sqLtypes[cnt] = CatalogManager.getColumn(filterColumnRef).type;
            cnt++;
        }

		int[] joinResult = new int[aggregateDatas.length];
		Arrays.fill(joinResult, -1);
		TrieManager manager = new TrieManager(query, context, varOrder);
		JoinProcessor.manager = manager;


		List<JoinProcessor> tasks = new ArrayList<>();
		long totalCombinations = manager.getTotalCombinations();
		System.out.println("Total combinations to process: " + totalCombinations);

		JoinProcessor.static_init(query, context, varOrder, aggregateDatas, aggregateInfo);
		

		// join tasks
		for (int i = 0; i < ThreadPool.THREAD; i++) {
			tasks.add(new JoinProcessor(i));
		}

		List<Future<int[]>> evaluateResults = ThreadPool.executorService.invokeAll(tasks);
		
		for(Future<int[]> queryResult: evaluateResults) {
			JoinProcessor.refresh_result(joinResult, queryResult.get());
		}

		return joinResult;
	}
}