package benchmark;

import static org.junit.jupiter.api.Assertions.fail;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import buffer.BufferManager;
import catalog.CatalogManager;
import config.GeneralConfig;
import config.ParallelConfig;
import config.StartupConfig;
import console.SkinnerCmd;
import diskio.PathUtil;
import indexing.Indexer;
import joining.BaseTrie;
import joining.parallel.threads.ThreadPool;
import net.sf.jsqlparser.statement.Statement;
import statistics.QueryStats;

/**
 * Benchmarks pre-, join, and post-processing stage and compares
 * the output sizes against the sizes of results produced by
 * Postgres.
 * 
 * @author immanueltrummer
 *
 */
public class BenchAndVerify {
	/**
	 * Processes all queries in given directory.
	 * 
	 * @param args	first argument is Skinner DB directory, 
	 * 				second argument is query directory
	 * 				third argument is Postgres database name
	 * 				fourth argument is Postgres user name
	 * 				fifth argument is Postgres user password
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		// Initialize database
		String SkinnerDbDir = args[0];
		String queryDir = args[1];
		Map<String, Statement> nameToQuery =
				BenchUtil.readAllQueries(queryDir);



		PathUtil.initSchemaPaths(SkinnerDbDir);
		CatalogManager.loadDB(PathUtil.schemaPath);
		PathUtil.initDataPaths(CatalogManager.currentDB);
		System.out.println("Loading data ...");
		GeneralConfig.inMemory = true;
		BufferManager.loadDB();
		System.out.println("Data loaded.");

		BaseTrie.orderCache = new HashMap<>();

		Indexer.indexAll(StartupConfig.INDEX_CRITERIA);

		
		long start_time = System.currentTimeMillis();
		
		ThreadPool.initThreadsPool(ParallelConfig.EXE_THREADS, ParallelConfig.PRE_THREADS);
		SkinnerCmd.dbDir = SkinnerDbDir;
		for (Entry<String, Statement> entry : nameToQuery.entrySet()) {
			String queryName = entry.getKey();
			System.out.println("---------------" + queryName + "---------------");
			// System.setOut(mute);
			Statement query = entry.getValue();
			QueryStats.queryName = queryName;
			long startMillis = System.currentTimeMillis();
			SkinnerCmd.processSQL(query.toString(), false);
			long totalMillis = System.currentTimeMillis() - startMillis;
			System.out.println("query time:" + totalMillis);
			BufferManager.unloadTempData();
			CatalogManager.removeTempTables();
		}

		System.out.println("======== END ========");
		System.out.println("TOtal cost: " + (System.currentTimeMillis() - start_time) + "ms.");
		ThreadPool.close();
	}
}
