package console;

import java.io.File;
import java.util.Scanner;
import java.util.regex.Pattern;

import buffer.BufferManager;
import catalog.CatalogManager;
import catalog.info.TableInfo;
import compression.Compressor;
import config.GeneralConfig;
import config.StartupConfig;
import ddl.TableCreator;
import diskio.LoadCSV;
import diskio.PathUtil;
import indexing.Indexer;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.drop.Drop;
import query.SQLexception;

/**
 * Runs Skinner command line console.
 * 
 * @author immanueltrummer
 *
 */
public class AVPJoinCmd {
	/**
	 * Path to database directory.
	 */
	static String dbDir;

	/**
	 * Checks whether file exists and displays
	 * error message if not. Returns true iff
	 * the file exists.
	 * 
	 * @param filePath check for file at that location
	 * @return true iff the file exists
	 */
	static boolean fileOrError(String filePath) {
		if ((new File(filePath)).exists()) {
			return true;
		} else {
			System.out.println("Error - input file at " +
					filePath + " does not exist");
			return false;
		}
	}

	/**
	 * Processes a command for loading data from a CSV file on disk.
	 * 
	 * @param input input command
	 * @throws Exception
	 */
	static void processLoadCmd(String input) throws Exception {
		// Load data from file into table
		String[] inputFrags = input.split("\\s");
		if (inputFrags.length != 5) {
			System.out.println("Error - specify table name, "
					+ "path to .csv file, separator, and null "
					+ "value representation, "
					+ "separated by spaces.");
		} else {
			// Retrieve schema information on table
			String tableName = inputFrags[1];
			TableInfo table = CatalogManager.currentDB.nameToTable.get(tableName);
			// Does the table exist?
			if (table == null) {
				System.out.println("Error - cannot find table " + tableName);
			} else {
				String csvPath = inputFrags[2];
				// Does input path exist?
				if (fileOrError(csvPath)) {
					String separatorStr = inputFrags[3];
					if (separatorStr.length() != 1) {
						System.out.println("Inadmissible separator: " +
								separatorStr + " (requires one character)");
					} else {
						char separator = separatorStr.charAt(0);
						String nullRepresentation = inputFrags[4];
						LoadCSV.load(csvPath, table,
								separator, nullRepresentation);
					}
				}
			}
		}
	}

	/**
	 * Processes SQL commands in specified file.
	 * 
	 * @param input input string for script command
	 * @throws Exception
	 */
	static void processFile(String input) throws Exception {
		// Check whether right parameters specified
		String[] inputFrags = input.split("\\s");
		if (inputFrags.length != 2) {
			System.err.println("Error - specify script path");
		} else {
			String path = inputFrags[1];
			// Verify whether input file exists
			if (fileOrError(path)) {
				Scanner scanner = new Scanner(new File(path));
				scanner.useDelimiter(Pattern.compile(";"));
				while (scanner.hasNext()) {
					String sqlCmd = scanner.next().trim();
					try {
						System.out.println("Processing statement '" + sqlCmd + "'");
						processInput(sqlCmd);
					} catch (Exception e) {
						System.err.println("Error processing command " + sqlCmd);
						e.printStackTrace();
					}
				}
				scanner.close();
			}
		}
	}

	/**
	 * Process input string as SQL statement.
	 * 
	 * @param input    input text
	 * @param benchRun whether this is a benchmark run (query results
	 *                 are not printed for benchmark runs)
	 * @throws Exception
	 */
	static void processSQL(String input, boolean benchRun) throws Exception {
		// Try parsing as SQL query
		Statement sqlStatement = null;
		try {
			sqlStatement = CCJSqlParserUtil.parse(input);
		} catch (Exception e) {
			System.out.println("Error in parsing SQL command");
			return;
		}
		// Distinguish statement type
		if (sqlStatement instanceof CreateTable) {
			TableInfo table = TableCreator.addTable(
					(CreateTable) sqlStatement);
			CatalogManager.currentDB.storeDB();
			System.out.println("Created " + table.toString());
		} else if (sqlStatement instanceof Drop) {
			Drop drop = (Drop) sqlStatement;
			String tableName = drop.getName().getName();
			// Verify that table to drop exists
			if (!CatalogManager.currentDB.nameToTable.containsKey(tableName)) {
				throw new SQLexception("Error - table " +
						tableName + " does not exist");
			}
			CatalogManager.currentDB.nameToTable.remove(tableName);
			CatalogManager.currentDB.storeDB();
			System.out.println("Dropped " + tableName);
		} else {
			System.out.println("Statement type " +
					sqlStatement.getClass().toString() +
					" not supported!");
		}
	}

	/**
	 * Executes input command, returns false iff
	 * the input was a termination command.
	 * 
	 * @param input input command to process
	 * @return false iff input was termination command
	 * @throws Exception
	 */
	static boolean processInput(String input) throws Exception {
		// Delete semicolons if any
		input = input.replace(";", "");
		// Determine input category
		if (input.equals("quit")) {
			// Terminate console
			return false;
		} else if (input.equals("compress")) {
			Compressor.compress();
		} else if (input.startsWith("exec")) {
			processFile(input);
		} else if (input.equals("index all")) {
			Indexer.indexAll(StartupConfig.INDEX_CRITERIA);
		} else if (input.equals("list")) {
			// Show overview of the database
			System.out.println(CatalogManager.currentDB.toString());
		} else if (input.startsWith("load ")) {
			processLoadCmd(input);
		} else if (input.isEmpty()) {
			// Nothing to do ...
		} else {
			try {
				processSQL(input, false);
			} catch (SQLexception e) {
				System.out.println(e.getMessage());
			}
		}
		return true;
	}

	/**
	 * Run Skinner console, using database schema
	 * at specified location.
	 * 
	 * @param args path to database directory
	 */
	public static void main(String[] args) throws Exception {
		// Verify number of command line arguments
		if (args.length != 1) {
			System.out.println("Error - specify the path"
					+ " to database directory!");
			return;
		}
		// Load database schema and initialize path mapping
		dbDir = args[0];
		PathUtil.initSchemaPaths(dbDir);
		CatalogManager.loadDB(PathUtil.schemaPath);
		PathUtil.initDataPaths(CatalogManager.currentDB);
		// Load data and/or dictionary
		if (GeneralConfig.inMemory) {
			// In-memory data processing
			BufferManager.loadDB();
		} else {
			// Disc data processing (not fully implemented!) -
			// string dictionary is still loaded.
			BufferManager.loadDictionary();
		}
		// Command line processing
		System.out.println("Enter 'help' for help and 'quit' to exit");
		Scanner scanner = new Scanner(System.in);
		boolean continueProcessing = true;
		while (continueProcessing) {
			System.out.print("> ");
			String input = scanner.nextLine();
			try {
				continueProcessing = processInput(input);
			} catch (Exception e) {
				System.err.println("Error processing command: ");
				e.printStackTrace();
			}
		}
		scanner.close();
	}
}
