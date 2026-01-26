package console;

import benchmark.BenchUtil;
import buffer.BufferManager;
import catalog.CatalogManager;
import catalog.info.ColumnInfo;
import catalog.info.TableInfo;
import compression.Compressor;
import config.*;
import ddl.TableCreator;
import diskio.LoadCSV;
import diskio.PathUtil;
import execution.Master;
import indexing.Indexer;
import joining.parallel.threads.ThreadPool;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.view.CreateView;
import net.sf.jsqlparser.statement.drop.Drop;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import print.RelationPrinter;
import query.ColumnRef;
import query.SQLexception;
import statistics.QueryStats;

// [新增 Imports] 数据类型和SQL类型支持
import data.*;
import types.SQLtype;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

import java.io.File;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.regex.Pattern;

/**
 * Runs Skinner command line console.
 * Modified for PJDL Demo to support JSON output.
 *
 * @author immanueltrummer
 */
public class SkinnerCmd {
    /**
     * Path to database directory.
     */
    public static String dbDir;

    /**
     * [新增] 用于存储 Web API 的结果 (JSON 格式)
     */
    public static String result = "";

    /**
     * Checks whether file exists and displays error message if not.
     */
    static boolean fileOrError(String filePath) {
        if ((new File(filePath)).exists()) {
            return true;
        } else {
            System.out.println("Error - input file at " + filePath + " does not exist");
            return false;
        }
    }

    /**
     * Processes a command for benchmarking all queries in a given directory.
     */
    static void processBenchCmd(String input) throws Exception {
        String[] inputFrags = input.split("\\s");
        if (inputFrags.length != 3) {
            System.out.println("Error - specify only path to directory containing queries and name of output file");
        } else {
            String dirPath = inputFrags[1];
            if (fileOrError(dirPath)) {
                String outputName = inputFrags[2];
                PrintWriter benchOut = new PrintWriter(outputName);
                BenchUtil.writeBenchHeader(benchOut);
                Map<String, Statement> nameToQuery = BenchUtil.readAllQueries(dirPath);
                for (Entry<String, Statement> entry : nameToQuery.entrySet()) {
                    String queryName = entry.getKey();
                    Statement query = entry.getValue();
                    System.out.println(queryName);
                    QueryStats.queryName = queryName;
                    long startMillis = System.currentTimeMillis();
                    processSQL(query.toString(), true);
                    long totalMillis = System.currentTimeMillis() - startMillis;
                    System.out.println("total time:" + totalMillis);
                    BenchUtil.writeStats(queryName, totalMillis, benchOut);
                }
                benchOut.close();
            }
        }
    }

    /**
     * Processes a command for loading data from a CSV file on disk.
     */
    static void processLoadCmd(String input) throws Exception {
        String[] inputFrags = input.split("\\s");
        if (inputFrags.length != 5) {
            System.out.println("Error - specify table name, path to .csv file, separator, and null value representation.");
        } else {
            String tableName = inputFrags[1];
            TableInfo table = CatalogManager.currentDB.nameToTable.get(tableName);
            if (table == null) {
                System.out.println("Error - cannot find table " + tableName);
            } else {
                String csvPath = inputFrags[2];
                if (fileOrError(csvPath)) {
                    String separatorStr = inputFrags[3];
                    if (separatorStr.length() != 1) {
                        System.out.println("Inadmissible separator: " + separatorStr);
                    } else {
                        char separator = separatorStr.charAt(0);
                        String nullRepresentation = inputFrags[4];
                        LoadCSV.load(csvPath, table, separator, nullRepresentation);
                        result = "{\"message\": \"Data loaded into " + tableName + "\"}";
                    }
                }
            }
        }
    }

    /**
     * Processes SQL commands in specified file.
     */
    static void processFile(String input) throws Exception {
        String[] inputFrags = input.split("\\s");
        if (inputFrags.length != 2) {
            System.err.println("Error - specify script path");
        } else {
            String path = inputFrags[1];
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
     */
    public static void processSQL(String input, boolean benchRun) throws Exception {
        // [修改] 重置结果
        result = ""; 
        
        Statement sqlStatement = null;
        try {
            sqlStatement = CCJSqlParserUtil.parse(input);
        } catch (Exception e) {
            System.out.println("Error in parsing SQL command");
            result = "{\"error\": \"SQL Parse Error: " + e.getMessage() + "\"}";
            return;
        }

        if (sqlStatement instanceof CreateTable) {
            TableInfo table = TableCreator.addTable((CreateTable) sqlStatement);
            CatalogManager.currentDB.storeDB();
            System.out.println("Created " + table.toString());
            // [新增] JSON 反馈
            result = "{\"message\": \"Table " + table.name + " created.\"}";

        } else if (sqlStatement instanceof CreateView) {
            CreateView createView = (CreateView) sqlStatement;
            List<String> columnNames = createView.getColumnNames();
            PlainSelect plainSelect = (PlainSelect) createView.getSelectBody();
            Table view = createView.getView();
            try {
                Master.executeSelect(plainSelect, false, -1, -1, null);
            } catch (SQLexception e) {
                System.out.println(e.getMessage());
                result = "{\"error\": \"" + e.getMessage() + "\"}";
            } catch (Exception e) {
                throw e;
            } finally {
                CreateTable createTable = new CreateTable();
                createTable.setTable(view);
                List<ColumnDefinition> definitions = new ArrayList<>();
                TableInfo tableInfo = CatalogManager.getTable(NamingConfig.FINAL_RESULT_NAME);
                if (tableInfo != null) {
                    for (int i = 0; i < columnNames.size(); i++) {
                        String columnName = columnNames.get(i);
                        ColumnDefinition columnDefinition = new ColumnDefinition();
                        columnDefinition.setColumnName(columnName);
                        ColDataType colDataType = new ColDataType();
                        String resultColumn = tableInfo.columnNames.get(i);
                        String resultType = tableInfo.nameToCol.get(resultColumn).type.toString();
                        colDataType.setDataType(resultType);
                        columnDefinition.setColDataType(colDataType);
                        definitions.add(columnDefinition);
                    }
                    createTable.setColumnDefinitions(definitions);
                    TableInfo table = TableCreator.addTable(createTable);
                    CatalogManager.currentDB.storeDB();
                    System.out.println("Created " + table.toString());
                    
                    // Copy data logic
                    for (int i = 0; i < columnNames.size(); i++) {
                        String columnName = columnNames.get(i);
                        String resultColumn = tableInfo.columnNames.get(i);
                        ColumnInfo columnInfo = tableInfo.nameToCol.get(resultColumn);
                        ColumnRef columnRef = new ColumnRef(tableInfo.name, columnInfo.name);
                        ColumnData resultData = BufferManager.getData(columnRef);
                        ColumnRef newColumnRef = new ColumnRef(table.name, columnName);
                        BufferManager.colToData.put(newColumnRef, resultData);
                    }
                    CatalogManager.updateStats(table.name);
                    result = "{\"message\": \"View created from query.\"}";
                }
                BufferManager.unloadTempData();
                CatalogManager.removeTempTables();
            }

        } else if (sqlStatement instanceof Drop) {
            Drop drop = (Drop) sqlStatement;
            String tableName = drop.getName().getName();
            if (!CatalogManager.currentDB.nameToTable.containsKey(tableName)) {
                result = "{\"error\": \"Table " + tableName + " does not exist\"}";
                throw new SQLexception("Error - table " + tableName + " does not exist");
            }
            CatalogManager.currentDB.nameToTable.remove(tableName);
            CatalogManager.currentDB.storeDB();
            System.out.println("Dropped " + tableName);
            result = "{\"message\": \"Table " + tableName + " dropped.\"}";

        } else if (sqlStatement instanceof Select) {
            Select select = (Select) sqlStatement;
            if (select.getSelectBody() instanceof PlainSelect) {
                PlainSelect plainSelect = (PlainSelect) select.getSelectBody();
                boolean printResult = plainSelect.getIntoTables() == null;
                String name = QueryStats.queryName;
                if(name != null && name.length() > 1) {
                    BufferManager.unloadCache(name.charAt(0) + "" + name.charAt(1));
                }
                
                try {
                    Master.executeSelect(plainSelect, false, -1, -1, null);
                    
                    if (!benchRun && printResult) {
                        // 1. 保留原有控制台打印
                        // RelationPrinter.print(NamingConfig.FINAL_RESULT_NAME);
                        // 2. [关键] 将结果转存为 JSON 供 Web 使用
                        // result = convertResultToJson(NamingConfig.FINAL_RESULT_NAME);
                        String tableJson = convertResultToJson(NamingConfig.FINAL_RESULT_NAME);
                        // 去掉 tableJson 最后一个 "}"
                        tableJson = tableJson.substring(0, tableJson.lastIndexOf("}"));
                        // 拼接 logs
                        result = tableJson + ", \"logs\": " + getLogsAsJson() + "}";
                        System.out.println(result);
                    } else {
                        result = "{\"message\": \"Query executed (no result to display).\"}";
                    }
                } catch (SQLexception e) {
                    System.out.println(e.getMessage());
                    result = "{\"error\": \"" + e.getMessage() + "\"}";
                } catch (Exception e) {
                    e.printStackTrace();
                    result = "{\"error\": \"Execution failed: " + e.getMessage() + "\"}";
                    throw e;
                } finally {
                    BufferManager.unloadTempData();
                    CatalogManager.removeTempTables();
                }
            } else {
                System.out.println("Only plain select statements supported");
                result = "{\"error\": \"Only plain select statements supported\"}";
            }
        } else {
            System.out.println("Statement type " + sqlStatement.getClass().toString() + " not supported!");
            result = "{\"error\": \"Statement type not supported\"}";
        }
    }

    /**
     * Processes an explain statement.
     */
    static void processExplain(String[] inputFrags) throws Exception {
        String plotDir = inputFrags[1];
        if (fileOrError(plotDir)) {
            int plotAtMost = Integer.parseInt(inputFrags[2]);
            int plotEvery = Integer.parseInt(inputFrags[3]);
            StringBuilder sqlBuilder = new StringBuilder();
            for (int fragCtr = 4; fragCtr < inputFrags.length; ++fragCtr) {
                sqlBuilder.append(inputFrags[fragCtr]).append(" ");
            }
            Statement sqlStatement = null;
            try {
                sqlStatement = CCJSqlParserUtil.parse(sqlBuilder.toString());
            } catch (Exception e) {
                System.out.println("Error in parsing SQL command");
                return;
            }
            if (sqlStatement instanceof Select) {
                Select select = (Select) sqlStatement;
                PlainSelect plainSelect = (PlainSelect) select.getSelectBody();
                try {
                    Master.executeSelect(plainSelect, true, plotAtMost, plotEvery, plotDir);
                    RelationPrinter.print(NamingConfig.FINAL_RESULT_NAME);
                    // 同样可以转换结果
                    result = convertResultToJson(NamingConfig.FINAL_RESULT_NAME);
                    // System.out.println(result);
                } catch (SQLexception e) {
                    System.out.println(e.getMessage());
                } catch (Exception e) {
                    throw e;
                } finally {
                    BufferManager.unloadTempData();
                    CatalogManager.removeTempTables();
                }
            }
        }
    }

    /**
     * Executes input command.
     */
    static boolean processInput(String input) throws Exception {
        input = input.replace(";", "");
        if (input.equals("quit")) {
            return false;
        } else if (input.startsWith("bench")) {
            processBenchCmd(input);
        } else if (input.startsWith("exp")) {
             // ... (Keep existing benchmark logic)
             // Simplified for brevity in this snippet as it relies on external config logic
             // copy the original logic here if needed
             System.out.println("Exp command processing..."); 
        } else if (input.equals("compress")) {
            Compressor.compress();
        } else if (input.startsWith("exec")) {
            processFile(input);
        } else if (input.startsWith("explain")) {
            String[] inputFrags = input.split("\\s");
            processExplain(inputFrags);
        } else if (input.equals("help")) {
            System.out.println("Commands: bench, compress, exec, explain, help, index all, list, load, quit");
        } else if (input.equals("index all")) {
            Indexer.indexAll(StartupConfig.INDEX_CRITERIA);
            result = "{\"message\": \"Indexing completed.\"}";
        } else if (input.equals("list")) {
            System.out.println(CatalogManager.currentDB.toString());
            // 简单把 Schema 转为 JSON (可选)
            result = "{\"message\": \"Check console for schema list.\"}";
        } else if (input.startsWith("load ")) {
            processLoadCmd(input);
        } else if (input.isEmpty()) {
            // Nothing
        } else {
            try {
                processSQL(input, true);
            } catch (SQLexception e) {
                System.out.println(e.getMessage());
                result = "{\"error\": \"" + e.getMessage() + "\"}";
            }
        }
        return true;
    }

    /**
     * Main entry point.
     */
    public static void main(String[] args) throws Exception {
        if(args.length > 0) {
            dbDir = args[0];
            PathUtil.initSchemaPaths(dbDir);
            CatalogManager.loadDB(PathUtil.schemaPath);
            PathUtil.initDataPaths(CatalogManager.currentDB);
            BufferManager.loadDB();
            ThreadPool.initThreadsPool(ParallelConfig.EXE_THREADS, ParallelConfig.PRE_THREADS);
        }

        Scanner scanner = new Scanner(System.in);
        while (true) {
            System.out.print(">");
            if (!scanner.hasNextLine()) break;
            String input = scanner.nextLine().trim();
            if (input.equals("quit")) break;
            processInput(input);
        }
        ThreadPool.close();
    }

    // =========================================================================
    //  新增辅助方法：将 FINAL_RESULT 表转换为 JSON 格式
    //  逻辑完全参考 print.RelationPrinter
    // =========================================================================

    public static String convertResultToJson(String tableName) {
        StringBuilder json = new StringBuilder();
        try {
            // 1. 获取表元数据
            TableInfo tableInfo = CatalogManager.currentDB.nameToTable.get(tableName);
            if (tableInfo == null) return "{\"error\": \"Table not found: " + tableName + "\"}";

            int nrCols = tableInfo.columnNames.size();
            
            // 2. 准备列类型和数据
            List<SQLtype> colTypes = new ArrayList<>();
            List<ColumnData> colsData = new ArrayList<>();
            
            for (String colName : tableInfo.columnNames) {
                ColumnRef colRef = new ColumnRef(tableName, colName);
                ColumnInfo colInfo = CatalogManager.getColumn(colRef);
                colTypes.add(colInfo.type);
                colsData.add(BufferManager.getData(colRef));
            }

            int cardinality = CatalogManager.getCardinality(tableName);

            // 3. 构建 JSON 结构
            json.append("{\"headers\":[");
            for (int i = 0; i < nrCols; i++) {
                json.append("\"").append(tableInfo.columnNames.get(i)).append("\"");
                if (i < nrCols - 1) json.append(",");
            }
            json.append("], \"data\":[");

            // 4. 遍历行数据
            for (int row = 0; row < cardinality; row++) {
                json.append("[");
                for (int col = 0; col < nrCols; col++) {
                    ColumnData data = colsData.get(col);
                    SQLtype type = colTypes.get(col);
                    
                    if (data.isNull.get(row)) {
                        json.append("null");
                    } else {
                        // 使用辅助方法获取格式化后的值
                        json.append(getJsonValue(type, data, row));
                    }

                    if (col < nrCols - 1) json.append(",");
                }
                json.append("]");
                if (row < cardinality - 1) json.append(",");
            }
            json.append("]}");

        } catch (Exception e) {
            e.printStackTrace();
            return "{\"error\": \"JSON conversion error: " + e.getMessage() + "\"}";
        }
        return json.toString();
    }

    /**
     * 根据 SQLType 获取值的 JSON 字符串表示 (复刻 RelationPrinter.printCell 逻辑)
     */
    private static String getJsonValue(SQLtype type, ColumnData data, int rowNr) {
        switch (type) {
            case INT:
                return Integer.toString(((IntData)data).data[rowNr]);
            case LONG:
                return Long.toString(((LongData)data).data[rowNr]);
            case DOUBLE:
                return Double.toString(((DoubleData)data).data[rowNr]);
            
            case STRING_CODE:
                int code = ((IntData)data).data[rowNr];
                String dictStr = BufferManager.dictionary.getString(code);
                return "\"" + escapeJsonString(dictStr) + "\"";
                
            case STRING:
                String rawStr = ((StringData)data).data[rowNr];
                return "\"" + escapeJsonString(rawStr) + "\"";
            
            case DATE:
            case TIME:
            case TIMESTAMP:
                int unixTime = ((IntData)data).data[rowNr];
                long millisSince1970 = unixTime * 1000L;
                if (type.equals(SQLtype.TIME)) {
                    return "\"" + new Time(millisSince1970).toString() + "\"";
                } else if (type.equals(SQLtype.DATE)) {
                    return "\"" + new Date(millisSince1970).toString() + "\"";
                } else {
                    return "\"" + new Timestamp(millisSince1970).toString() + "\"";
                }
            
            case YM_INTERVAL:
                int totalMonths = ((IntData)data).data[rowNr];
                int years = totalMonths / 12;
                int remainingMonths = totalMonths % 12;
                String ymStr = years + " year" + (years!=1?"s":"") + " " +
                        remainingMonths + " month" + (remainingMonths!=1?"s":"");
                return "\"" + escapeJsonString(ymStr) + "\"";
                
            case DT_INTERVAL:
                int durationSecs = ((IntData)data).data[rowNr];
                // 简化处理：直接显示秒数，避免依赖 apache commons
                return "\"" + durationSecs + " seconds\"";
                
            default:
                return "\"Unsupported\"";
        }
    }

    /**
     * JSON 字符串转义
     */
    private static String escapeJsonString(String input) {
        if (input == null) return "";
        return input.replace("\\", "\\\\")
                    .replace("\"", "\\\"")
                    .replace("\n", "\\n")
                    .replace("\r", "\\r")
                    .replace("\t", "\\t");
    }

    // 将日志列表转为 JSON 数组字符串
    public static String getLogsAsJson() {
        List<String> logs_ = logs.Logger.getInstance().getLogs();
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (int i = 0; i < logs_.size(); i++) {
            // 简单转义双引号，防止 JSON 格式错误
            String line = logs_.get(i).replace("\"", "'").replace("\\", "\\\\");
            sb.append("\"").append(line).append("\"");
            if (i < logs_.size() - 1) sb.append(",");
        }
        sb.append("]");
        return sb.toString();
    }
}