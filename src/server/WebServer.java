package server;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.concurrent.Executors;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

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
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;

public class WebServer {
    private HttpServer server;
    private int port;
    
    public WebServer(int port) {
        this.port = port;
    }
    
    public void start() {
        try {
            server = HttpServer.create(new InetSocketAddress(port), 0);
            server.createContext("/query", new SimpleQueryHandler());
            server.setExecutor(Executors.newCachedThreadPool());
            server.start();
            
            System.out.println("WebServer started on port " + port);
            System.out.println("GET endpoint: http://localhost:" + port + "/query?sql=YOUR_SQL_HERE");
            
        } catch (Exception e) {
            System.err.println("Failed to start WebServer: " + e.getMessage());
        }
    }
    
    public void stop() {
        if (server != null) {
            server.stop(0);
            System.out.println("WebServer stopped");
        }
    }
    
    private static class SimpleQueryHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            // 设置CORS头
            exchange.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
            exchange.getResponseHeaders().set("Access-Control-Allow-Methods", "GET, OPTIONS");
            exchange.getResponseHeaders().set("Access-Control-Allow-Headers", "Content-Type");
            
            // 处理预检请求
            if ("OPTIONS".equalsIgnoreCase(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(200, -1);
                exchange.getResponseBody().close();
                return;
            }
            
            if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
                sendError(exchange, 405, "Only GET method is allowed");
                return;
            }
            
            String query = exchange.getRequestURI().getQuery();
            if (query == null || !query.contains("sql=")) {
                sendError(exchange, 400, "Missing sql parameter");
                return;
            }
            
            String sql = extractSql(query);
            if (sql == null || sql.trim().isEmpty()) {
                sendError(exchange, 400, "Empty SQL query");
                return;
            }
            
            String jsonResponse = processSqlQuery(sql);
            try {
                BufferManager.unloadTempData();
                CatalogManager.removeTempTables();
            } catch (Exception e) {
                e.printStackTrace();
            }
            
            
            sendResponse(exchange, 200, jsonResponse);
        }
        
        private String extractSql(String query) {
            try {
                String[] pairs = query.split("&");
                for (String pair : pairs) {
                    int idx = pair.indexOf("=");
                    if (idx > 0 && "sql".equals(pair.substring(0, idx))) {
                        return URLDecoder.decode(pair.substring(idx + 1), StandardCharsets.UTF_8.name());
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        }

        
        private String processSqlQuery(String sql) {
            // 这里替换为您的实际查询逻辑
            // 示例实现 - 返回测试数据
            try {

                Statement query = CCJSqlParserUtil.parse(sql);
                SkinnerCmd.processSQL(query.toString(), false);
                return SkinnerCmd.result;
            } catch (Exception e) {
                return "{\"error\":\"Query execution failed: " + e.getMessage() + "\"}";
            }
        }
        
        private void sendResponse(HttpExchange exchange, int statusCode, String response) throws IOException {
            exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
            byte[] responseBytes = response.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(statusCode, responseBytes.length);
            
            OutputStream os = exchange.getResponseBody();
            os.write(responseBytes);
            os.close();
        }
        
        private void sendError(HttpExchange exchange, int statusCode, String message) throws IOException {
            String errorResponse = "{\"error\":\"" + message + "\"}";
            sendResponse(exchange, statusCode, errorResponse);
        }
    }
    
    public static void main(String[] args) {
        try {
            String SkinnerDbDir = "C:\\Users\\n" + //
                                "gnoten\\Desktop\\avp-tpch\\ego-facebook";
            PathUtil.initSchemaPaths(SkinnerDbDir);
            CatalogManager.loadDB(PathUtil.schemaPath);
            PathUtil.initDataPaths(CatalogManager.currentDB);
            System.out.println("Loading data ...");
            GeneralConfig.inMemory = true;
            BufferManager.loadDB();
            System.out.println("Data loaded.");
            Indexer.indexAll(StartupConfig.INDEX_CRITERIA);
		    BaseTrie.orderCache = new HashMap<>();
            ThreadPool.initThreadsPool(ParallelConfig.EXE_THREADS, ParallelConfig.PRE_THREADS);
		    SkinnerCmd.dbDir = SkinnerDbDir;
        } catch (Exception e) {
            System.err.println("Failed to initialize database: " + e.getMessage());
            return;
        }
        

        WebServer server = new WebServer(8080);
        server.start();
        
        Runtime.getRuntime().addShutdownHook(new Thread(server::stop));
        
        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}