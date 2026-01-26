package server;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
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
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import java.util.HashMap;

public class WebServer {
    private HttpServer server;
    private int port;
    
    public WebServer(int port) {
        this.port = port;
    }
    
    public void start() {
        try {
            // 创建服务
            server = HttpServer.create(new InetSocketAddress(port), 0);
            
            // 唯一的接口：查询
            server.createContext("/query", new QueryHandler());
            
            server.setExecutor(Executors.newCachedThreadPool());
            server.start();
            
            System.out.println("PJDL Server running on port " + port);
            
        } catch (Exception e) {
            System.err.println("Server start failed: " + e.getMessage());
        }
    }
    
    public void stop() {
        if (server != null) server.stop(0);
    }
    
    // 处理 SQL 查询
    private static class QueryHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            // CORS 头 (允许前端跨域访问)
            exchange.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
            exchange.getResponseHeaders().set("Access-Control-Allow-Methods", "GET, OPTIONS");
            
            if ("OPTIONS".equalsIgnoreCase(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(200, -1);
                return;
            }
            
            String responseStr;
            try {
                String queryURI = exchange.getRequestURI().getQuery();
                String sql = extractSqlParam(queryURI);
                
                if (sql == null || sql.trim().isEmpty()) {
                    throw new IllegalArgumentException("SQL is empty");
                }

                System.out.println("Executing: " + sql);
                
                // 执行查询并获取结果
                // 注意：确保你的 SkinnerCmd.processSQL 会把结果存入 SkinnerCmd.result
                Statement stmt = CCJSqlParserUtil.parse(sql);
                SkinnerCmd.processSQL(stmt.toString(), false); 
                responseStr = SkinnerCmd.result; 

            } catch (Exception e) {
                e.printStackTrace();
                // 返回错误 JSON
                responseStr = "{\"error\":\"" + e.getMessage().replace("\"", "'") + "\"}";
            }
            
            // 发送响应
            byte[] bytes = responseStr.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
            exchange.sendResponseHeaders(200, bytes.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(bytes);
            }
        }
        
        // 简单的参数提取工具
        private String extractSqlParam(String query) {
            if (query == null) return null;
            for (String param : query.split("&")) {
                String[] pair = param.split("=");
                if (pair.length == 2 && "sql".equals(pair[0])) {
                    try {
                        return URLDecoder.decode(pair[1], StandardCharsets.UTF_8.name());
                    } catch (Exception e) { return null; }
                }
            }
            return null;
        }
    }
    
    public static void main(String[] args) {
        // 初始化数据库 (保持你原有的逻辑)
        try {
            String SkinnerDbDir = "db_path"; // 修改为实际路径
            PathUtil.initSchemaPaths(SkinnerDbDir);
            CatalogManager.loadDB(PathUtil.schemaPath);
            PathUtil.initDataPaths(CatalogManager.currentDB);
            
            System.out.println("Loading data...");
            GeneralConfig.inMemory = true;
            BufferManager.loadDB();
            System.out.println("Data loaded.");
            
            Indexer.indexAll(StartupConfig.INDEX_CRITERIA);
            BaseTrie.orderCache = new HashMap<>();
            ThreadPool.initThreadsPool(ParallelConfig.EXE_THREADS, ParallelConfig.PRE_THREADS);
            SkinnerCmd.dbDir = SkinnerDbDir;
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }

        // 启动 Web 服务
        WebServer server = new WebServer(8080);
        server.start();
    }
}