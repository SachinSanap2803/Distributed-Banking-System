import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * WebDashboard.java
 * High-performance, zero-dependency Web GUI for the Distributed Banking System.
 *
 * Built directly on standard JDK HttpServer (com.sun.net.httpserver.*) and Java RMI.
 * Features:
 * - Real-time animated cluster topology (3 Branch Nodes)
 * - Bully Algorithm Leader visualizer with dynamic Coordinator badge
 * - Live Lamport Clock monitors on each branch and global causal order table
 * - One-click Deposit, Withdraw, and Cross-Branch Transfer forms
 * - Node crash & revive fault-tolerance simulator
 * - 10-thread simultaneous concurrency & race-condition benchmark runner
 */
public class WebDashboard {

    private static final int HTTP_PORT = 8080;
    private static final int RMI_PORT = 1099;
    private static final List<Integer> CLUSTER_NODE_IDS = Arrays.asList(1, 2, 3);

    private static Registry rmiRegistry;
    private static final Map<Integer, BankNode> localManagedNodes = new ConcurrentHashMap<>();
    private static final LamportClock dashboardClock = new LamportClock(1);

    public static void main(String[] args) {
        try {
            int httpPort = args.length > 0 ? Integer.parseInt(args[0]) : HTTP_PORT;

            // Connect to or initialize RMI Registry
            try {
                rmiRegistry = LocateRegistry.createRegistry(RMI_PORT);
                System.out.printf("[RMI] Embedded registry started on port %d.%n", RMI_PORT);
            } catch (RemoteException e) {
                rmiRegistry = LocateRegistry.getRegistry("localhost", RMI_PORT);
                System.out.printf("[RMI] Connected to existing registry on port %d.%n", RMI_PORT);
            }

            // Auto-bootstrap any unstarted nodes so dashboard works instantly out of the box
            bootstrapClusterIfEmpty();

            // Start HTTP Server
            HttpServer server = HttpServer.create(new InetSocketAddress(httpPort), 0);
            server.createContext("/", new StaticFileHandler());
            server.createContext("/api/state", new StateApiHandler());
            server.createContext("/api/logs", new LogsApiHandler());
            server.createContext("/api/transaction", new TransactionApiHandler());
            server.createContext("/api/election", new ElectionApiHandler());
            server.createContext("/api/kill", new KillNodeApiHandler());
            server.createContext("/api/revive", new ReviveNodeApiHandler());
            server.createContext("/api/benchmark", new BenchmarkApiHandler());

            server.setExecutor(Executors.newCachedThreadPool());
            server.start();

            System.out.println("=================================================================");
            System.out.println("  DISTRIBUTED BANKING SYSTEM - WEB DASHBOARD READY!");
            System.out.println("=================================================================");
            System.out.printf("  Dashboard URL: http://localhost:%d%n", httpPort);
            System.out.println("  Serving RMI Cluster on Port 1099 (Nodes 1, 2, 3)");
            System.out.println("  Open http://localhost:" + httpPort + " in your browser to interact!");
            System.out.println("=================================================================");

        } catch (Exception e) {
            System.err.println("Fatal error starting WebDashboard: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Checks if nodes 1, 2, 3 are bound in RMI. If not, boots them so the GUI works immediately.
     */
    private static synchronized void bootstrapClusterIfEmpty() {
        for (int id : CLUSTER_NODE_IDS) {
            try {
                rmiRegistry.lookup("BankNode_" + id);
            } catch (Exception e) {
                // Not bound -> instantiate
                try {
                    String name = (id == 1) ? "Downtown Branch" : (id == 2) ? "Uptown Branch" : "Metro Branch";
                    BankNode node = new BankNode(id, name, RMI_PORT, CLUSTER_NODE_IDS);
                    rmiRegistry.rebind("BankNode_" + id, node);
                    localManagedNodes.put(id, node);
                    System.out.printf("[Bootstrap] Spawned and bound BankNode %d (%s) into RMI Registry.%n", id, name);
                } catch (Exception ex) {
                    System.err.printf("[Bootstrap Error] Failed to launch Node %d: %s%n", id, ex.getMessage());
                }
            }
        }
    }

    private static BankInterface getNode(int nodeId) {
        try {
            return (BankInterface) rmiRegistry.lookup("BankNode_" + nodeId);
        } catch (Exception e) {
            return null;
        }
    }

    // =========================================================================
    //  REST API Handlers
    // =========================================================================

    /**
     * GET /api/state - Returns active cluster topology, node health, accounts, and leader.
     */
    static class StateApiHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
                sendResponse(exchange, 405, "text/plain", "Method Not Allowed");
                return;
            }

            int activeLeader = -1;
            StringBuilder nodesJson = new StringBuilder("[");

            for (int i = 0; i < CLUSTER_NODE_IDS.size(); i++) {
                int id = CLUSTER_NODE_IDS.get(i);
                BankInterface node = getNode(id);

                if (i > 0) nodesJson.append(",");

                if (node != null) {
                    try {
                        int leader = node.getCurrentLeaderId();
                        if (leader > activeLeader) activeLeader = leader;
                        int lClock = node.getLamportTime();
                        String branchName = node.getBranchName();

                        // Query account balances
                        String acc1 = "ACC-" + id + "01";
                        String acc2 = "ACC-" + id + "02";
                        double b1 = node.getBalance(acc1, dashboardClock.tick());
                        double b2 = node.getBalance(acc2, dashboardClock.tick());

                        String user1 = (id == 1) ? "Alice Downtown" : (id == 2) ? "Charlie Uptown" : "Evan Metro";
                        String user2 = (id == 1) ? "Bob Downtown" : (id == 2) ? "Diana Uptown" : "Fiona Metro";

                        nodesJson.append(String.format(
                                "{\"nodeId\":%d,\"name\":\"%s\",\"online\":true,\"lamportTime\":%d,\"leaderId\":%d,\"isLeader\":%b,\"accounts\":[{\"id\":\"%s\",\"holder\":\"%s\",\"balance\":%.2f},{\"id\":\"%s\",\"holder\":\"%s\",\"balance\":%.2f}]}",
                                id, branchName, lClock, leader, (id == leader), acc1, user1, b1, acc2, user2, b2
                        ));
                    } catch (RemoteException re) {
                        nodesJson.append(String.format("{\"nodeId\":%d,\"name\":\"Branch-%d\",\"online\":false}", id, id));
                    }
                } else {
                    nodesJson.append(String.format("{\"nodeId\":%d,\"name\":\"Branch-%d\",\"online\":false}", id, id));
                }
            }
            nodesJson.append("]");

            String json = String.format("{\"activeLeader\":%d,\"nodes\":%s}", activeLeader, nodesJson.toString());
            sendResponse(exchange, 200, "application/json", json);
        }
    }

    /**
     * GET /api/logs - Returns all transactions sorted by Lamport Causal Order.
     */
    static class LogsApiHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
                sendResponse(exchange, 405, "text/plain", "Method Not Allowed");
                return;
            }

            List<Transaction> logs = Collections.emptyList();

            // Try getting global log from highest reachable node or coordinator
            for (int i = 3; i >= 1; i--) {
                BankInterface node = getNode(i);
                if (node != null) {
                    try {
                        logs = node.getGlobalTransactionLog();
                        if (!logs.isEmpty()) break;
                    } catch (Exception ignored) {}
                }
            }

            StringBuilder sb = new StringBuilder("[");
            for (int i = 0; i < logs.size(); i++) {
                Transaction tx = logs.get(i);
                if (i > 0) sb.append(",");
                sb.append(String.format(
                        "{\"txId\":\"%s\",\"type\":\"%s\",\"source\":\"%s\",\"target\":\"%s\",\"amount\":%.2f,\"lamport\":%d,\"originNode\":%d,\"status\":\"%s\",\"details\":\"%s\"}",
                        escapeJson(tx.getTransactionId()),
                        tx.getType(),
                        escapeJson(tx.getSourceAccount()),
                        escapeJson(tx.getTargetAccount()),
                        tx.getAmount(),
                        tx.getLamportTimestamp(),
                        tx.getOriginNodeId(),
                        escapeJson(tx.getStatus()),
                        escapeJson(tx.getDetails())
                ));
            }
            sb.append("]");

            sendResponse(exchange, 200, "application/json", sb.toString());
        }
    }

    /**
     * POST /api/transaction - Executes Deposit, Withdraw, or Transfer.
     */
    static class TransactionApiHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
                sendResponse(exchange, 405, "text/plain", "Method Not Allowed");
                return;
            }

            Map<String, String> params = parseQueryParams(exchange);
            String type = params.getOrDefault("type", "").toUpperCase();
            int originNode = Integer.parseInt(params.getOrDefault("originNode", "1"));
            String source = params.getOrDefault("source", "");
            String target = params.getOrDefault("target", "");
            double amount = Double.parseDouble(params.getOrDefault("amount", "0"));

            BankInterface node = getNode(originNode);
            if (node == null) {
                sendResponse(exchange, 400, "application/json", "{\"success\":false,\"error\":\"Origin branch node is offline!\"}");
                return;
            }

            try {
                int clientTime = dashboardClock.tick();
                Transaction resultTx;

                switch (type) {
                    case "DEPOSIT":
                        resultTx = node.deposit(target, amount, clientTime);
                        break;
                    case "WITHDRAW":
                        resultTx = node.withdraw(source, amount, clientTime);
                        break;
                    case "TRANSFER":
                        resultTx = node.transfer(source, target, amount, clientTime);
                        break;
                    default:
                        sendResponse(exchange, 400, "application/json", "{\"success\":false,\"error\":\"Invalid transaction type.\"}");
                        return;
                }

                dashboardClock.updateAndTick(resultTx.getLamportTimestamp());
                String json = String.format(
                        "{\"success\":true,\"status\":\"%s\",\"txId\":\"%s\",\"lamport\":%d,\"details\":\"%s\"}",
                        resultTx.getStatus(), resultTx.getTransactionId(), resultTx.getLamportTimestamp(), escapeJson(resultTx.getDetails())
                );
                sendResponse(exchange, 200, "application/json", json);

            } catch (Exception e) {
                sendResponse(exchange, 500, "application/json", "{\"success\":false,\"error\":\"" + escapeJson(e.getMessage()) + "\"}");
            }
        }
    }

    /**
     * POST /api/election - Manually triggers Bully Election on a specified node.
     */
    static class ElectionApiHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            Map<String, String> params = parseQueryParams(exchange);
            int nodeId = Integer.parseInt(params.getOrDefault("nodeId", "1"));

            BankNode localNode = localManagedNodes.get(nodeId);
            if (localNode != null) {
                CompletableFuture.runAsync(localNode::startBullyElection);
                sendResponse(exchange, 200, "application/json", "{\"success\":true,\"message\":\"Election triggered on Node " + nodeId + "\"}");
                return;
            }

            BankInterface remoteNode = getNode(nodeId);
            if (remoteNode != null) {
                try {
                    remoteNode.electLeader(0); // Trigger
                    sendResponse(exchange, 200, "application/json", "{\"success\":true,\"message\":\"Election signal sent to Node " + nodeId + "\"}");
                } catch (Exception e) {
                    sendResponse(exchange, 500, "application/json", "{\"success\":false,\"error\":\"" + escapeJson(e.getMessage()) + "\"}");
                }
            } else {
                sendResponse(exchange, 400, "application/json", "{\"success\":false,\"error\":\"Node " + nodeId + " is offline.\"}");
            }
        }
    }

    /**
     * POST /api/kill - Simulates crash of a node by unbinding it from RMI registry.
     */
    static class KillNodeApiHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            Map<String, String> params = parseQueryParams(exchange);
            int nodeId = Integer.parseInt(params.getOrDefault("nodeId", "3"));

            try {
                rmiRegistry.unbind("BankNode_" + nodeId);
                System.out.printf("[Simulated Failure] Unbound BankNode_%d from RMI Registry.%n", nodeId);
                sendResponse(exchange, 200, "application/json", "{\"success\":true,\"message\":\"Node " + nodeId + " crashed/unbound successfully.\"}");
            } catch (Exception e) {
                sendResponse(exchange, 500, "application/json", "{\"success\":false,\"error\":\"" + escapeJson(e.getMessage()) + "\"}");
            }
        }
    }

    /**
     * POST /api/revive - Restores a crashed node back into the cluster.
     */
    static class ReviveNodeApiHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            Map<String, String> params = parseQueryParams(exchange);
            int nodeId = Integer.parseInt(params.getOrDefault("nodeId", "3"));

            try {
                BankNode node = localManagedNodes.get(nodeId);
                if (node == null) {
                    String name = (nodeId == 1) ? "Downtown Branch" : (nodeId == 2) ? "Uptown Branch" : "Metro Branch";
                    node = new BankNode(nodeId, name, RMI_PORT, CLUSTER_NODE_IDS);
                    localManagedNodes.put(nodeId, node);
                }
                rmiRegistry.rebind("BankNode_" + nodeId, node);
                node.startBullyElection(); // Trigger discovery

                System.out.printf("[Revive] Rebound BankNode_%d into RMI Registry.%n", nodeId);
                sendResponse(exchange, 200, "application/json", "{\"success\":true,\"message\":\"Node " + nodeId + " revived and rejoined cluster.\"}");
            } catch (Exception e) {
                sendResponse(exchange, 500, "application/json", "{\"success\":false,\"error\":\"" + escapeJson(e.getMessage()) + "\"}");
            }
        }
    }

    /**
     * POST /api/benchmark - Executes 10-thread concurrency test on ACC-101.
     */
    static class BenchmarkApiHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            BankInterface node = getNode(1);
            if (node == null) {
                sendResponse(exchange, 400, "application/json", "{\"success\":false,\"error\":\"Branch Node 1 is offline.\"}");
                return;
            }

            int threads = 10;
            double initialBal;
            try {
                initialBal = node.getBalance("ACC-101", dashboardClock.tick());
            } catch (Exception e) {
                sendResponse(exchange, 500, "application/json", "{\"success\":false,\"error\":\"" + escapeJson(e.getMessage()) + "\"}");
                return;
            }

            ExecutorService pool = Executors.newFixedThreadPool(threads);
            CountDownLatch startGate = new CountDownLatch(1);
            CountDownLatch endGate = new CountDownLatch(threads);
            AtomicInteger successCount = new AtomicInteger(0);

            long startTime = System.currentTimeMillis();
            for (int i = 0; i < threads; i++) {
                final boolean isDeposit = (i % 2 == 0);
                pool.submit(() -> {
                    try {
                        startGate.await();
                        int cTime = dashboardClock.tick();
                        if (isDeposit) {
                            node.deposit("ACC-101", 50.00, cTime);
                        } else {
                            node.withdraw("ACC-101", 50.00, cTime);
                        }
                        successCount.incrementAndGet();
                    } catch (Exception ignored) {
                    } finally {
                        endGate.countDown();
                    }
                });
            }

            startGate.countDown();
            try {
                endGate.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException ignored) {}
            long duration = System.currentTimeMillis() - startTime;
            pool.shutdown();

            double finalBal = 0;
            try {
                finalBal = node.getBalance("ACC-101", dashboardClock.tick());
            } catch (Exception ignored) {}

            boolean consistent = Math.abs(finalBal - initialBal) < 0.001;
            String json = String.format(
                    "{\"success\":true,\"durationMs\":%d,\"operations\":%d,\"initialBalance\":%.2f,\"finalBalance\":%.2f,\"consistent\":%b}",
                    duration, successCount.get(), initialBal, finalBal, consistent
            );
            sendResponse(exchange, 200, "application/json", json);
        }
    }

    // =========================================================================
    //  Static HTML/JS Single-Page UI
    // =========================================================================

    static class StaticFileHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            byte[] htmlBytes = DASHBOARD_HTML.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "text/html; charset=UTF-8");
            exchange.sendResponseHeaders(200, htmlBytes.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(htmlBytes);
            }
        }
    }

    private static void sendResponse(HttpExchange exchange, int statusCode, String contentType, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", contentType + "; charset=UTF-8");
        exchange.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
        exchange.sendResponseHeaders(statusCode, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }

    private static Map<String, String> parseQueryParams(HttpExchange exchange) throws IOException {
        Map<String, String> map = new HashMap<>();
        String query = exchange.getRequestURI().getQuery();
        if (query == null || query.isEmpty()) {
            // Read from body if POST
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(exchange.getRequestBody(), StandardCharsets.UTF_8))) {
                StringBuilder body = new StringBuilder();
                String line;
                while ((line = reader.readLine()) != null) body.append(line);
                query = body.toString();
            }
        }
        if (query != null && !query.isEmpty()) {
            for (String pair : query.split("&")) {
                int idx = pair.indexOf("=");
                if (idx > 0) {
                    String key = URLDecoder.decode(pair.substring(0, idx), "UTF-8");
                    String val = URLDecoder.decode(pair.substring(idx + 1), "UTF-8");
                    map.put(key, val);
                }
            }
        }
        return map;
    }

    private static String escapeJson(String s) {
        if (s == null) return "";
        return s.replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\b", "\\b")
                .replace("\f", "\\f")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t");
    }

    // Modern Fintech Dashboard UI (Single File Single-Page Application)
    private static final String DASHBOARD_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Distributed Banking System — Multi-Branch & Bully Visualizer</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <script src="https://unpkg.com/lucide@latest"></script>
  <style>
    @keyframes pulse-slow { 0%, 100% { opacity: 1; transform: scale(1); } 50% { opacity: 0.85; transform: scale(1.02); } }
    .animate-pulse-slow { animation: pulse-slow 3s infinite ease-in-out; }
    .glass-card { background: rgba(30, 41, 59, 0.7); backdrop-filter: blur(12px); border: 1px solid rgba(255, 255, 255, 0.08); }
    .badge-glow-green { box-shadow: 0 0 15px rgba(34, 197, 94, 0.4); }
    .badge-glow-amber { box-shadow: 0 0 15px rgba(245, 158, 11, 0.4); }
    .custom-scroll::-webkit-scrollbar { width: 6px; height: 6px; }
    .custom-scroll::-webkit-scrollbar-track { background: #0f172a; }
    .custom-scroll::-webkit-scrollbar-thumb { background: #334155; border-radius: 4px; }
  </style>
</head>
<body class="bg-slate-950 text-slate-100 min-h-screen font-sans antialiased custom-scroll">

  <!-- Header -->
  <header class="border-b border-slate-800 bg-slate-900/80 sticky top-0 z-50 backdrop-blur-md">
    <div class="max-w-7xl mx-auto px-4 py-3 flex flex-wrap items-center justify-between gap-4">
      <div class="flex items-center gap-3">
        <div class="p-2.5 bg-gradient-to-tr from-cyan-600 to-indigo-600 rounded-xl shadow-lg shadow-cyan-500/20">
          <i data-lucide="network" class="w-6 h-6 text-white"></i>
        </div>
        <div>
          <h1 class="text-lg font-bold text-white tracking-tight flex items-center gap-2">
            Distributed Banking Cluster
            <span class="text-xs font-semibold px-2 py-0.5 rounded-full bg-cyan-950 text-cyan-400 border border-cyan-800">
              Java RMI + Lamport + Bully
            </span>
          </h1>
          <p class="text-xs text-slate-400">Multi-Branch Synchronization & Dynamic Leader Election</p>
        </div>
      </div>

      <div class="flex items-center gap-3">
        <!-- Bully Coordinator Badge -->
        <div id="coordinatorBadge" class="flex items-center gap-2 px-3 py-1.5 rounded-lg bg-amber-950/60 border border-amber-600/40 text-amber-300 text-xs font-semibold badge-glow-amber">
          <i data-lucide="crown" class="w-4 h-4 text-amber-400"></i>
          <span>Coordinator: <strong id="leaderDisplay" class="text-amber-200">Detecting...</strong></span>
        </div>

        <!-- Concurrency Stress Test Button -->
        <button onclick="runBenchmark()" class="flex items-center gap-2 px-3 py-1.5 bg-indigo-600 hover:bg-indigo-500 active:scale-95 text-white text-xs font-medium rounded-lg shadow-md transition-all">
          <i data-lucide="zap" class="w-4 h-4"></i>
          <span>Stress Test (10 Threads)</span>
        </button>

        <!-- Refresh Button -->
        <button onclick="fetchState(); fetchLogs();" class="p-2 bg-slate-800 hover:bg-slate-700 text-slate-300 rounded-lg transition-all" title="Refresh State">
          <i data-lucide="refresh-cw" class="w-4 h-4"></i>
        </button>
      </div>
    </div>
  </header>

  <!-- Main Content Grid -->
  <main class="max-w-7xl mx-auto px-4 py-6 space-y-6">

    <!-- Cluster Nodes Grid (Downtown, Uptown, Metro) -->
    <div>
      <div class="flex items-center justify-between mb-3">
        <h2 class="text-sm font-semibold text-slate-300 flex items-center gap-2 uppercase tracking-wider">
          <i data-lucide="server" class="w-4 h-4 text-cyan-400"></i>
          Distributed Branch Nodes (Cluster State)
        </h2>
        <span class="text-xs text-slate-400">Auto-refreshing every 1.5s</span>
      </div>

      <div id="nodesGrid" class="grid grid-cols-1 md:grid-cols-3 gap-5">
        <!-- Dynamically rendered by JS -->
      </div>
    </div>

    <!-- Middle Row: Transaction Studio & Failure Simulator -->
    <div class="grid grid-cols-1 lg:grid-cols-12 gap-6">

      <!-- Transaction Studio (7 Cols) -->
      <div class="lg:col-span-7 glass-card rounded-2xl p-5 border border-slate-800 shadow-xl">
        <h3 class="text-sm font-semibold text-slate-200 flex items-center gap-2 mb-4">
          <i data-lucide="send" class="w-4 h-4 text-emerald-400"></i>
          Banking Transaction Studio (Local & Cross-Branch RMI)
        </h3>

        <!-- Form Tabs -->
        <div class="flex gap-2 mb-4 p-1 bg-slate-900/90 rounded-lg border border-slate-800">
          <button type="button" onclick="setTxType('TRANSFER')" id="tabTransfer" class="flex-1 py-1.5 text-xs font-semibold rounded-md bg-cyan-600 text-white transition-all">
            Cross-Branch Transfer
          </button>
          <button type="button" onclick="setTxType('DEPOSIT')" id="tabDeposit" class="flex-1 py-1.5 text-xs font-medium rounded-md text-slate-400 hover:text-white transition-all">
            Direct Deposit
          </button>
          <button type="button" onclick="setTxType('WITHDRAW')" id="tabWithdraw" class="flex-1 py-1.5 text-xs font-medium rounded-md text-slate-400 hover:text-white transition-all">
            Direct Withdraw
          </button>
        </div>

        <form id="txForm" onsubmit="submitTransaction(event)" class="space-y-4">
          <input type="hidden" id="txType" value="TRANSFER">

          <div class="grid grid-cols-1 sm:grid-cols-2 gap-4">
            <!-- Origin Branch Node -->
            <div>
              <label class="block text-xs font-medium text-slate-400 mb-1">Origin Branch Gateway</label>
              <select id="originNode" class="w-full bg-slate-900 border border-slate-700 rounded-lg px-3 py-2 text-xs text-white focus:outline-none focus:border-cyan-500">
                <option value="1">Branch 1 (Downtown)</option>
                <option value="2">Branch 2 (Uptown)</option>
                <option value="3">Branch 3 (Metro)</option>
              </select>
            </div>

            <!-- Amount -->
            <div>
              <label class="block text-xs font-medium text-slate-400 mb-1">Amount ($ USD)</label>
              <input type="number" step="0.01" min="1" id="txAmount" required value="150.00" class="w-full bg-slate-900 border border-slate-700 rounded-lg px-3 py-2 text-xs text-white focus:outline-none focus:border-cyan-500">
            </div>
          </div>

          <div class="grid grid-cols-1 sm:grid-cols-2 gap-4">
            <!-- Source Account -->
            <div id="sourceAccWrapper">
              <label class="block text-xs font-medium text-slate-400 mb-1">Source Account (Debit)</label>
              <select id="sourceAcc" class="w-full bg-slate-900 border border-slate-700 rounded-lg px-3 py-2 text-xs text-white focus:outline-none focus:border-cyan-500">
                <option value="ACC-101">ACC-101 (Alice Downtown - Node 1)</option>
                <option value="ACC-102">ACC-102 (Bob Downtown - Node 1)</option>
                <option value="ACC-201">ACC-201 (Charlie Uptown - Node 2)</option>
                <option value="ACC-202">ACC-202 (Diana Uptown - Node 2)</option>
                <option value="ACC-301">ACC-301 (Evan Metro - Node 3)</option>
                <option value="ACC-302">ACC-302 (Fiona Metro - Node 3)</option>
              </select>
            </div>

            <!-- Target Account -->
            <div id="targetAccWrapper">
              <label class="block text-xs font-medium text-slate-400 mb-1">Target Account (Credit)</label>
              <select id="targetAcc" class="w-full bg-slate-900 border border-slate-700 rounded-lg px-3 py-2 text-xs text-white focus:outline-none focus:border-cyan-500">
                <option value="ACC-201">ACC-201 (Charlie Uptown - Node 2)</option>
                <option value="ACC-101">ACC-101 (Alice Downtown - Node 1)</option>
                <option value="ACC-102">ACC-102 (Bob Downtown - Node 1)</option>
                <option value="ACC-202">ACC-202 (Diana Uptown - Node 2)</option>
                <option value="ACC-301">ACC-301 (Evan Metro - Node 3)</option>
                <option value="ACC-302">ACC-302 (Fiona Metro - Node 3)</option>
              </select>
            </div>
          </div>

          <div class="pt-2 flex items-center justify-between">
            <span class="text-xs text-slate-400 flex items-center gap-1.5">
              <i data-lucide="shield-check" class="w-3.5 h-3.5 text-cyan-400"></i>
              Fair <code>ReentrantLock</code> + Deadlock-Free Lexicographical Lock Order
            </span>
            <button type="submit" class="px-5 py-2 bg-gradient-to-r from-cyan-600 to-indigo-600 hover:from-cyan-500 hover:to-indigo-500 text-white text-xs font-semibold rounded-lg shadow-lg shadow-cyan-600/20 active:scale-95 transition-all">
              Execute RMI Transaction
            </button>
          </div>
        </form>

        <div id="txAlert" class="mt-4 hidden p-3 rounded-xl text-xs border"></div>
      </div>

      <!-- Bully Algorithm & Fault Simulator (5 Cols) -->
      <div class="lg:col-span-5 glass-card rounded-2xl p-5 border border-slate-800 shadow-xl flex flex-col justify-between">
        <div>
          <h3 class="text-sm font-semibold text-slate-200 flex items-center gap-2 mb-2">
            <i data-lucide="cpu" class="w-4 h-4 text-amber-400"></i>
            Bully Algorithm & Failure Simulator
          </h3>
          <p class="text-xs text-slate-400 mb-4">
            Simulate coordinator crash or network partition. Watch the surviving branch with the highest ID immediately crown itself coordinator.
          </p>

          <div class="space-y-3">
            <div class="flex items-center justify-between p-3 bg-slate-900/80 rounded-xl border border-slate-800">
              <div>
                <div class="text-xs font-semibold text-slate-200">Crash Coordinator (Node 3)</div>
                <div class="text-[11px] text-slate-400">Unbinds Node 3; triggers Bully election</div>
              </div>
              <div class="flex gap-2">
                <button onclick="killNode(3)" class="px-3 py-1.5 bg-rose-600/80 hover:bg-rose-500 text-white text-xs font-medium rounded-lg transition-all">
                  Kill Node 3
                </button>
                <button onclick="reviveNode(3)" class="px-3 py-1.5 bg-emerald-600/80 hover:bg-emerald-500 text-white text-xs font-medium rounded-lg transition-all">
                  Revive
                </button>
              </div>
            </div>

            <div class="flex items-center justify-between p-3 bg-slate-900/80 rounded-xl border border-slate-800">
              <div>
                <div class="text-xs font-semibold text-slate-200">Manual Election Trigger</div>
                <div class="text-[11px] text-slate-400">Force Node 1 or Node 2 to run election</div>
              </div>
              <div class="flex gap-2">
                <button onclick="triggerElection(1)" class="px-3 py-1.5 bg-amber-600/80 hover:bg-amber-500 text-white text-xs font-medium rounded-lg transition-all">
                  Elect via Node 1
                </button>
                <button onclick="triggerElection(2)" class="px-3 py-1.5 bg-amber-600/80 hover:bg-amber-500 text-white text-xs font-medium rounded-lg transition-all">
                  Elect via Node 2
                </button>
              </div>
            </div>
          </div>
        </div>

        <div class="mt-4 p-3 bg-indigo-950/40 border border-indigo-800/40 rounded-xl">
          <div class="flex items-center gap-2 text-indigo-300 text-xs font-semibold mb-1">
            <i data-lucide="info" class="w-3.5 h-3.5"></i>
            Bully Algorithm Rule
          </div>
          <p class="text-[11px] text-indigo-200/80 leading-relaxed">
            If $P_i$ detects failure, it messages all $P_j$ where $j > i$. If no higher node responds, $P_i$ proclaims VICTORY and broadcasts <code>COORDINATOR</code> to lower nodes.
          </p>
        </div>
      </div>

    </div>

    <!-- Bottom Row: Globally Synchronized Transaction Ledger -->
    <div class="glass-card rounded-2xl p-5 border border-slate-800 shadow-xl">
      <div class="flex flex-wrap items-center justify-between gap-3 mb-4">
        <div>
          <h3 class="text-sm font-semibold text-slate-200 flex items-center gap-2">
            <i data-lucide="history" class="w-4 h-4 text-cyan-400"></i>
            Globally Synchronized Causal Audit Ledger
          </h3>
          <p class="text-xs text-slate-400">
            Enforces Lamport Total Causal Order: $\text{Sorted by } (T_{\\text{Lamport}}, \\text{NodeID})$
          </p>
        </div>
        <div id="logCountBadge" class="text-xs font-mono px-2.5 py-1 rounded-md bg-slate-800 text-slate-300 border border-slate-700">
          0 Transactions Recorded
        </div>
      </div>

      <div class="overflow-x-auto">
        <table class="w-full text-left text-xs">
          <thead>
            <tr class="border-b border-slate-800 text-slate-400 bg-slate-900/50">
              <th class="py-2.5 px-3">Lamport Clock</th>
              <th class="py-2.5 px-3">Origin Node</th>
              <th class="py-2.5 px-3">Tx ID</th>
              <th class="py-2.5 px-3">Type</th>
              <th class="py-2.5 px-3">Source &rarr; Target</th>
              <th class="py-2.5 px-3">Amount</th>
              <th class="py-2.5 px-3">Status</th>
              <th class="py-2.5 px-3">Audit Details</th>
            </tr>
          </thead>
          <tbody id="logsTableBody" class="divide-y divide-slate-800/60 font-mono">
            <tr>
              <td colspan="8" class="text-center py-6 text-slate-500">Loading audit log...</td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>

  </main>

  <script>
    // State management
    let currentLeader = -1;

    function setTxType(type) {
      document.getElementById('txType').value = type;
      const tTab = document.getElementById('tabTransfer');
      const dTab = document.getElementById('tabDeposit');
      const wTab = document.getElementById('tabWithdraw');
      const srcWrap = document.getElementById('sourceAccWrapper');
      const tgtWrap = document.getElementById('targetAccWrapper');

      [tTab, dTab, wTab].forEach(b => {
        b.className = "flex-1 py-1.5 text-xs font-medium rounded-md text-slate-400 hover:text-white transition-all";
      });

      if (type === 'TRANSFER') {
        tTab.className = "flex-1 py-1.5 text-xs font-semibold rounded-md bg-cyan-600 text-white transition-all";
        srcWrap.style.display = 'block';
        tgtWrap.style.display = 'block';
      } else if (type === 'DEPOSIT') {
        dTab.className = "flex-1 py-1.5 text-xs font-semibold rounded-md bg-cyan-600 text-white transition-all";
        srcWrap.style.display = 'none';
        tgtWrap.style.display = 'block';
      } else if (type === 'WITHDRAW') {
        wTab.className = "flex-1 py-1.5 text-xs font-semibold rounded-md bg-cyan-600 text-white transition-all";
        srcWrap.style.display = 'block';
        tgtWrap.style.display = 'none';
      }
    }

    async function fetchState() {
      try {
        const res = await fetch('/api/state');
        const data = await res.json();
        currentLeader = data.activeLeader;

        document.getElementById('leaderDisplay').textContent = currentLeader > 0 ? `Branch Node ${currentLeader}` : 'No Active Leader';

        const grid = document.getElementById('nodesGrid');
        grid.innerHTML = data.nodes.map(n => {
          if (!n.online) {
            return `
              <div class="glass-card rounded-2xl p-5 border border-rose-900/40 bg-rose-950/20 relative overflow-hidden">
                <div class="flex items-center justify-between mb-3">
                  <div class="flex items-center gap-2">
                    <span class="w-2.5 h-2.5 rounded-full bg-rose-500 animate-ping"></span>
                    <h3 class="font-bold text-sm text-rose-300">Node ${n.nodeId} (${n.name})</h3>
                  </div>
                  <span class="px-2 py-0.5 rounded text-[11px] font-semibold bg-rose-950 text-rose-400 border border-rose-800">
                    OFFLINE / CRASHED
                  </span>
                </div>
                <p class="text-xs text-rose-200/70 mb-4">Node is unreachable or crashed in simulated failure.</p>
                <button onclick="reviveNode(${n.nodeId})" class="w-full py-1.5 bg-emerald-600 hover:bg-emerald-500 text-white text-xs font-semibold rounded-lg transition-all">
                  Revive & Rejoin Cluster
                </button>
              </div>
            `;
          }

          const isLeader = (n.nodeId === currentLeader);
          return `
            <div class="glass-card rounded-2xl p-5 border ${isLeader ? 'border-amber-500/50 bg-amber-950/10' : 'border-slate-800'} relative">
              ${isLeader ? `
                <div class="absolute -top-3 right-4 px-2.5 py-0.5 rounded-full bg-amber-500 text-slate-950 text-[10px] font-extrabold flex items-center gap-1 shadow-lg shadow-amber-500/30">
                  <i data-lucide="crown" class="w-3 h-3"></i> COORDINATOR
                </div>
              ` : ''}

              <div class="flex items-center justify-between mb-3">
                <div class="flex items-center gap-2">
                  <span class="w-2.5 h-2.5 rounded-full bg-emerald-500"></span>
                  <h3 class="font-bold text-sm text-white">Node ${n.nodeId}: ${n.name}</h3>
                </div>
                <span class="px-2 py-0.5 rounded text-[11px] font-mono bg-cyan-950 text-cyan-400 border border-cyan-800">
                  L-Clock: ${n.lamportTime}
                </span>
              </div>

              <!-- Accounts -->
              <div class="space-y-2 mb-4">
                ${n.accounts.map(acc => `
                  <div class="p-2.5 bg-slate-900/90 rounded-xl border border-slate-800 flex items-center justify-between">
                    <div>
                      <div class="text-xs font-semibold text-slate-200">${acc.id}</div>
                      <div class="text-[11px] text-slate-400">${acc.holder}</div>
                    </div>
                    <div class="text-xs font-bold text-emerald-400 font-mono">
                      $${acc.balance.toFixed(2)}
                    </div>
                  </div>
                `).join('')}
              </div>

              <!-- Controls -->
              <div class="flex gap-2">
                <button onclick="triggerElection(${n.nodeId})" class="flex-1 py-1.5 bg-slate-800 hover:bg-slate-700 text-slate-300 text-[11px] font-medium rounded-lg transition-all">
                  Trigger Election
                </button>
                <button onclick="killNode(${n.nodeId})" class="px-3 py-1.5 bg-rose-600/20 hover:bg-rose-600/40 text-rose-300 border border-rose-800/40 text-[11px] font-medium rounded-lg transition-all">
                  Crash
                </button>
              </div>
            </div>
          `;
        }).join('');

        lucide.createIcons();
      } catch (e) {
        console.error("State fetch error:", e);
      }
    }

    async function fetchLogs() {
      try {
        const res = await fetch('/api/logs');
        const logs = await res.json();

        document.getElementById('logCountBadge').textContent = `${logs.length} Transactions Recorded`;
        const tbody = document.getElementById('logsTableBody');

        if (logs.length === 0) {
          tbody.innerHTML = '<tr><td colspan="8" class="text-center py-6 text-slate-500">No transactions recorded yet.</td></tr>';
          return;
        }

        tbody.innerHTML = logs.map(tx => {
          let badgeColor = "bg-cyan-950 text-cyan-400 border-cyan-800";
          if (tx.type === 'TRANSFER') badgeColor = "bg-purple-950 text-purple-400 border-purple-800";
          if (tx.type === 'WITHDRAW') badgeColor = "bg-amber-950 text-amber-400 border-amber-800";

          return `
            <tr class="hover:bg-slate-900/50 transition-colors">
              <td class="py-2 px-3 text-cyan-400 font-bold">L-${tx.lamport}</td>
              <td class="py-2 px-3 text-slate-300">Node ${tx.originNode}</td>
              <td class="py-2 px-3 text-slate-400 text-[11px]">${tx.txId.substring(0, 8)}</td>
              <td class="py-2 px-3">
                <span class="px-2 py-0.5 rounded text-[10px] font-semibold border ${badgeColor}">
                  ${tx.type}
                </span>
              </td>
              <td class="py-2 px-3 text-slate-200">${tx.source} &rarr; ${tx.target}</td>
              <td class="py-2 px-3 text-emerald-400 font-semibold">$${tx.amount.toFixed(2)}</td>
              <td class="py-2 px-3">
                <span class="text-[11px] ${tx.status === 'COMMITTED' ? 'text-emerald-400' : 'text-rose-400'} font-semibold">
                  ${tx.status}
                </span>
              </td>
              <td class="py-2 px-3 text-slate-400 text-[11px] font-sans truncate max-w-xs" title="${tx.details}">
                ${tx.details}
              </td>
            </tr>
          `;
        }).join('');

      } catch (e) {
        console.error("Logs fetch error:", e);
      }
    }

    async function submitTransaction(e) {
      e.preventDefault();
      const type = document.getElementById('txType').value;
      const originNode = document.getElementById('originNode').value;
      const source = document.getElementById('sourceAcc').value;
      const target = document.getElementById('targetAcc').value;
      const amount = document.getElementById('txAmount').value;

      try {
        const res = await fetch('/api/transaction', {
          method: 'POST',
          headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
          body: `type=${type}&originNode=${originNode}&source=${source}&target=${target}&amount=${amount}`
        });
        const data = await res.json();
        const alertBox = document.getElementById('txAlert');
        alertBox.className = data.success && data.status === 'COMMITTED'
          ? "mt-4 p-3 rounded-xl text-xs border border-emerald-800 bg-emerald-950/60 text-emerald-200 block"
          : "mt-4 p-3 rounded-xl text-xs border border-rose-800 bg-rose-950/60 text-rose-200 block";
        alertBox.innerHTML = `<strong>${data.status}:</strong> ${data.details || data.error} (Lamport Clock: ${data.lamport})`;

        fetchState();
        fetchLogs();
      } catch (err) {
        alert("Transaction failed: " + err);
      }
    }

    async function triggerElection(nodeId) {
      await fetch('/api/election', {
        method: 'POST',
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
        body: `nodeId=${nodeId}`
      });
      setTimeout(() => { fetchState(); fetchLogs(); }, 600);
    }

    async function killNode(nodeId) {
      await fetch('/api/kill', {
        method: 'POST',
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
        body: `nodeId=${nodeId}`
      });
      setTimeout(() => { fetchState(); fetchLogs(); }, 500);
    }

    async function reviveNode(nodeId) {
      await fetch('/api/revive', {
        method: 'POST',
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
        body: `nodeId=${nodeId}`
      });
      setTimeout(() => { fetchState(); fetchLogs(); }, 800);
    }

    async function runBenchmark() {
      const btn = event.currentTarget;
      btn.disabled = true;
      btn.innerHTML = `<span class="animate-spin inline-block mr-1">⚙</span> Stress Testing...`;

      try {
        const res = await fetch('/api/benchmark', { method: 'POST' });
        const data = await res.json();
        alert(`Stress Test Complete!\\n\\nDuration: ${data.durationMs}ms\\nConcurrent Ops: ${data.operations}/10\\nInitial Bal: $${data.initialBalance}\\nFinal Bal: $${data.finalBalance}\\nZero Race Conditions: ${data.consistent ? "PASSED (100% Consistent)" : "FAILED"}`);
        fetchState();
        fetchLogs();
      } catch (e) {
        alert("Benchmark error: " + e);
      } finally {
        btn.disabled = false;
        btn.innerHTML = `<i data-lucide="zap" class="w-4 h-4"></i><span>Stress Test (10 Threads)</span>`;
        lucide.createIcons();
      }
    }

    // Auto-refresh loop
    fetchState();
    fetchLogs();
    setInterval(() => {
      fetchState();
      fetchLogs();
    }, 1500);
  </script>
</body>
</html>
""";
}
