import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * ClientApp.java
 * Interactive Command Line Interface for end-users and administrators.
 *
 * Features:
 * - Direct RMI invocation to any branch node (with dynamic branch switching)
 * - Client-side Lamport Logical Clock progression
 * - Balance inquiry, deposit, withdrawal, and cross-branch atomic transfers
 * - Real-time audit log visualization (local and globally synchronized)
 * - Automated multi-threaded concurrency benchmark verifying race-condition immunity
 */
public class ClientApp {

    private final LamportClock clientClock;
    private BankInterface currentBranch;
    private int connectedNodeId;
    private String connectedBranchName;
    private final int rmiPort;
    private final Scanner scanner;

    public ClientApp(int defaultNodeId, int port) {
        this.clientClock = new LamportClock(0);
        this.connectedNodeId = defaultNodeId;
        this.rmiPort = port;
        this.scanner = new Scanner(System.in);
    }

    public boolean connectToBranch(int targetNodeId) {
        try {
            Registry registry = LocateRegistry.getRegistry("localhost", rmiPort);
            String serviceName = "BankNode_" + targetNodeId;
            this.currentBranch = (BankInterface) registry.lookup(serviceName);
            this.connectedNodeId = targetNodeId;
            this.connectedBranchName = currentBranch.getBranchName();

            // Sync Lamport clock with node
            int nodeClock = currentBranch.getLamportTime();
            clientClock.updateAndTick(nodeClock);

            System.out.printf("%n[Connected] Successfully bound to Branch Node %d: '%s' (Node Lamport Time: %d)%n",
                    connectedNodeId, connectedBranchName, nodeClock);
            return true;
        } catch (Exception e) {
            System.err.printf("[Connection Error] Unable to reach Branch Node %d: %s%n", targetNodeId, e.getMessage());
            return false;
        }
    }

    public void run() {
        printBanner();

        if (!connectToBranch(connectedNodeId)) {
            System.out.println("Attempting fallback to Node 1...");
            if (!connectToBranch(1)) {
                System.out.println("Could not connect to any branch node. Ensure BankNode servers are running!");
                System.out.println("Exiting client.");
                return;
            }
        }

        boolean running = true;
        while (running) {
            printMenu();
            System.out.print("Enter choice [0-9]: ");
            String input = scanner.nextLine().trim();

            try {
                switch (input) {
                    case "1":
                        handleBalanceInquiry();
                        break;
                    case "2":
                        handleDeposit();
                        break;
                    case "3":
                        handleWithdraw();
                        break;
                    case "4":
                        handleTransfer();
                        break;
                    case "5":
                        handleViewLocalLog();
                        break;
                    case "6":
                        handleViewGlobalLog();
                        break;
                    case "7":
                        handleClusterStatus();
                        break;
                    case "8":
                        handleSwitchBranch();
                        break;
                    case "9":
                        handleConcurrencyBenchmark();
                        break;
                    case "0":
                        running = false;
                        System.out.println("Exiting Banking Client. Goodbye!");
                        break;
                    default:
                        System.out.println("Invalid option. Please enter a number between 0 and 9.");
                }
            } catch (Exception e) {
                System.err.println("Operation failed: " + e.getMessage());
                // In case current branch crashed, prompt to switch
                System.out.println("Hint: The connected branch might be offline. Use Option 8 to switch branches.");
            }
            System.out.println();
        }
    }

    private void handleBalanceInquiry() throws Exception {
        System.out.println("\n--- Balance Inquiry ---");
        System.out.println("Pre-seeded accounts: ACC-101, ACC-102 (Downtown), ACC-201, ACC-202 (Uptown), ACC-301, ACC-302 (Metro)");
        System.out.print("Enter Account ID: ");
        String accountId = scanner.nextLine().trim().toUpperCase();

        int sendTime = clientClock.tick();
        System.out.printf("[Client Lamport: %d] Sending balance inquiry for %s...%n", sendTime, accountId);

        double balance = currentBranch.getBalance(accountId, sendTime);
        int recvTime = clientClock.updateAndTick(currentBranch.getLamportTime());

        System.out.printf(">>> Account: %s | Current Balance: $%.2f [Client Lamport: %d]%n",
                accountId, balance, recvTime);
    }

    private void handleDeposit() throws Exception {
        System.out.println("\n--- Deposit Funds ---");
        System.out.print("Enter Account ID: ");
        String accountId = scanner.nextLine().trim().toUpperCase();

        System.out.print("Enter Deposit Amount ($): ");
        double amount = Double.parseDouble(scanner.nextLine().trim());

        int sendTime = clientClock.tick();
        System.out.printf("[Client Lamport: %d] Submitting Deposit request ($%.2f -> %s)...%n",
                sendTime, amount, accountId);

        Transaction tx = currentBranch.deposit(accountId, amount, sendTime);
        int recvTime = clientClock.updateAndTick(tx.getLamportTimestamp());

        System.out.println("\n--- Transaction Receipt ---");
        System.out.printf("  Status:    %s%n", tx.getStatus());
        System.out.printf("  Tx ID:     %s%n", tx.getTransactionId());
        System.out.printf("  L-Clock:   Server=%d, Client Updated=%d%n", tx.getLamportTimestamp(), recvTime);
        System.out.printf("  Details:   %s%n", tx.getDetails());
    }

    private void handleWithdraw() throws Exception {
        System.out.println("\n--- Withdraw Funds ---");
        System.out.print("Enter Account ID: ");
        String accountId = scanner.nextLine().trim().toUpperCase();

        System.out.print("Enter Withdrawal Amount ($): ");
        double amount = Double.parseDouble(scanner.nextLine().trim());

        int sendTime = clientClock.tick();
        System.out.printf("[Client Lamport: %d] Submitting Withdrawal request ($%.2f from %s)...%n",
                sendTime, amount, accountId);

        Transaction tx = currentBranch.withdraw(accountId, amount, sendTime);
        int recvTime = clientClock.updateAndTick(tx.getLamportTimestamp());

        System.out.println("\n--- Transaction Receipt ---");
        System.out.printf("  Status:    %s%n", tx.getStatus());
        System.out.printf("  Tx ID:     %s%n", tx.getTransactionId());
        System.out.printf("  L-Clock:   Server=%d, Client Updated=%d%n", tx.getLamportTimestamp(), recvTime);
        System.out.printf("  Details:   %s%n", tx.getDetails());
    }

    private void handleTransfer() throws Exception {
        System.out.println("\n--- Fund Transfer (Local or Cross-Branch) ---");
        System.out.print("Enter Source Account (From): ");
        String from = scanner.nextLine().trim().toUpperCase();

        System.out.print("Enter Target Account (To):   ");
        String to = scanner.nextLine().trim().toUpperCase();

        System.out.print("Enter Transfer Amount ($):   ");
        double amount = Double.parseDouble(scanner.nextLine().trim());

        int sendTime = clientClock.tick();
        System.out.printf("[Client Lamport: %d] Submitting Transfer ($%.2f: %s -> %s)...%n",
                sendTime, amount, from, to);

        Transaction tx = currentBranch.transfer(from, to, amount, sendTime);
        int recvTime = clientClock.updateAndTick(tx.getLamportTimestamp());

        System.out.println("\n--- Transaction Receipt ---");
        System.out.printf("  Status:    %s%n", tx.getStatus());
        System.out.printf("  Tx ID:     %s%n", tx.getTransactionId());
        System.out.printf("  L-Clock:   Server=%d, Client Updated=%d%n", tx.getLamportTimestamp(), recvTime);
        System.out.printf("  Details:   %s%n", tx.getDetails());
    }

    private void handleViewLocalLog() throws Exception {
        System.out.printf("%n=== Local Audit Log for Branch %d (%s) ===%n", connectedNodeId, connectedBranchName);
        List<Transaction> logs = currentBranch.getAuditLog();
        if (logs.isEmpty()) {
            System.out.println("(No transactions recorded yet at this branch.)");
            return;
        }
        printTransactionTable(logs);
    }

    private void handleViewGlobalLog() throws Exception {
        System.out.println("\n=== Global Synchronized Audit Log (Coordinator Replicated) ===");
        List<Transaction> logs = currentBranch.getGlobalTransactionLog();
        if (logs.isEmpty()) {
            System.out.println("(No transactions recorded globally yet.)");
            return;
        }
        System.out.printf("Total Global Records: %d (Enforcing Total Causal Lamport Order)%n", logs.size());
        printTransactionTable(logs);
    }

    private void handleClusterStatus() throws Exception {
        System.out.println("\n=== Distributed Cluster Status ===");
        int leaderId = currentBranch.getCurrentLeaderId();
        int nodeClock = currentBranch.getLamportTime();
        System.out.printf("  Connected Branch: Node %d ('%s')%n", connectedNodeId, connectedBranchName);
        System.out.printf("  Node Lamport Clock:   %d%n", nodeClock);
        System.out.printf("  Client Lamport Clock: %d%n", clientClock.getValue());
        System.out.printf("  Current Coordinator (Bully Leader): Node %d%n", leaderId);

        // Scan known nodes
        System.out.println("\nCluster Node Discovery:");
        Registry registry = LocateRegistry.getRegistry("localhost", rmiPort);
        for (int i = 1; i <= 3; i++) {
            try {
                BankInterface peer = (BankInterface) registry.lookup("BankNode_" + i);
                int peerLeader = peer.getCurrentLeaderId();
                int peerClock = peer.getLamportTime();
                String isLead = (i == peerLeader) ? " [COORDINATOR/PRIMARY]" : "";
                System.out.printf("  - Node %d: ONLINE | Name: %-15s | L-Clock: %2d | Leader: Node %d%s%n",
                        i, peer.getBranchName(), peerClock, peerLeader, isLead);
            } catch (Exception e) {
                System.out.printf("  - Node %d: OFFLINE / UNREACHABLE%n", i);
            }
        }
    }

    private void handleSwitchBranch() {
        System.out.println("\n--- Switch Connected Branch ---");
        System.out.println("Available Nodes: 1 (Downtown), 2 (Uptown), 3 (Metro)");
        System.out.print("Enter Target Node ID to connect to: ");
        try {
            int newId = Integer.parseInt(scanner.nextLine().trim());
            connectToBranch(newId);
        } catch (NumberFormatException e) {
            System.out.println("Invalid Node ID.");
        }
    }

    /**
     * Concurrency Benchmark:
     * Spawns multiple threads executing simultaneous deposits and withdrawals
     * on a single account to prove thread safety and consistency without race conditions.
     */
    private void handleConcurrencyBenchmark() throws Exception {
        System.out.println("\n=================================================================");
        System.out.println("  CONCURRENCY & THREAD-SAFETY STRESS TEST");
        System.out.println("=================================================================");
        System.out.println("Test Plan: 10 Concurrent Threads performing simultaneous updates");
        System.out.println("Target Account: ACC-101 (Downtown Alice)");

        double initialBal = currentBranch.getBalance("ACC-101", clientClock.tick());
        System.out.printf("Initial Balance of ACC-101: $%.2f%n", initialBal);

        int numThreads = 10;
        double depositAmount = 50.0;
        double withdrawAmount = 50.0;
        // 5 threads deposit $50 each, 5 threads withdraw $50 each -> Expected net change: $0.00!

        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        CountDownLatch startSignal = new CountDownLatch(1);
        CountDownLatch doneSignal = new CountDownLatch(numThreads);
        AtomicInteger successOps = new AtomicInteger(0);

        System.out.println("Spawning 5 Deposit threads (+$50) and 5 Withdraw threads (-$50)...");

        for (int i = 0; i < numThreads; i++) {
            final boolean isDeposit = (i % 2 == 0);
            executor.submit(() -> {
                try {
                    startSignal.await(); // All threads blast off simultaneously
                    int localTime = clientClock.tick();
                    if (isDeposit) {
                        currentBranch.deposit("ACC-101", depositAmount, localTime);
                    } else {
                        currentBranch.withdraw("ACC-101", withdrawAmount, localTime);
                    }
                    successOps.incrementAndGet();
                } catch (Exception e) {
                    System.err.println("Thread exception: " + e.getMessage());
                } finally {
                    doneSignal.countDown();
                }
            });
        }

        // Trigger simultaneous execution
        long start = System.currentTimeMillis();
        startSignal.countDown();
        doneSignal.await();
        long elapsed = System.currentTimeMillis() - start;
        executor.shutdown();

        double finalBal = currentBranch.getBalance("ACC-101", clientClock.tick());
        System.out.println("-----------------------------------------------------------------");
        System.out.printf("Benchmark Finished in %d ms! Total Successful Operations: %d/%d%n",
                elapsed, successOps.get(), numThreads);
        System.out.printf("Initial Balance:  $%.2f%n", initialBal);
        System.out.printf("Final Balance:    $%.2f%n", finalBal);
        System.out.printf("Expected Balance: $%.2f%n", initialBal);

        if (Math.abs(finalBal - initialBal) < 0.001) {
            System.out.println(">>> RESULT: PASSED! Zero race conditions detected. Strict consistency maintained.");
        } else {
            System.err.println(">>> RESULT: FAILED! Balance discrepancy detected.");
        }
        System.out.println("=================================================================");
    }

    private void printTransactionTable(List<Transaction> transactions) {
        System.out.println("--------------------------------------------------------------------------------------------------------------------------------");
        System.out.printf("%-10s | %-6s | %-8s | %-8s | %-8s | %-10s | %-12s | %s%n",
                "L-Timestamp", "Origin", "Tx ID", "Type", "Source", "Target", "Amount", "Status & Details");
        System.out.println("--------------------------------------------------------------------------------------------------------------------------------");
        for (Transaction tx : transactions) {
            String shortId = tx.getTransactionId().substring(0, Math.min(8, tx.getTransactionId().length()));
            System.out.printf("%-10d | Node %-2d | %-8s | %-8s | %-8s | %-10s | $%-11.2f | [%s] %s%n",
                    tx.getLamportTimestamp(), tx.getOriginNodeId(), shortId,
                    tx.getType(), tx.getSourceAccount(), tx.getTargetAccount(),
                    tx.getAmount(), tx.getStatus(), tx.getDetails());
        }
        System.out.println("--------------------------------------------------------------------------------------------------------------------------------");
    }

    private void printBanner() {
        System.out.println("=================================================================");
        System.out.println("  DISTRIBUTED BANKING SYSTEM - CLIENT CONSOLE");
        System.out.println("  RMI Multi-Branch | Lamport Clocks | Bully Leader Election");
        System.out.println("=================================================================");
    }

    private void printMenu() {
        System.out.println("-----------------------------------------------------------------");
        System.out.printf(" Connected to: Node %d [%s] | Client Lamport Clock: %d%n",
                connectedNodeId, connectedBranchName, clientClock.getValue());
        System.out.println("-----------------------------------------------------------------");
        System.out.println("  [1] Check Account Balance");
        System.out.println("  [2] Deposit Funds");
        System.out.println("  [3] Withdraw Funds");
        System.out.println("  [4] Transfer Funds (Intra-Branch or Cross-Branch)");
        System.out.println("  [5] View Local Audit Log of Connected Branch");
        System.out.println("  [6] View Global Synchronized Audit Log (Coordinator Replicated)");
        System.out.println("  [7] View Cluster Status & Active Leader");
        System.out.println("  [8] Switch Connected Branch (Connect to Node 1, 2, or 3)");
        System.out.println("  [9] Run Concurrency Stress Test (Verify Race Condition Immunity)");
        System.out.println("  [0] Exit");
        System.out.println("-----------------------------------------------------------------");
    }

    public static void main(String[] args) {
        int initialNode = args.length > 0 ? Integer.parseInt(args[0]) : 1;
        int port = args.length > 1 ? Integer.parseInt(args[1]) : 1099;

        ClientApp app = new ClientApp(initialNode, port);
        app.run();
    }
}
