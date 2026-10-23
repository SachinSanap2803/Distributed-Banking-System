import java.io.Serializable;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.ExportException;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * BankNode.java
 * Branch Server Node representing an independent banking branch in the distributed system.
 *
 * Implements:
 * 1. Java RMI remote services (deposit, withdraw, balance inquiry, cross-branch transfer)
 * 2. Concurrency control with fair locks to eliminate race conditions
 * 3. Lamport Logical Clock synchronization for causal event ordering
 * 4. Bully Election Algorithm for dynamic fault tolerance and leader election
 * 5. Coordinator duties when elected primary branch
 */
public class BankNode extends UnicastRemoteObject implements BankInterface {
    private static final long serialVersionUID = 1L;

    private final int nodeId;
    private final String branchName;
    private final int rmiPort;
    private final List<Integer> clusterNodeIds;

    // Concurrency: thread-safe storage for branch accounts
    private final Map<String, Account> localAccounts;

    // Lamport's Logical Clock
    private final LamportClock lamportClock;

    // Audit logs
    private final List<Transaction> localAuditLog;

    // Bully Leader Election State
    private volatile int currentLeaderId;
    private volatile boolean isLeader;
    private final AtomicBoolean electionInProgress;
    private final ScheduledExecutorService heartbeatScheduler;
    private CoordinatorManager coordinatorManager;

    public BankNode(int nodeId, String branchName, int rmiPort, List<Integer> clusterNodeIds) throws RemoteException {
        super();
        this.nodeId = nodeId;
        this.branchName = branchName;
        this.rmiPort = rmiPort;
        this.clusterNodeIds = new ArrayList<>(clusterNodeIds);

        this.localAccounts = new ConcurrentHashMap<>();
        this.lamportClock = new LamportClock(0);
        this.localAuditLog = new CopyOnWriteArrayList<>();

        // Highest ID is assumed initial candidate until discovery
        this.currentLeaderId = Collections.max(clusterNodeIds);
        this.isLeader = (this.nodeId == this.currentLeaderId);
        this.electionInProgress = new AtomicBoolean(false);

        if (this.isLeader) {
            this.coordinatorManager = new CoordinatorManager(nodeId);
        }

        this.heartbeatScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "Heartbeat-Monitor-" + nodeId);
            t.setDaemon(true);
            return t;
        });

        seedInitialAccounts();
    }

    /**
     * Seeds initial branch accounts based on branch node ID.
     */
    private void seedInitialAccounts() {
        switch (nodeId) {
            case 1:
                localAccounts.put("ACC-101", new Account("ACC-101", "Alice Downtown", 1500.00));
                localAccounts.put("ACC-102", new Account("ACC-102", "Bob Downtown", 2800.00));
                break;
            case 2:
                localAccounts.put("ACC-201", new Account("ACC-201", "Charlie Uptown", 2200.00));
                localAccounts.put("ACC-202", new Account("ACC-202", "Diana Uptown", 3500.00));
                break;
            case 3:
                localAccounts.put("ACC-301", new Account("ACC-301", "Evan Metro", 1900.00));
                localAccounts.put("ACC-302", new Account("ACC-302", "Fiona Metro", 4200.00));
                break;
            default:
                localAccounts.put("ACC-" + nodeId + "01", new Account("ACC-" + nodeId + "01", "BranchUser-" + nodeId + "A", 1000.00));
                localAccounts.put("ACC-" + nodeId + "02", new Account("ACC-" + nodeId + "02", "BranchUser-" + nodeId + "B", 2000.00));
                break;
        }
    }

    // =========================================================================
    //  Banking Operations (Client Facing)
    // =========================================================================

    @Override
    public double getBalance(String accountId, int clientLamportTime) throws RemoteException {
        // Lamport Rule: update clock upon receiving remote message
        int lTime = lamportClock.updateAndTick(clientLamportTime);

        Account account = localAccounts.get(accountId);
        if (account == null) {
            // Check if account belongs to a remote branch
            int targetNodeId = resolveNodeIdForAccount(accountId);
            if (targetNodeId != this.nodeId) {
                BankInterface remotePeer = getPeerNode(targetNodeId);
                if (remotePeer != null) {
                    return remotePeer.getBalance(accountId, lTime);
                }
            }
            throw new IllegalArgumentException("Account " + accountId + " does not exist in cluster.");
        }

        double bal = account.getBalance();
        System.out.printf("[%s | L-Time: %2d] Balance inquiry on %s -> $%.2f%n",
                branchName, lTime, accountId, bal);

        recordLocalTransaction(new Transaction(
                UUID.randomUUID().toString(),
                Transaction.Type.BALANCE_INQUIRY,
                accountId, "N/A", bal, lTime, nodeId, "SUCCESS",
                "Balance verified for " + account.getHolderName()
        ));

        return bal;
    }

    @Override
    public Transaction deposit(String accountId, double amount, int clientLamportTime) throws RemoteException {
        int lTime = lamportClock.updateAndTick(clientLamportTime);

        Account account = localAccounts.get(accountId);
        if (account == null) {
            // Forward to designated branch if account is hosted remotely
            int targetNodeId = resolveNodeIdForAccount(accountId);
            if (targetNodeId != this.nodeId) {
                BankInterface remotePeer = getPeerNode(targetNodeId);
                if (remotePeer != null) {
                    return remotePeer.deposit(accountId, amount, lTime);
                }
            }
            Transaction failTx = new Transaction(
                    UUID.randomUUID().toString(), Transaction.Type.DEPOSIT,
                    "CASH", accountId, amount, lTime, nodeId, "FAILED_INVALID_ACCOUNT",
                    "Account " + accountId + " not found."
            );
            recordAndSync(failTx);
            return failTx;
        }

        // Thread-safe atomic deposit
        account.deposit(amount);
        int finalLTime = lamportClock.tick();

        Transaction tx = new Transaction(
                UUID.randomUUID().toString(), Transaction.Type.DEPOSIT,
                "CASH", accountId, amount, finalLTime, nodeId, "COMMITTED",
                String.format("Deposited $%.2f into %s. New Balance: $%.2f", amount, accountId, account.getBalance())
        );

        System.out.printf("[%s | L-Time: %2d] DEPOSIT SUCCESS -> Account: %s, Amount: $%.2f, New Balance: $%.2f%n",
                branchName, finalLTime, accountId, amount, account.getBalance());

        recordAndSync(tx);
        return tx;
    }

    @Override
    public Transaction withdraw(String accountId, double amount, int clientLamportTime) throws RemoteException {
        int lTime = lamportClock.updateAndTick(clientLamportTime);

        Account account = localAccounts.get(accountId);
        if (account == null) {
            int targetNodeId = resolveNodeIdForAccount(accountId);
            if (targetNodeId != this.nodeId) {
                BankInterface remotePeer = getPeerNode(targetNodeId);
                if (remotePeer != null) {
                    return remotePeer.withdraw(accountId, amount, lTime);
                }
            }
            Transaction failTx = new Transaction(
                    UUID.randomUUID().toString(), Transaction.Type.WITHDRAW,
                    accountId, "CASH", amount, lTime, nodeId, "FAILED_INVALID_ACCOUNT",
                    "Account " + accountId + " not found."
            );
            recordAndSync(failTx);
            return failTx;
        }

        // Thread-safe atomic withdrawal
        boolean success = account.withdraw(amount);
        int finalLTime = lamportClock.tick();

        String status = success ? "COMMITTED" : "FAILED_INSUFFICIENT_FUNDS";
        String detail = success
                ? String.format("Withdrew $%.2f from %s. New Balance: $%.2f", amount, accountId, account.getBalance())
                : String.format("Failed to withdraw $%.2f from %s. Insufficient funds (Balance: $%.2f)", amount, accountId, account.getBalance());

        Transaction tx = new Transaction(
                UUID.randomUUID().toString(), Transaction.Type.WITHDRAW,
                accountId, "CASH", amount, finalLTime, nodeId, status, detail
        );

        System.out.printf("[%s | L-Time: %2d] WITHDRAW %s -> Account: %s, Amount: $%.2f, Balance: $%.2f%n",
                branchName, finalLTime, status, accountId, amount, account.getBalance());

        recordAndSync(tx);
        return tx;
    }

    @Override
    public Transaction transfer(String fromAccount, String toAccount, double amount, int clientLamportTime) throws RemoteException {
        int lTime = lamportClock.updateAndTick(clientLamportTime);
        String txId = UUID.randomUUID().toString();

        int sourceBranch = resolveNodeIdForAccount(fromAccount);
        int targetBranch = resolveNodeIdForAccount(toAccount);

        // Case 1: If neither source nor target is local, forward request to source branch
        if (sourceBranch != this.nodeId) {
            BankInterface srcPeer = getPeerNode(sourceBranch);
            if (srcPeer != null) {
                return srcPeer.transfer(fromAccount, toAccount, amount, lTime);
            } else {
                Transaction failTx = new Transaction(txId, Transaction.Type.TRANSFER,
                        fromAccount, toAccount, amount, lTime, nodeId, "FAILED_NETWORK",
                        "Source branch node " + sourceBranch + " unreachable.");
                recordAndSync(failTx);
                return failTx;
            }
        }

        // Case 2: Source account is local to this node
        Account srcAcc = localAccounts.get(fromAccount);
        if (srcAcc == null) {
            Transaction failTx = new Transaction(txId, Transaction.Type.TRANSFER,
                    fromAccount, toAccount, amount, lTime, nodeId, "FAILED_INVALID_ACCOUNT",
                    "Source account " + fromAccount + " does not exist.");
            recordAndSync(failTx);
            return failTx;
        }

        // Intra-branch transfer (both accounts in this branch)
        if (targetBranch == this.nodeId) {
            Account tgtAcc = localAccounts.get(toAccount);
            if (tgtAcc == null) {
                Transaction failTx = new Transaction(txId, Transaction.Type.TRANSFER,
                        fromAccount, toAccount, amount, lTime, nodeId, "FAILED_INVALID_ACCOUNT",
                        "Target account " + toAccount + " does not exist.");
                recordAndSync(failTx);
                return failTx;
            }

            // Deadlock-free locking: enforce global acquisition order by accountId
            Account firstLock = fromAccount.compareTo(toAccount) < 0 ? srcAcc : tgtAcc;
            Account secondLock = firstLock == srcAcc ? tgtAcc : srcAcc;

            firstLock.getLock().lock();
            try {
                secondLock.getLock().lock();
                try {
                    if (srcAcc.getBalance() < amount) {
                        int failTime = lamportClock.tick();
                        Transaction failTx = new Transaction(txId, Transaction.Type.TRANSFER,
                                fromAccount, toAccount, amount, failTime, nodeId, "FAILED_INSUFFICIENT_FUNDS",
                                "Insufficient balance in " + fromAccount);
                        recordAndSync(failTx);
                        return failTx;
                    }
                    srcAcc.withdraw(amount);
                    tgtAcc.deposit(amount);
                    int commitTime = lamportClock.tick();
                    Transaction successTx = new Transaction(txId, Transaction.Type.TRANSFER,
                            fromAccount, toAccount, amount, commitTime, nodeId, "COMMITTED",
                            String.format("Intra-branch transfer $%.2f: %s -> %s", amount, fromAccount, toAccount));
                    System.out.printf("[%s | L-Time: %2d] INTRA-BRANCH TRANSFER COMMITTED: %s -> %s ($%.2f)%n",
                            branchName, commitTime, fromAccount, toAccount, amount);
                    recordAndSync(successTx);
                    return successTx;
                } finally {
                    secondLock.getLock().unlock();
                }
            } finally {
                firstLock.getLock().unlock();
            }
        }

        // Case 3: Distributed Cross-Branch Transfer (Atomic 2-Phase Commit with compensating rollback)
        srcAcc.getLock().lock();
        try {
            if (srcAcc.getBalance() < amount) {
                int failTime = lamportClock.tick();
                Transaction failTx = new Transaction(txId, Transaction.Type.TRANSFER,
                        fromAccount, toAccount, amount, failTime, nodeId, "FAILED_INSUFFICIENT_FUNDS",
                        "Insufficient balance in " + fromAccount);
                recordAndSync(failTx);
                return failTx;
            }

            // Phase 1: Debit locally
            srcAcc.withdraw(amount);
            int midLTime = lamportClock.tick();

            // Phase 2: Remote invocation to credit target branch
            BankInterface targetPeer = getPeerNode(targetBranch);
            if (targetPeer == null) {
                // Compensating rollback
                srcAcc.deposit(amount);
                int rollbackTime = lamportClock.tick();
                Transaction rollbackTx = new Transaction(txId, Transaction.Type.TRANSFER,
                        fromAccount, toAccount, amount, rollbackTime, nodeId, "FAILED_TARGET_UNREACHABLE",
                        "Target Branch " + targetBranch + " offline. Local debit rolled back.");
                recordAndSync(rollbackTx);
                return rollbackTx;
            }

            try {
                boolean credited = targetPeer.internalCredit(toAccount, amount, midLTime);
                if (credited) {
                    int finalTime = lamportClock.tick();
                    Transaction commitTx = new Transaction(txId, Transaction.Type.TRANSFER,
                            fromAccount, toAccount, amount, finalTime, nodeId, "COMMITTED",
                            String.format("Cross-branch transfer $%.2f from Node %d (%s) to Node %d (%s)",
                                    amount, nodeId, fromAccount, targetBranch, toAccount));
                    System.out.printf("[%s | L-Time: %2d] CROSS-BRANCH TRANSFER COMMITTED: %s (Branch %d) -> %s (Branch %d) [$%.2f]%n",
                            branchName, finalTime, fromAccount, nodeId, toAccount, targetBranch, amount);
                    recordAndSync(commitTx);
                    return commitTx;
                } else {
                    // Target account not found on remote branch, rollback
                    srcAcc.deposit(amount);
                    int rollbackTime = lamportClock.tick();
                    Transaction rollbackTx = new Transaction(txId, Transaction.Type.TRANSFER,
                            fromAccount, toAccount, amount, rollbackTime, nodeId, "FAILED_REMOTE_REJECT",
                            "Target branch rejected credit. Local debit rolled back.");
                    recordAndSync(rollbackTx);
                    return rollbackTx;
                }
            } catch (RemoteException re) {
                // Network failure during remote call -> rollback local balance
                srcAcc.deposit(amount);
                int rollbackTime = lamportClock.tick();
                Transaction rollbackTx = new Transaction(txId, Transaction.Type.TRANSFER,
                        fromAccount, toAccount, amount, rollbackTime, nodeId, "FAILED_NETWORK_ROLLBACK",
                        "RMI failure to Branch " + targetBranch + ". Local debit rolled back safely.");
                recordAndSync(rollbackTx);
                return rollbackTx;
            }
        } finally {
            srcAcc.getLock().unlock();
        }
    }

    // =========================================================================
    //  Inter-Branch Internal Synchronization
    // =========================================================================

    @Override
    public boolean internalCredit(String accountId, double amount, int remoteLamportTime) throws RemoteException {
        int lTime = lamportClock.updateAndTick(remoteLamportTime);
        Account account = localAccounts.get(accountId);
        if (account != null) {
            account.deposit(amount);
            System.out.printf("[%s | L-Time: %2d] Remote Credit applied: %s +$%.2f (New Bal: $%.2f)%n",
                    branchName, lTime, accountId, amount, account.getBalance());
            return true;
        }
        return false;
    }

    @Override
    public boolean internalDebit(String accountId, double amount, int remoteLamportTime) throws RemoteException {
        int lTime = lamportClock.updateAndTick(remoteLamportTime);
        Account account = localAccounts.get(accountId);
        if (account != null) {
            boolean ok = account.withdraw(amount);
            System.out.printf("[%s | L-Time: %2d] Remote Debit applied: %s -$%.2f (Success: %b)%n",
                    branchName, lTime, accountId, amount, ok);
            return ok;
        }
        return false;
    }

    @Override
    public void syncTransaction(Transaction tx) throws RemoteException {
        if (tx != null) {
            lamportClock.updateAndTick(tx.getLamportTimestamp());
            recordLocalTransaction(tx);
            if (isLeader && coordinatorManager != null) {
                coordinatorManager.recordTransaction(tx);
            }
        }
    }

    @Override
    public List<Transaction> getAuditLog() throws RemoteException {
        List<Transaction> copy = new ArrayList<>(localAuditLog);
        Collections.sort(copy);
        return copy;
    }

    @Override
    public List<Transaction> getGlobalTransactionLog() throws RemoteException {
        if (isLeader && coordinatorManager != null) {
            return coordinatorManager.getOrderedGlobalLog();
        }
        // If not coordinator, query the active leader
        BankInterface leader = getPeerNode(currentLeaderId);
        if (leader != null) {
            try {
                return leader.getGlobalTransactionLog();
            } catch (RemoteException ignored) {
                // Fall back to local log if leader unreachable
            }
        }
        return getAuditLog();
    }

    private void recordLocalTransaction(Transaction tx) {
        boolean exists = localAuditLog.stream()
                .anyMatch(t -> t.getTransactionId().equals(tx.getTransactionId()));
        if (!exists) {
            localAuditLog.add(tx);
        }
    }

    private void recordAndSync(Transaction tx) {
        recordLocalTransaction(tx);

        // If this node is the leader, update CoordinatorManager and broadcast
        if (isLeader && coordinatorManager != null) {
            coordinatorManager.recordTransaction(tx);
            Map<Integer, BankInterface> peers = getAllActivePeers();
            coordinatorManager.broadcastSyncToPeers(tx, peers);
        } else {
            // Forward sync to current coordinator
            try {
                BankInterface leader = getPeerNode(currentLeaderId);
                if (leader != null) {
                    leader.syncTransaction(tx);
                }
            } catch (RemoteException re) {
                System.err.printf("[%s] Notice: Failed to sync Tx %s to Coordinator %d (Offline). Will reconcile upon new election.%n",
                        branchName, tx.getTransactionId().substring(0, 8), currentLeaderId);
            }
        }
    }

    // =========================================================================
    //  Bully Election Algorithm & Fault Tolerance
    // =========================================================================

    @Override
    public boolean pingLeader() throws RemoteException {
        // Return true if alive and healthy
        return true;
    }

    @Override
    public boolean electLeader(int senderNodeId) throws RemoteException {
        System.out.printf("[%s | L-Time: %2d] [BULLY] Received ELECTION message from Node %d.%n",
                branchName, lamportClock.tick(), senderNodeId);

        if (this.nodeId > senderNodeId) {
            // Asynchronously start election to claim higher leadership
            CompletableFuture.runAsync(this::startBullyElection);
            // Respond with ACK / "OK" to sender
            return true;
        }
        return false;
    }

    @Override
    public void announceLeader(int leaderNodeId) throws RemoteException {
        this.currentLeaderId = leaderNodeId;
        lamportClock.tick();

        if (leaderNodeId == this.nodeId) {
            becomeCoordinator();
            System.out.printf("[%s] [BULLY] *** VICTORY! Node %d is now the Primary Coordinator! ***%n",
                    branchName, this.nodeId);
        } else {
            this.isLeader = false;
            this.coordinatorManager = null;
            System.out.printf("[%s] [BULLY] Acknowledged: Node %d is the new Coordinator/Leader.%n",
                    branchName, leaderNodeId);
        }
        electionInProgress.set(false);
    }

    /**
     * Executes the Bully Algorithm election protocol:
     * 1. If this node has the highest ID in the entire cluster, win immediately.
     * 2. Otherwise, send ELECTION messages to all nodes with ID > this.nodeId.
     * 3. If no higher node responds with ACK within timeout, declare victory and broadcast COORDINATOR.
     * 4. If a higher node responds, wait for its COORDINATOR announcement.
     */
    public synchronized void startBullyElection() {
        if (!electionInProgress.compareAndSet(false, true)) {
            return; // Election already underway
        }

        System.out.printf("%n======================================================================%n");
        System.out.printf("[%s | L-Time: %2d] [BULLY] INITIATING ELECTION (Current Node ID: %d)...%n",
                branchName, lamportClock.tick(), nodeId);
        System.out.printf("======================================================================%n");

        List<Integer> higherNodes = new ArrayList<>();
        for (int peerId : clusterNodeIds) {
            if (peerId > this.nodeId) {
                higherNodes.add(peerId);
            }
        }

        if (higherNodes.isEmpty()) {
            // Case 1: Node has highest ID in cluster -> Instant victory
            proclaimVictory();
            return;
        }

        // Case 2: Contact all higher nodes
        boolean higherNodeResponded = false;
        for (int higherId : higherNodes) {
            try {
                System.out.printf("[%s] [BULLY] Sending ELECTION message to higher Node %d...%n", branchName, higherId);
                BankInterface peer = getPeerNode(higherId);
                if (peer != null) {
                    boolean ack = peer.electLeader(this.nodeId);
                    if (ack) {
                        System.out.printf("[%s] [BULLY] Received ACK from higher Node %d.%n", branchName, higherId);
                        higherNodeResponded = true;
                    }
                }
            } catch (Exception e) {
                System.out.printf("[%s] [BULLY] Higher Node %d is offline or unreachable.%n", branchName, higherId);
            }
        }

        if (!higherNodeResponded) {
            // No higher-ID nodes responded: declare victory
            System.out.printf("[%s] [BULLY] No higher node responded! Taking leadership.%n", branchName);
            proclaimVictory();
        } else {
            // Wait for higher node to announce coordinator
            System.out.printf("[%s] [BULLY] Higher node responded. Waiting for COORDINATOR announcement...%n", branchName);
            CompletableFuture.delayedExecutor(4, TimeUnit.SECONDS).execute(() -> {
                if (electionInProgress.get()) {
                    System.out.printf("[%s] [BULLY] Timeout waiting for announcement. Re-initiating election...%n", branchName);
                    electionInProgress.set(false);
                    startBullyElection();
                }
            });
        }
    }

    private void proclaimVictory() {
        becomeCoordinator();
        broadcastAnnouncement(this.nodeId);
        electionInProgress.set(false);
    }

    private void becomeCoordinator() {
        this.isLeader = true;
        this.currentLeaderId = this.nodeId;
        if (this.coordinatorManager == null) {
            this.coordinatorManager = new CoordinatorManager(this.nodeId);
        }
        // Merge local transactions into global coordinator log
        this.coordinatorManager.mergeLogs(this.localAuditLog);

        // Reconcile and pull audit logs from all reachable active peers
        for (int peerId : clusterNodeIds) {
            if (peerId == this.nodeId) continue;
            try {
                BankInterface peer = getPeerNode(peerId);
                if (peer != null) {
                    this.coordinatorManager.mergeLogs(peer.getAuditLog());
                }
            } catch (Exception ignored) {
                // Peer unreachable
            }
        }

        System.out.printf("[%s] *** PROMOTED TO COORDINATOR. Global Log Reconciled with %d records. ***%n",
                branchName, this.coordinatorManager.getOrderedGlobalLog().size());
    }

    private void broadcastAnnouncement(int leaderId) {
        System.out.printf("[%s] [BULLY] Broadcasting COORDINATOR(%d) to all peer nodes...%n", branchName, leaderId);
        for (int peerId : clusterNodeIds) {
            if (peerId == this.nodeId) continue;
            try {
                BankInterface peer = getPeerNode(peerId);
                if (peer != null) {
                    peer.announceLeader(leaderId);
                }
            } catch (Exception e) {
                // Peer down
            }
        }
    }

    /**
     * Periodic Heartbeat task checking coordinator liveness every 3 seconds.
     */
    private void startHeartbeatMonitor() {
        heartbeatScheduler.scheduleAtFixedRate(() -> {
            try {
                if (isLeader) {
                    return; // Leader does not ping itself
                }

                BankInterface leader = getPeerNode(currentLeaderId);
                if (leader == null) {
                    System.out.printf("[%s] [HEARTBEAT] Coordinator Node %d not found in registry! Triggering election...%n",
                            branchName, currentLeaderId);
                    startBullyElection();
                    return;
                }

                // Ping coordinator
                boolean alive = leader.pingLeader();
                if (!alive) {
                    System.out.printf("[%s] [HEARTBEAT] Coordinator Node %d ping failed! Triggering election...%n",
                            branchName, currentLeaderId);
                    startBullyElection();
                }
            } catch (RemoteException re) {
                System.out.printf("[%s] [HEARTBEAT] Coordinator Node %d is UNREACHABLE (%s)! Initiating Bully Election...%n",
                        branchName, currentLeaderId, re.getMessage());
                startBullyElection();
            } catch (Exception ex) {
                // Catch any lookup or unexpected error
            }
        }, 3, 3, TimeUnit.SECONDS);
    }

    // =========================================================================
    //  Cluster Routing & Discovery Helpers
    // =========================================================================

    private BankInterface getPeerNode(int targetNodeId) {
        try {
            Registry registry = LocateRegistry.getRegistry("localhost", rmiPort);
            return (BankInterface) registry.lookup("BankNode_" + targetNodeId);
        } catch (Exception e) {
            return null;
        }
    }

    private Map<Integer, BankInterface> getAllActivePeers() {
        Map<Integer, BankInterface> peers = new HashMap<>();
        for (int id : clusterNodeIds) {
            if (id == this.nodeId) continue;
            BankInterface peer = getPeerNode(id);
            if (peer != null) {
                peers.put(id, peer);
            }
        }
        return peers;
    }

    /**
     * Resolves which branch node owns an account based on account ID naming convention:
     * ACC-1xx -> Branch 1
     * ACC-2xx -> Branch 2
     * ACC-3xx -> Branch 3
     */
    public static int resolveNodeIdForAccount(String accountId) {
        if (accountId != null && accountId.startsWith("ACC-")) {
            char branchChar = accountId.charAt(4);
            if (Character.isDigit(branchChar)) {
                return Character.getNumericValue(branchChar);
            }
        }
        return 1; // Default fallback
    }

    @Override
    public int getNodeId() throws RemoteException {
        return nodeId;
    }

    @Override
    public String getBranchName() throws RemoteException {
        return branchName;
    }

    @Override
    public int getCurrentLeaderId() throws RemoteException {
        return currentLeaderId;
    }

    @Override
    public int getLamportTime() throws RemoteException {
        return lamportClock.getValue();
    }

    // =========================================================================
    //  Bootstrap / Main Method
    // =========================================================================

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Usage: java BankNode <NodeID> [BranchName] [RmiPort]");
            System.out.println("Example:");
            System.out.println("  java BankNode 1 \"Downtown Branch\" 1099");
            System.out.println("  java BankNode 2 \"Uptown Branch\"   1099");
            System.out.println("  java BankNode 3 \"Metro Branch\"    1099");
            System.exit(1);
        }

        try {
            int nodeId = Integer.parseInt(args[0]);
            String branchName = args.length > 1 ? args[1] : "Branch-" + nodeId;
            int port = args.length > 2 ? Integer.parseInt(args[2]) : 1099;

            List<Integer> cluster = Arrays.asList(1, 2, 3);

            // Locate or bootstrap shared RMI Registry on the specified port
            Registry registry;
            try {
                registry = LocateRegistry.createRegistry(port);
                System.out.printf("[RMI] Created embedded RMI Registry on port %d.%n", port);
            } catch (ExportException e) {
                registry = LocateRegistry.getRegistry("localhost", port);
                System.out.printf("[RMI] Connected to existing RMI Registry on port %d.%n", port);
            }

            // Create and bind BankNode instance
            BankNode node = new BankNode(nodeId, branchName, port, cluster);
            String serviceName = "BankNode_" + nodeId;
            registry.rebind(serviceName, node);

            System.out.println("=================================================================");
            System.out.printf("  BANK NODE %d: [%s] READY%n", nodeId, branchName);
            System.out.printf("  Bound in RMI Registry as: rmi://localhost:%d/%s%n", port, serviceName);
            System.out.printf("  Initial Lamport Clock: 0%n");
            System.out.printf("  Initial Presumed Coordinator: Node %d%n", node.currentLeaderId);
            System.out.println("=================================================================");
            System.out.println("Hosted Accounts on this branch:");
            node.localAccounts.values().forEach(acc -> System.out.println("  - " + acc));
            System.out.println("-----------------------------------------------------------------");

            // Start heartbeat failure detector
            node.startHeartbeatMonitor();

            // Check if presumed coordinator is alive on boot; if not, trigger election
            try {
                BankInterface currentCoord = node.getPeerNode(node.currentLeaderId);
                if (currentCoord == null || !currentCoord.pingLeader()) {
                    System.out.printf("[%s] Presumed coordinator Node %d not responsive. Starting election...%n",
                            branchName, node.currentLeaderId);
                    node.startBullyElection();
                } else {
                    System.out.printf("[%s] Successfully connected to active Coordinator Node %d.%n",
                            branchName, node.currentLeaderId);
                }
            } catch (Exception e) {
                node.startBullyElection();
            }

            System.out.println("\nNode is active and serving transactions. Press Ctrl+C in this terminal to simulate node failure.");

        } catch (Exception e) {
            System.err.println("Error initializing BankNode: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
