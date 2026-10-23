import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * CoordinatorManager.java
 * Coordinates cross-branch validation and maintains the synchronized global transaction log.
 * Activated on the elected primary/coordinator node.
 */
public class CoordinatorManager implements Serializable {
    private static final long serialVersionUID = 1L;

    private final int coordinatorNodeId;
    private final List<Transaction> globalTransactionLog;

    public CoordinatorManager(int coordinatorNodeId) {
        this.coordinatorNodeId = coordinatorNodeId;
        this.globalTransactionLog = new CopyOnWriteArrayList<>();
    }

    public int getCoordinatorNodeId() {
        return coordinatorNodeId;
    }

    /**
     * Records a validated transaction into the global log.
     * Guarantees thread-safe storage.
     */
    public synchronized void recordTransaction(Transaction tx) {
        if (tx != null) {
            // Deduplicate if already present by transactionId
            boolean exists = globalTransactionLog.stream()
                    .anyMatch(t -> t.getTransactionId().equals(tx.getTransactionId()));
            if (!exists) {
                globalTransactionLog.add(tx);
                System.out.printf("[Coordinator %d] Global Log Recorded: Tx %s (Lamport=%d, Status=%s)%n",
                        coordinatorNodeId, tx.getTransactionId().substring(0, Math.min(8, tx.getTransactionId().length())),
                        tx.getLamportTimestamp(), tx.getStatus());
            }
        }
    }

    /**
     * Broadcasts a transaction to all connected peer branches so their local logs remain synced.
     */
    public void broadcastSyncToPeers(Transaction tx, Map<Integer, BankInterface> peers) {
        if (tx == null || peers == null) return;

        for (Map.Entry<Integer, BankInterface> entry : peers.entrySet()) {
            int peerId = entry.getKey();
            BankInterface peer = entry.getValue();

            // Do not sync back to the coordinator or origin node if it already recorded it
            if (peerId == coordinatorNodeId || peerId == tx.getOriginNodeId()) {
                continue;
            }

            try {
                peer.syncTransaction(tx);
            } catch (RemoteException e) {
                System.err.printf("[Coordinator %d] Warning: Failed to sync Tx %s to Branch Node %d: %s%n",
                        coordinatorNodeId, tx.getTransactionId(), peerId, e.getMessage());
            }
        }
    }

    /**
     * Returns an immutable copy of all global transactions sorted by total causal order
     * (Lamport Logical Timestamp, then Node ID tie-breaker).
     */
    public synchronized List<Transaction> getOrderedGlobalLog() {
        List<Transaction> copy = new ArrayList<>(globalTransactionLog);
        Collections.sort(copy);
        return Collections.unmodifiableList(copy);
    }

    /**
     * Imports existing transactions when a new node is promoted to coordinator.
     */
    public synchronized void mergeLogs(List<Transaction> existingLogs) {
        if (existingLogs != null) {
            for (Transaction tx : existingLogs) {
                recordTransaction(tx);
            }
        }
    }
}
