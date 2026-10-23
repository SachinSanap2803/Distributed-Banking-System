import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Objects;

/**
 * Transaction.java
 * Represents an immutable distributed banking transaction record.
 * Implements Comparable to enforce total causal ordering via Lamport timestamps
 * with Node ID tie-breaking.
 */
public class Transaction implements Serializable, Comparable<Transaction> {
    private static final long serialVersionUID = 1L;

    public enum Type {
        DEPOSIT, WITHDRAW, TRANSFER, BALANCE_INQUIRY
    }

    private final String transactionId;
    private final Type type;
    private final String sourceAccount;
    private final String targetAccount;
    private final double amount;
    private final int lamportTimestamp;
    private final int originNodeId;
    private final String status;
    private final long wallClockTime;
    private final String details;

    public Transaction(String transactionId, Type type, String sourceAccount,
                       String targetAccount, double amount, int lamportTimestamp,
                       int originNodeId, String status, String details) {
        this.transactionId = transactionId;
        this.type = type;
        this.sourceAccount = sourceAccount != null ? sourceAccount : "N/A";
        this.targetAccount = targetAccount != null ? targetAccount : "N/A";
        this.amount = amount;
        this.lamportTimestamp = lamportTimestamp;
        this.originNodeId = originNodeId;
        this.status = status;
        this.details = details != null ? details : "";
        this.wallClockTime = System.currentTimeMillis();
    }

    public String getTransactionId() {
        return transactionId;
    }

    public Type getType() {
        return type;
    }

    public String getSourceAccount() {
        return sourceAccount;
    }

    public String getTargetAccount() {
        return targetAccount;
    }

    public double getAmount() {
        return amount;
    }

    public int getLamportTimestamp() {
        return lamportTimestamp;
    }

    public int getOriginNodeId() {
        return originNodeId;
    }

    public String getStatus() {
        return status;
    }

    public long getWallClockTime() {
        return wallClockTime;
    }

    public String getDetails() {
        return details;
    }

    /**
     * Total Causal Order defined by:
     * 1. Ascending Lamport Timestamp
     * 2. Tie-break with Node ID (lower node ID ordered first or consistent deterministic order)
     * 3. Fallback to transaction ID
     */
    @Override
    public int compareTo(Transaction other) {
        if (this.lamportTimestamp != other.lamportTimestamp) {
            return Integer.compare(this.lamportTimestamp, other.lamportTimestamp);
        }
        if (this.originNodeId != other.originNodeId) {
            return Integer.compare(this.originNodeId, other.originNodeId);
        }
        return this.transactionId.compareTo(other.transactionId);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Transaction that = (Transaction) o;
        return Objects.equals(transactionId, that.transactionId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(transactionId);
    }

    @Override
    public String toString() {
        SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss.SSS");
        String formattedTime = sdf.format(new Date(wallClockTime));
        return String.format("[L-Time: %2d | Node: %d | %s] Tx: %-8s | %-8s | Src: %-7s | Dst: %-7s | $%-8.2f | Status: %-10s | %s",
                lamportTimestamp, originNodeId, formattedTime,
                transactionId.substring(0, Math.min(8, transactionId.length())),
                type, sourceAccount, targetAccount, amount, status, details);
    }
}
