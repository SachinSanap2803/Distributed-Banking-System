import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

/**
 * BankInterface.java
 * Remote interface declaring RMI operations for:
 * 1. Banking transactions (deposit, withdraw, balance inquiry, cross-branch transfer)
 * 2. Lamport logical clock synchronization
 * 3. Bully leader election and fault tolerance
 * 4. Audit logging across distributed branch nodes
 */
public interface BankInterface extends Remote {

    // --- Client Banking Operations ---

    /**
     * Inquires the balance of an account.
     */
    double getBalance(String accountId, int clientLamportTime) throws RemoteException;

    /**
     * Performs a deposit operation on a target account.
     */
    Transaction deposit(String accountId, double amount, int clientLamportTime) throws RemoteException;

    /**
     * Performs a withdrawal operation on a source account.
     */
    Transaction withdraw(String accountId, double amount, int clientLamportTime) throws RemoteException;

    /**
     * Performs an atomic fund transfer between two accounts (local or cross-branch).
     */
    Transaction transfer(String fromAccount, String toAccount, double amount, int clientLamportTime) throws RemoteException;


    // --- Inter-Branch Internal Synchronization ---

    /**
     * Remotely credits an account during a cross-branch transfer.
     */
    boolean internalCredit(String accountId, double amount, int lamportTime) throws RemoteException;

    /**
     * Remotely debits an account during a cross-branch transfer.
     */
    boolean internalDebit(String accountId, double amount, int lamportTime) throws RemoteException;

    /**
     * Synchronizes a transaction record to the coordinator or peer audit log.
     */
    void syncTransaction(Transaction tx) throws RemoteException;

    /**
     * Returns the local branch transaction audit log.
     */
    List<Transaction> getAuditLog() throws RemoteException;

    /**
     * Returns the globally ordered transaction log maintained by the coordinator.
     */
    List<Transaction> getGlobalTransactionLog() throws RemoteException;


    // --- Bully Leader Election & Fault Tolerance ---

    /**
     * Heartbeat check sent to the active coordinator/leader.
     * @return true if leader is alive and operational.
     */
    boolean pingLeader() throws RemoteException;

    /**
     * Bully Algorithm election message sent to higher-ID nodes.
     * @param senderNodeId Node ID of the node initiating or relaying the election.
     * @return true (ACK / "OK") indicating a higher-ID node is alive and will take over.
     */
    boolean electLeader(int senderNodeId) throws RemoteException;

    /**
     * Bully Algorithm victory proclamation broadcast by the newly elected leader.
     * @param leaderNodeId Node ID of the newly crowned coordinator.
     */
    void announceLeader(int leaderNodeId) throws RemoteException;


    // --- Node Metadata ---

    int getNodeId() throws RemoteException;

    String getBranchName() throws RemoteException;

    int getCurrentLeaderId() throws RemoteException;

    int getLamportTime() throws RemoteException;
}
