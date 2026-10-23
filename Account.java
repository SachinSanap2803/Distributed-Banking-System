import java.io.Serializable;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Account.java
 * Represents a bank account with fine-grained concurrency control.
 * Uses ReentrantLock to guarantee thread-safe balance updates and prevent race conditions.
 */
public class Account implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String accountId;
    private final String holderName;
    private double balance;
    private final transient ReentrantLock lock = new ReentrantLock(true); // Fair lock for thread safety

    public Account(String accountId, String holderName, double initialBalance) {
        if (initialBalance < 0) {
            throw new IllegalArgumentException("Initial balance cannot be negative.");
        }
        this.accountId = accountId;
        this.holderName = holderName;
        this.balance = initialBalance;
    }

    public String getAccountId() {
        return accountId;
    }

    public String getHolderName() {
        return holderName;
    }

    /**
     * Retrieves the current balance in a thread-safe manner.
     */
    public double getBalance() {
        lock.lock();
        try {
            return balance;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Deposits funds into the account safely.
     * @param amount Amount to deposit (must be positive).
     * @return New balance after deposit.
     */
    public double deposit(double amount) {
        if (amount <= 0) {
            throw new IllegalArgumentException("Deposit amount must be strictly positive.");
        }
        lock.lock();
        try {
            balance += amount;
            return balance;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Withdraws funds from the account safely.
     * @param amount Amount to withdraw.
     * @return true if withdrawal succeeded, false if insufficient balance.
     */
    public boolean withdraw(double amount) {
        if (amount <= 0) {
            throw new IllegalArgumentException("Withdrawal amount must be strictly positive.");
        }
        lock.lock();
        try {
            if (balance >= amount) {
                balance -= amount;
                return true;
            }
            return false;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Provides access to the lock for coordinated multi-account transactions (e.g., transfers).
     */
    public ReentrantLock getLock() {
        return lock;
    }

    @Override
    public String toString() {
        return String.format("[%s] %s | Balance: $%.2f", accountId, holderName, getBalance());
    }
}
