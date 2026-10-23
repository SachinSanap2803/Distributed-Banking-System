import java.io.Serializable;

/**
 * LamportClock.java
 * Thread-safe implementation of Lamport's Logical Clock.
 * Enforces causal event ordering across distributed nodes:
 * 1. Tick locally before sending or on internal events: C_i = C_i + 1
 * 2. On message receive with timestamp T_msg: C_i = max(C_i, T_msg) + 1
 */
public class LamportClock implements Serializable {
    private static final long serialVersionUID = 1L;

    private int value;

    public LamportClock() {
        this.value = 0;
    }

    public LamportClock(int initialValue) {
        this.value = initialValue;
    }

    /**
     * Reads the current clock value.
     */
    public synchronized int getValue() {
        return value;
    }

    /**
     * Increments the clock for a local event or before sending an RMI message.
     * @return New Lamport timestamp.
     */
    public synchronized int tick() {
        value++;
        return value;
    }

    /**
     * Updates the clock upon receiving a remote message with a timestamp.
     * Applies Lamport's rule: C_local = max(C_local, T_received) + 1
     * @param receivedTimestamp Timestamp received from the remote caller/message.
     * @return Updated Lamport timestamp.
     */
    public synchronized int updateAndTick(int receivedTimestamp) {
        value = Math.max(value, receivedTimestamp) + 1;
        return value;
    }

    @Override
    public synchronized String toString() {
        return "LamportTime=" + value;
    }
}
