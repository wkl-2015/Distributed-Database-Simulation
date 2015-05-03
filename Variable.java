
import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;

/**
 * Created by hongdaj on 11/26/14.
 */

/**
 *  Variable is a value stored in site, e.g., X1. Several transactions may hold
 *  read-only (shared) locks on the variable but only one transaction at time 
 *  can hold a write (exclusive) lock on the variable.
 */
public class Variable {
    // id is an int number from 1 - 20
    private int id;
    private int value;
    private int pendingValue;
    // the last time that this variable is written
    private long lastWrittenTime;
    private List<Lock> locks;
    private boolean readable;


    public Variable(int id, long time) {
        this.id = id;
        //Each variable xi is initialized to the value 10i
        this.value = 10 * id;
        this.pendingValue = value;
        this.lastWrittenTime = time;
        this.locks = new ArrayList<Lock>();
        this.readable = true;
    }

    public boolean addLock(LockType locktype, int transactionId) {
        if (locks.size() != 0 && locks.get(0).getLockType() == LockType.WRITE) {
            return false;
        } else {
            locks.add(new Lock(locktype, transactionId));
            return true;
        }
    }

    public void setPendingValue(int value){
        this.pendingValue = value;
    }

    public void writePendingValueToValue(){
        if(!readable){
            setReadable();
        }
        this.value = this.pendingValue;
    }

    protected void clearLocks() {
        locks.clear();
    }

    protected void removeLockByTransaction(int tId) {
        Iterator<Lock> iter = locks.iterator();
        while (iter.hasNext()) {
            if (iter.next().getTransactionId() == tId) {
                iter.remove();
                break;
            }
        }
    }

    public LockType getLockType(){
        if (locks.size() == 0) {
            return null;
        } else {
            return locks.get(0).getLockType();
        }
    }

    public boolean hasNoLock() {
        return getLockType() == null;
    }

    public boolean hasLockOfTransaction(int tId) {
        for (Lock lock : locks) {
            if (lock.getTransactionId() == tId) {
                return true;
            }
        }
        return false;
    }

    public List<Integer> getLockOwners(){
        List<Integer> ownerIds = new ArrayList<Integer>();
        for (Lock lock : locks) {
            ownerIds.add(lock.getTransactionId());
        }
        return ownerIds;
    }

    public int getWriteLockTransactionId() {
        if (locks.size() != 0 && locks.get(0).getLockType() == LockType.WRITE) {
            return locks.get(0).getTransactionId();
        } else {
            throw new RuntimeException(
                "Not write lock in getWriteLockTransactionId"
            );
        }
    }

    public int getId() {
        return this.id;
    }

    protected void setValue(int value){
        this.value = value;
    }

    public void setReadable(){
        this.readable = true;
    }

    public void setUnReadable(){
        this.readable = false;
    }

    public boolean isReadable(){
        return this.readable;
    }

    public int getValue() {
        return this.value;
    }

    public long getLastWrittenTime() {
        return this.lastWrittenTime;
    }
}
