/**
 * Created by hongdaj on 11/26/14.
 */

/**
 * a Lock object represents a lock, read or write
 * It contains information like lock type, who owns this lock
 */
public class Lock {
    private LockType locktype;
    // id of transaction who owns this lock
    private int transactionId;

    public Lock(LockType lockType, int transactionId){
        this.locktype = lockType;
        this.transactionId = transactionId;
    }

    public LockType getLockType(){
        return this.locktype;
    }

    public int getTransactionId(){
        return this.transactionId;
    }

    public boolean setLockType(LockType locktype) {
        if (this.locktype == LockType.READ || this.locktype == LockType.WRITE) {
            return false;
        } else {
            this.locktype = locktype;
            return true;
        }
    }
}
