import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;
import java.util.Queue;
import java.util.LinkedList;

/**
 * Created by hongdaj on 11/26/14.
 */

/**
 * TM is the main class of this project, it receives commands from transactions 
 * and decides whether requests can be done
 */
public class TransactionManager {
    private static long time = 0;
    private List<Transaction> transactions;
    // TODO: (optional) change the read location of sites to database manager.
    private List<Site> sites;
    private List<OldCopy> oldCopies;
    private DataManager DM;
    private Queue<Message> todoBuffer;
    private List<Message> pendingWrite;

    public TransactionManager(){
        transactions = new ArrayList<Transaction>();
        oldCopies = new ArrayList<OldCopy>();
        sites = new ArrayList<Site>();
        initializeSites(10);
        DM = new DataManager(this.sites);
        todoBuffer = new LinkedList<Message>();
        pendingWrite = new ArrayList<Message>();
    }

    private void initializeSites(int n){
        for (int i = 1; i < n + 1; i++) {
            sites.add(new Site(i));
        }
    }

    private static void timeTick(){
        time++;
    }

    public static long getTime(){
        return time;
    }

    /**
     *  Execute messages from a new line.
     *  Operations in todoBuffer should be processed first.
     *  @param newMessages  Messages from a new line of input.
     */
    public void execute(List<Message> newMessages){
        System.out.println();
        System.out.println("===== Time: " + time);
        if (newMessages == null) {
            return;
        }
        // Append the newMessages from the new line to the end of todoBuffer.
        for (Message msg : newMessages) {
            todoBuffer.add(msg);
        }
        int bufferSize = todoBuffer.size();
        for(int i = 0; i < bufferSize; i++){
            Message msg = todoBuffer.poll();
            System.out.println("--> " + msg.rawMessageString);
            switch(msg.operation) {
                case BEGIN:
                    begin(msg);
                    break;
                case BEGINRO:
                    beginRO(msg);
                    break;
                case R:
                    read(msg);
                    break;
                case W:
                    write(msg);
                    break;
                case DUMPALL:
                    dump(msg);
                    break;
                case DUMPSITE:
                    dump(msg);
                    break;
                case DUMPVARIABLE:
                    dump(msg);
                    break;
                case END:
                    end(msg);
                    break;
                case FAIL:
                    fail(msg);
                    break;
                case RECOVER:
                    recover(msg);
                    break;
            }
        }
        timeTick();
    }


    private void begin(Message message){
        Transaction tr = new Transaction(message.transactionId, false, time);
        transactions.add(tr);
        System.out.println("Transaction " + message.transactionId + " begins!");
    }

    /**
     *  A read only transaction need a copy of the variables at the time the
     *  transaction begins. Thus it can read consistent value without worry
     *  about the variables being modified because of the lack of read lock.
     *  @param message  The message to process or to be put into todo buffer.
     */
    private void beginRO(Message message){
        // If all sites failed, old copy can't be created.
        // Then delay the transaction.
        if (DM.isAllSiteDown()) {
            todoBuffer.add(message);
            System.out.println("T" + message.transactionId + " waits");
        } else {
            OldCopy copy = createOldCopy(time);
            oldCopies.add(copy);
            Transaction tr = new Transaction(message.transactionId, true, time);
            transactions.add(tr);
        }
        System.out.println("Begin-only transaction " + message.transactionId + " begins!");
    }

    /**
     *  A read-only transaction reads its old copy, provided that not all sites
     *  are down when reading.
     *  A read-write transaction tries to read by: Check if at least 1 site is
     *  up. Obtain at least 1 lock from any up site. If successful, read the
     *  value. If it can't, check begin time with conflicting transaction, and
     *  add to todoBuffer.
     *  @param message  The message to process or to be put into todoBuffer.
     */
    private void read(Message message){
        if(!isTransactionInList(message.transactionId)){
            return;
        }
        // if the site containing the odd variable is down, throw this message
        // into todoBuffer
        if(message.variable % 2 == 1){
            int siteId = message.variable % 10 + 1;
            if(!sites.get(siteId - 1).isUp()){
                todoBuffer.add(message);
                System.out.println("T" + message.transactionId + " waits");
                return;
            }
        }
        else if (DM.isAllSiteDown()) {
            todoBuffer.add(message);
            System.out.println("T" + message.transactionId + " waits");
            return;
        }
        Transaction transaction = getTransactionById(message.transactionId);
        if (transaction.isReadOnly()) {
            // A read-only transaction can read its old copy.
            OldCopy copy = getOldCopy(transaction.getBeginTime());
            int value = copy.getValueOfVariable(message.variable);
            System.out.println("x" + message.variable + "=" + value);
            return;
        } else {
            // A read-write transaction.
            // Check if this variable are unreadable on all sites.
            if (message.variable % 2 == 0) {
                // Possibly when all sites are recovered from failure.
                boolean unreadableOnAllSite = true;
                for (Site site : sites) {
                    if(site.getVar(message.variable).isReadable()) {
                        unreadableOnAllSite = false;
                    }
                }
                if (unreadableOnAllSite) {
                    todoBuffer.add(message);
                    System.out.println("T" + message.transactionId + " waits");
                    return;
                }
            }
            // A read-write transaction tries to read by obtain lock.
            for (Site site : sites) {
                if (site.containsVar(message.variable)) {
                    Variable var = site.getVar(message.variable);
                    if (var.addLock(LockType.READ, message.transactionId)) {
                        int value = DM.read(message.variable);
                        System.out.println("x" + message.variable + "=" + value);
                        return;
                    }
                }
            }
            // If it can't obtain any lock. Then it must be all write locks.
            for (Site site : sites) {
                if (site.containsVar(message.variable)) {
                    // TODO: unify index to be {1, 2, ...}.
                    // TODO: check containsVar before getVar.
                    Variable var = site.getVar(message.variable);
                    int tid = var.getWriteLockTransactionId();
                    Transaction tr = getTransactionById(tid);
                    // "We'll ensure that no 2 transactions will have the same age".
                    if (tr.getBeginTime() < transaction.getBeginTime()) {
                        // Die.
                        abort(message.transactionId);
                        return;
                    } else {
                        // Wait.
                        todoBuffer.add(message);
                        System.out.println("T" + message.transactionId + " waits");
                        return;
                    }
                }
            }
        }
    }


    private void write(Message message){
        if(!isTransactionInList(message.transactionId)){
            return;
        }
        if (DM.isAllSiteDown()) {
            todoBuffer.add(message);
            System.out.println("T" + message.transactionId + " waits");
            return;
        }
        boolean addToTodo = true;
        int tId = message.transactionId;
        Transaction t = getTransactionById(tId);
        for (Site site : sites) {
            if (site.isUp()) {
                if (!site.containsVar(message.variable)) {
                    continue;
                }
                Variable var = site.getVar(message.variable);
                if (var.hasNoLock()) {
                    var.addLock(LockType.WRITE, tId);
                } else if (var.hasLockOfTransaction(tId)){
                    if (var.getLockType() == LockType.READ) {
                        // The transaction has a read lock on the var already.
                        var.removeLockByTransaction(tId);
                        if (var.getLockType() == LockType.READ) {
                            // There're read locks by other transactions.
                            for (Transaction tr : transactions) {
                                if (tr.getBeginTime() < t.getBeginTime()) {
                                    abort(tId);
                                    return;
                                }
                            }
                        } else {
                            if (var.hasNoLock()) {
                                var.addLock(LockType.WRITE, tId);
                            }
                        }
                    }
                } else {
                    // Variable has locks of other transactions.
                    for (int trid : var.getLockOwners()) {
                        if (getTransactionById(trid).getBeginTime()
                                    < t.getBeginTime()) {
                            abort(tId);
                            return;
                        }
                    }
                }
            }
        }

        // Commit or try again next time.
        if (hasAllLocksFromLivingSites(message.transactionId,
                message.variable)) {
            DM.write(message.variable, message.value);
            pendingWrite.add(message);
        } else {
            todoBuffer.add(message);
            System.out.println("T" + message.transactionId + " waits");
        }
    }

    private void dump(Message message){
        int defaultValue = Integer.MIN_VALUE;
        if (message.site == defaultValue && message.variable == defaultValue) {
            DM.dumpAll();
        } else if (message.site != defaultValue) {
            DM.dumpSite(message.site);
        } else if (message.variable != defaultValue) {
            DM.dumpVariable(message.variable);
        }
    }

    private void end(Message message){
        if(!isTransactionInList(message.transactionId)){
            return;
        }
        int tId = message.transactionId;
        // If it's a read-only transaction, try to clear old copy list
        if (getTransactionById(tId).isReadOnly()) {
            deleteCopyAtTime(getTransactionById(tId).getBeginTime());
        }
        // TODO: figure out whether to abort or commit a transaction right now.
        for (Message msg : todoBuffer) {
            if (msg.transactionId == tId) {
                abort(tId);
            }
        }
        commit(tId);
    }

    private void fail(Message message){
        // Abort all transctions who hold lock on this site.
        List<Integer> tids = sites.get(message.site - 1).getLockOwners();
        for (int tid : tids) {
            abort(tid);
        }
        // Set all variable on this site to unreadable and clear the locks.
        DM.fail(message.site);
    }

    private void recover(Message message){
        DM.recover(message.site);
    }

    private void abort(int tId) {
        // Remove messages of this transaction in todoBuffer.
        removeMessagesOfTransaction(tId, todoBuffer);
        // Remove the locks of this transaction at all sites.
        for (Site site : sites) {
            site.removeLockByTransaction(tId);
        }
        // Remove this transaction from transaction list when it aborts
        Iterator<Transaction> iter = transactions.iterator();
        while(iter.hasNext()){
            if(iter.next().getId() == tId){
                iter.remove();
            }
        }
        System.out.println("T" + tId + " aborted");
    }

    private void commit(int tId) {
        // Finish pending writes.
        for(Message msg: pendingWrite){
            if(msg.operation == Operation.W && msg.transactionId == tId){
                DM.commit(msg.variable);
            }
        }
        // Remove messages of this transaction in todoBuffer.
        removeMessagesOfTransaction(tId, todoBuffer);
        // Remove the locks of this transaction at all sites.
        for (Site site : sites) {
            site.removeLockByTransaction(tId);
        }
        // Remove this transaction from transaction list when it aborts
        Iterator<Transaction> iter = transactions.iterator();
        while(iter.hasNext()){
            if(iter.next().getId() == tId){
                iter.remove();
            }
        }
        System.out.println("T" + tId + " committed");
    }

    /**
     *  @param tid  The transaction's id.
     *  @param vid  The variable's id.
     *  @return     If the transaction acquired all locks from living sites.
     */
    private boolean hasAllLocksFromLivingSites(int tid, int vid) {
        for (Site site : sites) {
            if (!site.isUp() || !site.containsVar(vid)) {
                continue;
            }
            if (!site.getVar(vid).getLockOwners().contains(tid)) {
                return false;
            }
        }
        return true;
    }

    private OldCopy createOldCopy(long time){
        List<Integer> values = new ArrayList<Integer>();
        for(int i = 1; i < 21; i++){
            values.add(DM.read(i));
        }      
        return new OldCopy(time, values);
    }

    private OldCopy getOldCopy(long time) {
        // If all site failed, can't return the old copy.
        Iterator<OldCopy> iter = oldCopies.iterator();
        while(iter.hasNext()) {
            OldCopy oldCopy = iter.next();
            if (oldCopy.getTime() == time) {
                return oldCopy;
            }
        }
        return null;
    }

    /**
     *  Delete one old copy at a given time. If there're several old copies 
     *  at a given time, then only delete one old copy. The rest copies
     *  at that given time are still needed by other read-only transaction. 
     */
    private void deleteCopyAtTime(long time){
        Iterator<OldCopy> iter = oldCopies.iterator();
        while (iter.hasNext()) {
            if (iter.next().getTime() == time) {
                iter.remove();
                break;
            }
        }
    }

    /**
     *  Delete the messages of a transaction in the todoBuffer.
     *  @param tid      The id of the transaction.
     *  @param queue    The queue of messages to do the deletion.
     */
    private void removeMessagesOfTransaction(int tid, Queue<Message> queue) {
        Iterator<Message> iter = queue.iterator();
        while (iter.hasNext()) {
            if (iter.next().transactionId == tid) {
                iter.remove();
            }
        }
    }

    private Transaction getTransactionById(int tid) {
        for (Transaction tr : transactions) {
            if (tid == tr.getId()) {
                return tr;
            }
        }
        return null;
    }

    private boolean isTransactionInList(int tId){
        for(Transaction tr: transactions){
            if(tr.getId() == tId){
                return true;
            }
        }
        return false;
    }
}
