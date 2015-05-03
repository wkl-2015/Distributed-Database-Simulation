import java.util.ArrayList;
import java.util.Hashtable;
import java.util.HashSet;
import java.util.List;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Created by hongdaj on 11/26/14.
 */
public class Site {
    // numbered from 1 - 10
    int id;
    // indexed from 1 - 20
    private Hashtable<Integer, Variable> data;
    // indicates this site is up(true) or fail(fail)
    private boolean isUp;

    public Site(int id){
        this.id = id;
        this.isUp = true;
        data = new Hashtable<Integer, Variable>();
        //initialize variables
        if(id % 2 == 0){
            //The odd indexed variables are at one site each (i.e. 1 + index number mod 10 )
            for(int i = 1; i < 21; i++){
                if(i % 2 == 0) {
                    data.put(i, new Variable(i, TransactionManager.getTime()));
                }
            }
            data.put(id - 1, new Variable(id - 1, TransactionManager.getTime()));
            data.put(id - 1 + 10, new Variable(id - 1 + 10, TransactionManager.getTime()));
        }
        else{
            for(int i = 1; i < 21; i++){
                if(i % 2 == 0) {
                    data.put(i, new Variable(i, TransactionManager.getTime()));
                }
            }
        }
    }

    public boolean isUp() {
        return isUp;
    }

    public void setUp(){
        if(this.isUp){
            System.out.println("Warning: site " + id + " is already alive!");
            return;
        }
        this.isUp = true;
    }

    public void setDown(){
        if(!this.isUp){
            System.out.println("Warning: site " + id + " is already down!");
            return;
        }
        this.isUp = false;
        // Set all variable unreadable on this site.
        Set<Integer> keys = data.keySet();
        for (int key : keys) {
            data.get(key).setUnReadable();
        }
        // Clear the locks on all variable on this site.
        for (int key : keys) {
            data.get(key).clearLocks();
        }
    }

    public void removeLockByTransaction(int tId) {
        Set<Integer> keys = data.keySet();
        for (int key : keys) {
            data.get(key).removeLockByTransaction(tId);
        }
    }

    public boolean containsVar(int varId){
        return data.containsKey(varId);
    }

    public int getId(){
        return this.id;
    }

    /**
     *  Find the transactions holding locks on this site.
     *  @return A list of unduplicated transaction ids.
     */
    public List<Integer> getLockOwners() {
        List<Integer> ownerIds = new ArrayList<Integer>();
        Set<Integer> keys = data.keySet();
        for (int key : keys) {
            ownerIds.addAll(data.get(key).getLockOwners());
        }
        // Remove duplicated transaction ids.
        HashSet<Integer> hashset = new HashSet<Integer>();
        hashset.addAll(ownerIds);
        ownerIds.clear();
        ownerIds.addAll(hashset);
        return ownerIds;
    }

    public Variable getVar(int varId){
        if(!data.containsKey(varId)){
            throw new RuntimeException("x" + varId + " is not found in site " + id + "!");
        }
        return data.get(varId);
    }


    public void updatePendingValue(int varId, int pendingValue){
        if(data.containsKey(varId)){
            Variable var = data.get(varId);
            var.setPendingValue(pendingValue);
        }
        else {
            throw new RuntimeException("x" + varId + " is not found in site " + id + "!");
        }
    }

}
