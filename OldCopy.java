/**
 * Created by hongdaj on 11/26/14.
 */

import java.util.ArrayList;
import java.util.List;

/**
 * OldCopy is used by read-only transaction, stored in TM
 */
public class OldCopy {
    private long time;
    private List<Integer> values;

    public OldCopy(long time, List<Integer> values){
        this.time = time;
        this.values = new ArrayList<Integer>(values);
    }

    public long getTime(){
        return this.time;
    }

    public int getValueOfVariable(int index){
        return values.get(index - 1);
    }

}
