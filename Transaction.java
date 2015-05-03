import java.util.ArrayList;
import java.util.List;

/**
 * Created by hongdaj on 11/26/14.
 */
public class Transaction {
    private int id;
    private long beginTime;
    // Indicates if this transaction is read-only.
    private boolean readOnly;
    // Usually the size of this buffer should be 1, but we use list for safety.
    private List<Message> buffer;

    public Transaction(int id, boolean readOnly, long beginTime){
        this.id = id;
        this.readOnly = readOnly;
        this.beginTime = beginTime;
        this.buffer = new ArrayList<Message>();
    }

    public Message getMessageFromBuffer(){
        return buffer.get(0);
    }

    public void addMessageToBuffer(Message message){
        if (buffer.size() == 1) {
            System.out.println("Warning: Buffer of T" + id + " is full.");
        } else {
            buffer.add(message);
        }
    }

    public long getBeginTime() {
        return beginTime;
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    public int getId() {
        return id;
    }
}
