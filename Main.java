import java.util.List;
/**
 * Created by hongdaj on 11/27/14.
 */
public class Main {
    public static void main(String[] args){
        String testFile = args[0];
        IO io = new IO(testFile);
        //io.showAll();
        TransactionManager TM = new TransactionManager();
        while (io.hasNextLine()) {
            List<Message> messages = io.getNextMessages();
            TM.execute(messages);
        }
        System.out.println();
        System.out.println("--------------------------------------------------------");
        System.out.println();

        return;
    }
}