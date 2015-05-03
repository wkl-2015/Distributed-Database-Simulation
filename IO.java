import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

/**
 * Created by hongdaj on 11/26/14.
 */

/**
 * IO class is a utility class, used to read and parse commands from file
 */
public class IO {
    //TODO: can be changed to interactive mode - read one line at a time.
    private Queue<String> rawMessageBuffer;
    private String pathToFile;

    public IO(String pathToFile){
        rawMessageBuffer = new LinkedList<String>();
        this.pathToFile = pathToFile;
        try {
            readWholeFile();
        }catch (IOException ex){
            System.out.println("Read file exception!");
        }
    }

    private void readWholeFile() throws IOException{
        BufferedReader br = new BufferedReader(new FileReader(pathToFile));
        String line;
        while ((line = br.readLine()) != null) {
            if (!line.isEmpty()) {
                rawMessageBuffer.add(line);
            }
        }
        //rawMessageBuffer.add("dump()");
        br.close();
    }

    public boolean hasNextLine() {
        return rawMessageBuffer.size() > 0;
    }

    /**
     * Get the messages at next time tick
     * @return A list of messages
     */
    public List<Message> getNextMessages(){
        if(rawMessageBuffer.size() > 0){
            String line = rawMessageBuffer.poll();
            return parseLine(line);
        }
        return null;
    }

    private List<Message> parseLine(String line){
        List<Message> messages = new ArrayList<Message>();
        String[] rawMessages = line.split(";");
        for(int i = 0; i < rawMessages.length; i++){
            messages.add(parseRawMessageString(rawMessages[i]));
        }
        return messages;
    }

    /**
     *  Read a string of message and return a message object.
     *  @param msgStr   The message string, e.g., "W(T1, x1,5)".
     *  @return         The message object parsed from the string.
     */
    private Message parseRawMessageString(String msgStr){
        if(msgStr == null || msgStr.isEmpty()){
            throw new IllegalArgumentException(
                "Empty message in parseRawMessageString!"
            );
        }

        // Convert to lower cases and remove any white spaces.
        String str = msgStr.toLowerCase().replaceAll("\\s+","");;
        String tmp = str.substring(str.indexOf("(") + 1, str.indexOf(")"));
        String[] parameters = new String[]{};
        if (!tmp.isEmpty()) {
            parameters = tmp.split(",");
        }

        if(str.startsWith("beginro")){
            String id = parameters[0].substring(1, parameters[0].length());
            int tid = Integer.parseInt(id);
            return new Message.Builder(msgStr).
                operation(Operation.BEGINRO).
                transactionId(tid).
                build();
        }

        if(str.startsWith("begin")){
            String id = parameters[0].substring(1, parameters[0].length());
            int tid = Integer.parseInt(id);
            return new Message.Builder(msgStr).
                    operation(Operation.BEGIN).
                    transactionId(tid).
                    build();
        }

        if(str.startsWith("recover")){
            int siteId = Integer.parseInt(parameters[0]);
            return new Message.Builder(msgStr).
                operation(Operation.RECOVER).
                site(siteId).
                build();
        }

        if(str.startsWith("r")){
            String id = parameters[0].substring(1, parameters[0].length());
            int tid = Integer.parseInt(id);
            id = parameters[1].substring(1, parameters[1].length());
            int vid = Integer.parseInt(id);
            return new Message.Builder(msgStr).
                    operation(Operation.R).
                    transactionId(tid).
                    variable(vid).
                    build();
        }

        if(str.startsWith("w")){
            String id = parameters[0].substring(1, parameters[0].length());
            int tid = Integer.parseInt(id);
            id = parameters[1].substring(1, parameters[1].length());
            int vid = Integer.parseInt(id);
            return new Message.Builder(msgStr).
                    operation(Operation.W).
                    transactionId(tid).
                    variable(vid).
                    value(Integer.parseInt(parameters[2])).
                    build();
        }

        if(str.startsWith("end")){
            String id = parameters[0].substring(1, parameters[0].length());
            int tid = Integer.parseInt(id);
            return new Message.Builder(msgStr).
                    operation(Operation.END).
                    transactionId(tid).
                    build();
        }

        if(str.startsWith("dump")){
            if (parameters.length == 0) {
                return new Message.Builder(msgStr).
                        operation(Operation.DUMPALL).
                        build();
            } else if (parameters[0].charAt(0) == 'x') {
                String id = parameters[0].
                        substring(1, parameters[0].length());
                int vid = Integer.parseInt(id);
                return new Message.Builder(msgStr).
                        operation(Operation.DUMPVARIABLE).
                        variable(vid).
                        build();
            } else {
                int siteid = Integer.parseInt(parameters[0]);
                return new Message.Builder(msgStr).
                        operation(Operation.DUMPSITE).
                        site(siteid).
                        build();
            }
        }

        if(str.startsWith("fail")){
            int siteId = Integer.parseInt(parameters[0]);
            return new Message.Builder(msgStr).
                    operation(Operation.FAIL).
                    site(siteId).
                    build();
        }



        return null;
    }

    public void showAll(){
        System.out.println("Test File: " + pathToFile);
        for(int i = 0; i < rawMessageBuffer.size(); i++){
            String tmp = rawMessageBuffer.poll();
            System.out.println(tmp);
            rawMessageBuffer.add(tmp);
        }
    }
}
