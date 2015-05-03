/**
 * Created by hongdaj on 11/26/14.
 */

import java.util.ArrayList;
import java.util.List;

/**
 * a Message object represents a command like W(T1, x1, 10).
 * A message is usually created by IO object.
 * A message is usually thrown to TM after creating, and then assigned to one
 * transaction if necessary.
 */
public class Message {
    // raw message, e.g,"W(T1, x1, 10)", are mostly used to debug.
    public final String rawMessageString;
    public final Operation operation;
    public final int transactionId;
    public final int variable;
    public final int site;
    public final int value;

    public static class Builder {
        // Required parameters.
        private final String rawMessageString;

        // Optional parameters - initialized to default values.
        private Operation operation;
        private int transactionId;
        private int variable;
        private int site;
        private int value;

        public Builder(String rawMessageString) {
            this.rawMessageString = rawMessageString.replaceAll("\\s+", "");
            this.operation = null;
            this.transactionId = Integer.MIN_VALUE;
            this.variable = Integer.MIN_VALUE;
            this.site = Integer.MIN_VALUE;
            this.value = 0;
        }

        public Builder operation(Operation op) {
            operation = op;
            return this;
        }

        public Builder transactionId(int tr) {
            transactionId = tr;
            return this;
        }

        public Builder variable(int vr) {
            variable = vr;
            return this;
        }

        public Builder site(int st) {
            site = st;
            return this;
        }

        public Builder value(int va) {
            value = va;
            return this;
        }

        public Message build() {
            return new Message(this);
        }
    }
    
    private Message(Builder bd) {
        rawMessageString = bd.rawMessageString;
        operation = bd.operation;
        transactionId = bd.transactionId;
        variable = bd.variable;
        site = bd.site;
        value = bd.value;
    }

    @Override
    public String toString() {
        return rawMessageString + " " + operation.toString() + transactionId +
                " " + variable + " " + site + " " + value;
    }
}
