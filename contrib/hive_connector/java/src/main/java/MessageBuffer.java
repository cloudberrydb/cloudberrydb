import java.lang.StringBuffer;

public class MessageBuffer {
    private StringBuffer message;

    public MessageBuffer() {
        message = new StringBuffer();
    }

    public MessageBuffer(String msg) {
        message = new StringBuffer(msg);
    }

    public String getVal() {
        return message.toString();
    }

    public void resetVal() {
        message.delete(0, message.length());
    }

    public void catVal(String msg) {
        message.append(msg);
    }

    public void catVal(int val) {
        message.append(val);
    }

}
