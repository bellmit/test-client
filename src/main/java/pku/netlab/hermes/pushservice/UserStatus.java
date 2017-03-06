package pku.netlab.hermes.pushservice;

/**
 * Created by kindt on 2016/8/18 0018.
 */
public class UserStatus {
    private short curMsgID = 0;
    private String uid;

    public UserStatus(String uid) {
        this.uid = uid;
    }

    public short getCurMsgID() {
        return curMsgID;
    }

    public void setCurMsgID(short curMsgID) {
        this.curMsgID = curMsgID;
    }
}
