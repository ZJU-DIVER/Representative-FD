package RepFD.EaseCalculation;

import java.io.Serializable;
import java.util.BitSet;

public class SimpleCombinationHelperkey implements Serializable {
    private static final long serialVersionUID = 1L;
    private BitSet rhsCandidates;
    private boolean valid;
    private BitSet mmkey = new BitSet();
    public BitSet getMmkey() {
        return mmkey;
    }
    public void setMmkey(BitSet mmkey) {//从哪个开始算partition
        this.mmkey = mmkey;
    }

//    public boolean hasKey() {
//        return key;
//    }
//
//    public void setKey(boolean key) {
//        this.key = key;
//    }

    public SimpleCombinationHelperkey() {
        valid = true;
    }

    public BitSet getRhsCandidates() {
        return rhsCandidates;
    }

    public void setRhsCandidates(BitSet rhsCandidates) {
        this.rhsCandidates = rhsCandidates;
    }

    public boolean isValid() {
        return valid;
    }

    public void setInvalid() {
        this.valid = false;
    }
}