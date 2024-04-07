package RepFD.EaseCalculation;

import java.io.Serializable;
import java.util.BitSet;

//tane原本的CombinationHelper，删去所有strippedPartition相关,不再在每个node记录strippedPartition
public class SimpleCombinationHelperkey implements Serializable {
    private static final long serialVersionUID = 1L;
    private BitSet rhsCandidates;
    private boolean valid;
    private BitSet mmkey = new BitSet();//从哪个开始算partition
    //private boolean key=false;//mm中有该node的partition

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