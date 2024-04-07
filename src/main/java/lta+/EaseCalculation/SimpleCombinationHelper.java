package RepFD.EaseCalculation;

import java.io.Serializable;
import java.util.BitSet;

//tane原本的CombinationHelper，删去所有strippedPartition相关,不再在每个node记录strippedPartition
public class SimpleCombinationHelper implements Serializable {
    private static final long serialVersionUID = 1L;
    private BitSet rhsCandidates;
    private boolean valid;

    public SimpleCombinationHelper() {
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