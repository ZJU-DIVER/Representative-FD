package RepFD;

import java.io.Serializable;
import java.util.BitSet;

//没用
//Tane原本的node记录的信息（包括rhsCandidates，valid，partition）
// 我们的方法改用EaseCalculation/simpleCombinationHelper
public class CombinationHelper implements Serializable {
    private static final long serialVersionUID = 1L;
    private BitSet rhsCandidates;
    private boolean valid;
    private StrippedPartition partition;

    public CombinationHelper() {
        valid = true;
    }

    public BitSet getRhsCandidates() {
        return rhsCandidates;
    }

    public void setRhsCandidates(BitSet rhsCandidates) {
        this.rhsCandidates = rhsCandidates;
    }

    public StrippedPartition getPartition() {
        return partition;
    }

    public void setPartition(StrippedPartition partition) {
        this.partition = partition;
    }

    public boolean isValid() {
        return valid;
    }

    public void setInvalid() {
        this.valid = false;
        partition = null;
    }
}