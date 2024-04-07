package RepFD.EaseCalculation;

import RepFD.StrippedPartition;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.io.Serializable;
import java.util.BitSet;

//tane原本的CombinationHelper，删去所有strippedPartition相关,不再在每个node记录strippedPartition
//修改新的partition计算

public class SimpleCombinationHelperNP implements Serializable {
    private static final long serialVersionUID = 1L;
    private BitSet rhsCandidates;
    private boolean valid;
    private ObjectArrayList<BitSet> children;

    //null
    private StrippedPartition partition;
    private ObjectArrayList<BitSet> maxPList;//包含xi的计算过的最大的partition

    public SimpleCombinationHelperNP() {
        valid = true;
        children = new ObjectArrayList<>();
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

    public void setChildren(ObjectArrayList<BitSet> children) {
        this.children = children;
    }

    public ObjectArrayList<BitSet> getChildren() {
        return children;
    }
}