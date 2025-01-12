package RepFD.EaseCalculation;

import RepFD.StrippedPartition;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.io.Serializable;
import java.util.BitSet;

public class SimpleCombinationHelperNP implements Serializable {
    private static final long serialVersionUID = 1L;
    private BitSet rhsCandidates;
    private boolean valid;
    private ObjectArrayList<BitSet> children;
    private StrippedPartition partition;
    private ObjectArrayList<BitSet> maxPList;

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