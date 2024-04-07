package RepFD.EaseCalculation;

import RepFD.FDResult;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import it.unimi.dsi.fastutil.objects.ObjectSet;

import java.io.Serializable;
import java.util.*;

//tane原本的CombinationHelper，删去所有strippedPartition相关,不再在每个node记录strippedPartition
//hamming 距离和新比较方式
public class SimpleCombinationHelperHM implements Serializable {
    private static final long serialVersionUID = 1L;
    private BitSet rhsCandidates;
    private Int2ObjectOpenHashMap<ObjectOpenHashSet<FDResult>> simiFDs;
    //private List<FDResult> simiFDs;


    private boolean valid;

    public SimpleCombinationHelperHM() {
        valid = true;
        simiFDs = new Int2ObjectOpenHashMap<>();
        //simiFDs = new ArrayList<>();
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

    public Int2ObjectOpenHashMap<ObjectOpenHashSet<FDResult>> getSimiFDs() {
        return simiFDs;
    }

    public void setSimiFDs(Int2ObjectOpenHashMap<ObjectOpenHashSet<FDResult>> simiFDs) {
        this.simiFDs = simiFDs;
    }

//    public Map<Integer, Set<FDResult>> getSimiFDs() {
//        return simiFDs;
//    }
//
//    public void setSimiFDs(Map<Integer, Set<FDResult>> simiFDs) {
//        this.simiFDs = simiFDs;
//    }
}