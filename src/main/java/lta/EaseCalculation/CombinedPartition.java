package RepFD.EaseCalculation;

import java.util.ArrayList;
import java.util.BitSet;

import RepFD.StrippedPartition;

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;

public class CombinedPartition extends Object2ObjectOpenHashMap<BitSet, StrippedPartition> {
    private static final long serialVersionUID = -7385828030861564827L;

    private int numberOfColumns;
    private BitSet key;

    public CombinedPartition(int numberOfColumns) {
        this.numberOfColumns = numberOfColumns;
        this.key = new BitSet(numberOfColumns);
    }

    public void addPartition(BitSet key, StrippedPartition partition) {

        this.put(key, partition);
    }

    public StrippedPartition getAtomicPartition(int columnIndex) {
        this.key.clear(0, this.numberOfColumns);

        this.key.set(columnIndex);
        return this.get(this.key);
    }

    public ArrayList<StrippedPartition> getBestMatchingPartitionsLazy(BitSet path) {
        ArrayList<StrippedPartition> bestMatchingPartitions = new ArrayList<>();

        for (int columnIndex = path.nextSetBit(0); columnIndex >= 0; columnIndex = path.nextSetBit(columnIndex + 1)) {
            bestMatchingPartitions.add(this.getAtomicPartition(columnIndex));
        }
        return bestMatchingPartitions;
    }

    public ArrayList<StrippedPartition> getBestMatchingPartitions(BitSet path) {
        BitSet pathCopy = (BitSet) path.clone();
        ArrayList<StrippedPartition> bestMatchingPartitions = new ArrayList<>();
        long notCoveredColumns = pathCopy.cardinality();

        // the strategy is greedy and fit first
        while (notCoveredColumns > 0) {
            long maxCoverCount = 0;
            BitSet maxCoverPath = null;
            StrippedPartition maxCoverPartition = null;
            for (BitSet savedCollection : this.keySet()) {
                BitSet sCollection = (BitSet) pathCopy.clone();
                sCollection.and(savedCollection);
                if (savedCollection.equals(sCollection)) {
                    long currentCoverCount = savedCollection.cardinality();
                    if (currentCoverCount > maxCoverCount) {
                        maxCoverCount = currentCoverCount;
                        maxCoverPath = savedCollection;
                        maxCoverPartition = this.get(savedCollection);
                    }
                }
            }
           
            pathCopy.andNot(maxCoverPath);
            notCoveredColumns = pathCopy.cardinality();
            bestMatchingPartitions.add(maxCoverPartition);
        }
        if (bestMatchingPartitions.size() < 2) {
            System.out.println("notCoveredColumns : " + notCoveredColumns);
        }
        return bestMatchingPartitions;
    }
}