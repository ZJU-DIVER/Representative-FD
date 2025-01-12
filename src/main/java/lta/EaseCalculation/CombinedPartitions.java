package RepFD.EaseCalculation;

import RepFD.StrippedPartition;
import gnu.trove.map.hash.TLongObjectHashMap;

import java.util.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.stream.Collectors;

public class CombinedPartitions extends TLongObjectHashMap<HashMap<BitSet, StrippedPartition>> {
    private static final long serialVersionUID = -7385828030861564827L;
    private static final int PARTITION_THRESHOLD = 100000;
    private static final boolean USE_MEMORY_MANAGEMENT = true;

    private int numberOfColumns;
    //private int minNumberOfColumns=2;
    private int maxNumberOfColumns = 0;
    //private int diff=5000;
    private BitSet key;
//        private TObjectIntHashMap<BitSet> usageCounter;
//        private LinkedList<BitSet> leastRecentlyUsedPartitions;
//        private TObjectIntHashMap<BitSet> totalCount;

    public CombinedPartitions(int numberOfColumns) {
        this.numberOfColumns = numberOfColumns;
        this.key = new BitSet(numberOfColumns);
//            if (USE_MEMORY_MANAGEMENT) {
//                this.usageCounter = new TObjectIntHashMap<>();
//                this.leastRecentlyUsedPartitions = new LinkedList<BitSet>();
//                this.totalCount = new TObjectIntHashMap<>();
//            }
        for (long cardinality = 0; cardinality <= this.numberOfColumns; cardinality++) {
            this.put(cardinality, new HashMap<BitSet, StrippedPartition>());
        }
    }

//        public int getTotalCount() {
//            int totalCount = 0;
//            for (BitSet key : this.totalCount.keySet()) {
//                totalCount += this.totalCount.get(key);
//            }
//            return totalCount;
//        }
//        public int getUniqueCount() {
//            return this.totalCount.size();
//        }

    public void reset() {
        for (long cardinality = 2; cardinality <= this.numberOfColumns; cardinality++) {
            this.get(cardinality).clear();
        }
    }

    public int getCount() {
        int cumulatedCount = 0;
        for (HashMap<BitSet, StrippedPartition> elementsOfLevel : this.valueCollection()) {
            cumulatedCount += elementsOfLevel.size();
        }
        return cumulatedCount;
    }

    public StrippedPartition get(BitSet key) {
        StrippedPartition result = this.get(key.cardinality()).get(key);
//            if (USE_MEMORY_MANAGEMENT && result != null) {
//                this.usageCounter.adjustValue(key, 1);
//                this.leastRecentlyUsedPartitions.remove(key);
//                this.leastRecentlyUsedPartitions.addLast(key);
//                freeSpace();
//            }
        //freespace
        int c = this.getCount();
        if (c > PARTITION_THRESHOLD) {
            myFreeSpace(c);
        }
        return result;
    }

    public void myFreeSpace(int c) {
        System.out.println("Count before:\t" + c);
        c = c / 3;
        //int n=(maxNumberOfColumns-minNumberOfColumns)/2+minNumberOfColumns;
        for (int i = 2; i <= maxNumberOfColumns; i++) {
            if (this.get(i).size() == 0) {
                continue;
            }
            if (c < this.get(i).size()) {
                //minNumberOfColumns=maxNumberOfColumns;
                Set<BitSet> sub = this.get(i).keySet();
                sub = sub.stream().limit(c).collect(Collectors.toSet());
                for (BitSet bitSet : sub) {
                    this.get(i).remove(bitSet);
                }
                //minNumberOfColumns=i;
                break;
            }
            c -= this.get(i).size();
            this.get(i).clear();
            if (c <= 0) {
                //minNumberOfColumns=i+1;
                break;
            }
        }
//            if(c>0){
//                minNumberOfColumns=maxNumberOfColumns;
//                Set<BitSet> sub=this.get(minNumberOfColumns).keySet();
//                sub=sub.stream().limit(c).collect(Collectors.toSet());
//                for (BitSet bitSet : sub) {
//                    this.get(minNumberOfColumns).remove(bitSet);
//                }
//            }
        System.out.println("Count after:\t" + this.getCount());//c/2
    }

//        private void freeSpace() {
//            int num=PARTITION_THRESHOLD + this.numberOfColumns;
//            int before=this.getCount();
//            if (this.getCount() > num&&this.leastRecentlyUsedPartitions.size()>num/3) {
//                System.out.println("Count before:\t" + before);
//                int[] usageCounters = this.usageCounter.values();
//                Arrays.sort(usageCounters);
//                int medianOfUsage = 0,largeMedianOfUsage = 0;
//                int multi=9000/diff;
//                medianOfUsage = usageCounters[usageCounters.length-usageCounters.length / multi];
//                largeMedianOfUsage=usageCounters[usageCounters.length-usageCounters.length / (multi+1)];
//                if (usageCounters.length % 2 == 0) {
//                    medianOfUsage += usageCounters[usageCounters.length / 2 + 1];
//                    medianOfUsage /= 2;
//                }
//                int numberOfPartitionsToDelete = (PARTITION_THRESHOLD + this.numberOfColumns)/2;
//                int deletedColumns = 0;
//                Iterator<BitSet> deleteIt = this.leastRecentlyUsedPartitions.iterator();
//                while (deleteIt.hasNext() && deletedColumns < numberOfPartitionsToDelete) {
//                    BitSet keyOfPartitionToDelete = deleteIt.next();
//                    if(!(keyOfPartitionToDelete.cardinality() == 1) ){
//                        deleteIt.remove();
//                        this.removePartition(keyOfPartitionToDelete);
//                        this.usageCounter.remove(keyOfPartitionToDelete);
//                        deletedColumns++;
//                        if(keyOfPartitionToDelete.cardinality()<=maxNumberOfColumns/2+1&&this.usageCounter.get
//                        (keyOfPartitionToDelete) <= largeMedianOfUsage){
//                            deleteIt.remove();
//                            this.removePartition(keyOfPartitionToDelete);
//                            this.usageCounter.remove(keyOfPartitionToDelete);
//                            deletedColumns++;
//                        }
//                        else{
//                            if ((this.usageCounter.get(keyOfPartitionToDelete) <= medianOfUsage )) {
//                                deleteIt.remove();
//                                this.removePartition(keyOfPartitionToDelete);
//                                this.usageCounter.remove(keyOfPartitionToDelete);
//                                deletedColumns++;
//                            }
//                        }
//
//                    }
//                }
//                diff=before-this.getCount();
//                if(this.getCount()>num){
//                    System.out.println(this.leastRecentlyUsedPartitions.size());
//                }
//                System.out.println("Count after:\t" + this.getCount());
//            }
//        }

    public void addPartition(BitSet key, StrippedPartition partition) {
//            long cardinalityOfPartitionIndices = key.cardinality();
        if (key.cardinality() > maxNumberOfColumns) {
            maxNumberOfColumns = key.cardinality();
        }
//            if(key.cardinality()<minNumberOfColumns&&key.cardinality()>=2){
//                minNumberOfColumns= key.cardinality();
//            }
        this.get(key.cardinality()).put(key, partition);
//            if (USE_MEMORY_MANAGEMENT) {
//                this.leastRecentlyUsedPartitions.addLast(key);
//                this.usageCounter.adjustOrPutValue(key, 1, 1);
//                this.totalCount.adjustOrPutValue(key, 1, 1);
//            }
    }

    private void removePartition(BitSet partitionKey) {
        long cardinalityOfPartitionIndices = partitionKey.cardinality();
        this.get(cardinalityOfPartitionIndices).remove(partitionKey);
    }

//    public void addPartitions(ArrayList<Partition> partitions) {
//        for (Partition partition : partitions) {
//            this.addPartition(partition);
//        }
//    }

    public StrippedPartition getAtomicPartition(int columnIndex) {
        BitSet k = (BitSet) this.key.clone();
        k.clear();
        k.set(columnIndex);
        return this.get(0).get(k);
    }

    public ArrayList<BitSet> getBestMatchingAttributs(BitSet path) {
        BitSet pathCopy = (BitSet) path.clone();
        ArrayList<BitSet> bestMatchingPartitions = new ArrayList<>();
        long notCoveredColumns = pathCopy.cardinality();
        long sizeOfLastMatch = notCoveredColumns;

        // the strategy is greedy and fit first
        outer:
        while (notCoveredColumns > 0) {
            // we don't need to check the sizes above the last match size again
            for (long collectionCardinality = Math.min(notCoveredColumns, sizeOfLastMatch); collectionCardinality > 0; collectionCardinality--) {
                HashMap<BitSet, StrippedPartition> candidatesOfLevel = this.get(collectionCardinality);
                for (BitSet candidateOfLevel : candidatesOfLevel.keySet()) {
                    BitSet sCollection = (BitSet) pathCopy.clone();
                    sCollection.and(candidateOfLevel);
                    if (candidateOfLevel.equals(sCollection)) {
//						bestMatchingPartitions.add(candidatesOfLevel.get(candidateOfLevel));
                        bestMatchingPartitions.add((BitSet) candidateOfLevel.clone());
                        notCoveredColumns -= collectionCardinality;
                        pathCopy.andNot(candidateOfLevel);
                        sizeOfLastMatch = collectionCardinality;
                        continue outer;
                    }
                }
            }
        }
        return bestMatchingPartitions;
    }

    public ArrayList<StrippedPartition> getBestMatchingPartitions(BitSet path) {
        BitSet pathCopy = (BitSet) path.clone();
        ArrayList<StrippedPartition> bestMatchingPartitions = new ArrayList<>();
        long notCoveredColumns = pathCopy.cardinality();
        long sizeOfLastMatch = notCoveredColumns;

        // the strategy is greedy and fit first
        outer:
        while (notCoveredColumns > 0) {
            // we don't need to check the sizes above the last match size again
            for (long collectionCardinality = Math.min(notCoveredColumns, sizeOfLastMatch); collectionCardinality > 0; collectionCardinality--) {
                HashMap<BitSet, StrippedPartition> candidatesOfLevel = this.get(collectionCardinality);
                for (BitSet candidateOfLevel : candidatesOfLevel.keySet()) {
                    BitSet sCollection = (BitSet) pathCopy.clone();
                    sCollection.and(candidateOfLevel);
                    if (candidateOfLevel.equals(sCollection)) {
//						bestMatchingPartitions.add(candidatesOfLevel.get(candidateOfLevel));
                        bestMatchingPartitions.add(this.get(candidateOfLevel));
                        notCoveredColumns -= collectionCardinality;
                        pathCopy.andNot(candidateOfLevel);
                        sizeOfLastMatch = collectionCardinality;
                        continue outer;
                    }
                }
            }
        }
        return bestMatchingPartitions;
    }
}