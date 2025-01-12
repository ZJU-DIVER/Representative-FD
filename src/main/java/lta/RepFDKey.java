package RepFD;

import RepFD.EaseCalculation.CombinedPartitions;
import RepFD.EaseCalculation.SimpleCombinationHelperkey;
import com.opencsv.CSVWriter;
import experiments.Representativeness;
import it.unimi.dsi.fastutil.longs.LongBigArrayBigList;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectBigArrayBigList;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;
import tools.FDTree;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;

public class RepFDKey {
    public String filename;
    private String path;
    private String tableName;
    double threshold;
    int w1, w2;
    private int numberAttributes;
    private long numberTuples;
    private List<String> columnNames;
    private ObjectArrayList<ColumnIdentifier> columnIdentifiers;
    private Object2ObjectOpenHashMap<BitSet, SimpleCombinationHelperkey> level0 = null;
    private Object2ObjectOpenHashMap<BitSet, SimpleCombinationHelperkey> level1 = null;
    private Object2ObjectOpenHashMap<BitSet, ObjectArrayList<BitSet>> prefix_blocks = null;
    private LongBigArrayBigList tTable;
    private int result;
    private List<List<FDResult>> result_l;
    private FDTree candidateTree;
    private Object2ObjectOpenHashMap<BitSet, SimpleCombinationHelperkey> skippedNodes = null;
    private List<BitSet> keyLHS;
    private CombinedPartitions mmCombinedPartitions;
    private int currentLevel = 0;
    public double runtime;
    public int fdNum;
    public CSVWriter csvWriter;
    public int similarityFunc;

    public RepFDKey(String filename, String path, double th, int w1, int w2) {
        this.threshold = th;
        this.w1 = w1;
        this.w2 = w2;
        this.filename = filename;
        this.path = path;
        this.similarityFunc = 2;
    }

    public double[] execute() throws IOException, CsvException {
        long t1 = System.currentTimeMillis();
        result = 0;
        keyLHS = new ArrayList();
        result_l = new ArrayList<>();
        result_l.add(new ArrayList());
        level0 = new Object2ObjectOpenHashMap<BitSet, SimpleCombinationHelperkey>();
        level1 = new Object2ObjectOpenHashMap<BitSet, SimpleCombinationHelperkey>();
        prefix_blocks = new Object2ObjectOpenHashMap<BitSet, ObjectArrayList<BitSet>>();

        // Get information about table from database or csv file
        ObjectArrayList<Object2ObjectOpenHashMap<Object, LongBigArrayBigList>> partitions = loadData();
        setColumnIdentifiers();
        numberAttributes = this.columnNames.size();
        candidateTree = new FDTree(this.numberAttributes);
        skippedNodes = new Object2ObjectOpenHashMap<>();
        this.mmCombinedPartitions = new CombinedPartitions(this.numberAttributes);
        candidateTree.fds_num = 0;

        // Initialize table used for stripped partition product
        tTable = new LongBigArrayBigList(numberTuples);
        for (long i = 0; i < numberTuples; i++) {
            tTable.add(-1);
        }

        // Initialize Level 0
        SimpleCombinationHelperkey chLevel0 = new SimpleCombinationHelperkey();
        BitSet rhsCandidatesLevel0 = new BitSet();
        rhsCandidatesLevel0.set(1, numberAttributes + 1);
        chLevel0.setRhsCandidates(rhsCandidatesLevel0);
        StrippedPartition spLevel0 = new StrippedPartition(numberTuples);
        mmCombinedPartitions.addPartition(new BitSet(), spLevel0);
        level0.put(new BitSet(), chLevel0);
        String outPath =
                System.getProperty("user.dir") + "/repFD_result/" + filename + "_" + this.w1 + "-" + this.w2 + "-" + this.threshold + ".csv";
        csvWriter = new CSVWriter(new FileWriter(outPath));

        // Initialize Level 1
        for (int i = 1; i <= numberAttributes; i++) {
            BitSet combinationLevel1 = new BitSet();
            combinationLevel1.set(i);
            SimpleCombinationHelperkey chLevel1 = new SimpleCombinationHelperkey();
            BitSet rhsCandidatesLevel1 = new BitSet();
            rhsCandidatesLevel1.set(1, numberAttributes + 1);
            chLevel1.setRhsCandidates(rhsCandidatesLevel0);
            StrippedPartition spLevel1 = new StrippedPartition(partitions.get(i - 1));
            mmCombinedPartitions.addPartition(combinationLevel1, spLevel1);
            level1.put(combinationLevel1, chLevel1);
        }

        // while loop (main part of TANE)
        int l = 1;
        while (!level1.isEmpty() && l <= numberAttributes) {
            result_l.add(new ArrayList<>());
            // compute dependencies for a level
            computeDependencies();
            // prune the search space
            prune();
            // compute the combinations for the next level
            generateNextLevel();
            l++;
        }
        long end = System.currentTimeMillis();
        runtime = (double) (end - t1) / 1000;
        fdNum = this.result;
        for (int i = 0; i < this.result_l.size(); i++) {
            for (FDResult fd : this.result_l.get(i)) {
                System.out.println(fd.getLhs() + " -> " + fd.getRhs());
            }
        }
        System.out.println("Fds num: " + fdNum);
        System.out.println("Total Time: " + runtime + "s");
        csvWriter.close();

        double[] re = new double[2];
        re[0] = fdNum;
        re[1] = runtime;
        return re;
    }

    /**
     * check if the input candidate is similar with one of found FDs
     *
     * @param XX -> a :candidate FD.
     */
    public boolean checkSimilarity(BitSet XX, int a) {
        int level = this.currentLevel;
        if (this.result == 0) {
            return false;
        }
        switch (this.similarityFunc) {
            case 1:
            case 2:
                while (this.result_l.get(level).size() == 0) {
                    level--;
                }
                for (int i = this.result_l.get(level).size(); i-- > 0; ) {
                    FDResult fd = this.result_l.get(level).get(i);
                    BitSet l = fd.getLhs();
                    BitSet lhs = (BitSet) l.clone();
                    int rhs = fd.getRhs();
                    lhs.and(XX);
                    double sim = lhs.cardinality();
                    sim = (double) Math.round(sim / (XX.cardinality() + l.cardinality() - sim) * 100000) / 100000;
                    sim = sim * this.w1;
                    if (a == rhs) {
                        sim += 1 * this.w2;
                    }
                    double cou =
                            Math.sqrt((l.cardinality() * XX.cardinality()) / (float) this.numberAttributes / (float) this.numberAttributes);
                    if ((w1 + w2 - sim) / (double) (w1 + w2) < (threshold)) {
                        return true;
                    }
                }
                while (level >= 2 && this.result_l.get(level - 1).size() == 0) {
                    level--;
                }
                if (level == 1) {
                    return false;
                }
                for (int i = this.result_l.get(level - 1).size(); i-- > 0; ) {
                    FDResult fd = this.result_l.get(level - 1).get(i);
                    BitSet l = fd.getLhs();
                    BitSet lhs = (BitSet) l.clone();
                    int rhs = fd.getRhs();
                    lhs.and(XX);
                    double sim = lhs.cardinality();
                    sim = (double) Math.round(sim / (XX.cardinality() + l.cardinality() - sim) * 100000) / 100000;
                    sim = sim * this.w1;
                    if (a == rhs) {
                        sim += 1 * this.w2;
                    }
                    double cou = Math.sqrt((l.cardinality() * XX.cardinality()) / (float) this.numberAttributes);
                    if ((w1 + w2 - sim) / (double) (w1 + w2) < (threshold)) {
                        return true;
                    }
                }
                break;
        }
        return false;
    }

    /**
     * Loads the data from the database or a csv file and
     * creates for each attribute a HashMap, which maps the values to a List of tuple ids.
     *
     * @return A ObjectArrayList with the HashMaps.
     */
    private ObjectArrayList<Object2ObjectOpenHashMap<Object, LongBigArrayBigList>> loadData() throws IOException,
            CsvException {
        CSVReader csvReader = new CSVReader(new FileReader(path));
        List<String[]> tuples = csvReader.readAll();
        long t0 = System.currentTimeMillis();
        if (tuples.size() != 0) {
            columnNames = Arrays.asList(tuples.get(0));
            numberAttributes = columnNames.size();
            tuples.remove(0);
            ObjectArrayList<Object2ObjectOpenHashMap<Object, LongBigArrayBigList>> partitions =
                    new ObjectArrayList<Object2ObjectOpenHashMap<Object, LongBigArrayBigList>>(this.numberAttributes);
            for (int i = 0; i < this.numberAttributes; i++) {
                Object2ObjectOpenHashMap<Object, LongBigArrayBigList> partition = new Object2ObjectOpenHashMap<Object
                        , LongBigArrayBigList>();
                partitions.add(partition);
            }
            int tupleId = 0;
            for (String[] row : tuples) {
                for (int i = 0; i < this.numberAttributes; i++) {
                    Object2ObjectOpenHashMap<Object, LongBigArrayBigList> partition = partitions.get(i);
                    String entry = row[i];
                    if (partition.containsKey(entry)) {
                        partition.get(entry).add(tupleId);
                    } else {
                        LongBigArrayBigList newEqClass = new LongBigArrayBigList();
                        newEqClass.add(tupleId);
                        partition.put(entry, newEqClass);
                    }
                }
                tupleId++;
            }
            this.numberTuples = tupleId;
            System.out.println("Partition Time: " + (double) (System.currentTimeMillis() - t0) / 1000 + "s");
            return partitions;
        }
        return new ObjectArrayList<Object2ObjectOpenHashMap<Object, LongBigArrayBigList>>(0);
    }

    /**
     * Initialize Cplus (resp. rhsCandidates) for each combination of the level.
     */
    private void initializeCplusForLevel() {
        for (BitSet X : level1.keySet()) {
            ObjectArrayList<BitSet> CxwithoutA_list = new ObjectArrayList<BitSet>();

            // clone of X for usage in the following loop
            BitSet Xclone = (BitSet) X.clone();
            for (int A = X.nextSetBit(0); A >= 0; A = X.nextSetBit(A + 1)) {
                Xclone.clear(A);
                BitSet CxwithoutA = level0.get(Xclone).getRhsCandidates();
                CxwithoutA_list.add(CxwithoutA);
                Xclone.set(A);
            }

            BitSet CforX = new BitSet();
            if (!CxwithoutA_list.isEmpty()) {
                CforX.set(1, numberAttributes + 1);
                for (BitSet CxwithoutA : CxwithoutA_list) {
                    CforX.and(CxwithoutA);
                }
            }
            SimpleCombinationHelperkey ch = level1.get(X);
            ch.setRhsCandidates(CforX);
        }
    }

    /**
     * implementation 1 of getPartition
     * <p>
     * get a StrippedPartition from mmCombinedPartitions by an FD candidate (node)
     *
     * @param key :an FD candidate (node)
     */
    private StrippedPartition getPartition(BitSet key, SimpleCombinationHelperkey node) {
        BitSet k = (BitSet) key.clone();
        StrippedPartition resultPartition = null;
        resultPartition = mmCombinedPartitions.get(k);
        if (resultPartition != null) {
            node.setMmkey(k);
            return resultPartition;
        }

        ArrayList<BitSet> subs = new ArrayList<>();
        if (node.getMmkey().cardinality() != 0 && mmCombinedPartitions.get(node.getMmkey()) != null) {
            BitSet m1 = (BitSet) node.getMmkey().clone();
            subs.add(node.getMmkey());
            m1.xor(k);
            for (int i = m1.nextSetBit(0); i >= 0; i = m1.nextSetBit(i + 1)) {
                BitSet m2 = new BitSet();
                m2.set(i);
                subs.add(m2);
            }
        } else {
            subs = mmCombinedPartitions.getBestMatchingAttributs(k);
        }

        resultPartition = mmCombinedPartitions.get(subs.get(0));
        BitSet subk = (BitSet) subs.get(0).clone();
        for (int i = 1; i < subs.size(); i++) {
            subk.or(subs.get(i));
            resultPartition = multiply(resultPartition, mmCombinedPartitions.get(subs.get(i)));//new
            // StrippedPartition as the product of the two given StrippedPartitions.
            mmCombinedPartitions.addPartition((BitSet) subk.clone(), resultPartition);
        }
        node.setMmkey(k);
        return resultPartition;
    }

    /**
     * implementation 2 of getPartition
     * <p>
     * get a StrippedPartition from mmCombinedPartitions by an FD candidate (lhs+rhs)
     *
     * @param lkey - attribute set (lhs)
     * @param rkey - an attribute (rhs)
     */
    private StrippedPartition getPartition(BitSet lkey, int rkey, SimpleCombinationHelperkey node) {
        BitSet lhsWithRhs = (BitSet) lkey.clone();
        lhsWithRhs.set(rkey);
        BitSet rhs = new BitSet();
        rhs.set(rkey);
        StrippedPartition resultPartition = null;

        resultPartition = mmCombinedPartitions.get(lhsWithRhs);
        if (resultPartition != null) {
            node.setMmkey(lhsWithRhs);
            return resultPartition;
        }

        if (lkey.cardinality() == 1) {
            resultPartition = multiply(mmCombinedPartitions.get(lkey), mmCombinedPartitions.get(rhs));
            mmCombinedPartitions.addPartition(lhsWithRhs, resultPartition);
            node.setMmkey(lhsWithRhs);
            return resultPartition;
        }

        resultPartition = mmCombinedPartitions.get(lkey);
        if (resultPartition != null) {
            resultPartition = multiply(resultPartition, mmCombinedPartitions.get(rhs));
            mmCombinedPartitions.addPartition(lhsWithRhs, resultPartition);
            node.setMmkey(lhsWithRhs);
            return resultPartition;
        }
        ArrayList<BitSet> subs = new ArrayList<>();
        if (node.getMmkey().cardinality() != 0 && mmCombinedPartitions.get(node.getMmkey()) != null) {
            BitSet m1 = (BitSet) node.getMmkey().clone();
            subs.add(node.getMmkey());
            m1.xor(lhsWithRhs);
            for (int i = m1.nextSetBit(0); i >= 0; i = m1.nextSetBit(i + 1)) {
                BitSet m2 = new BitSet();
                m2.set(i);
                subs.add(m2);
            }
        } else {
            subs = mmCombinedPartitions.getBestMatchingAttributs(lhsWithRhs);
        }

        resultPartition = mmCombinedPartitions.get(subs.get(0));
        BitSet subk = (BitSet) subs.get(0).clone();
        for (int i = 0, j = i + 1; i < subs.size() - 1; i++) {
            subk.or(subs.get(j));
            resultPartition = multiply(resultPartition, mmCombinedPartitions.get(subs.get(j)));
            mmCombinedPartitions.addPartition((BitSet) subk.clone(), resultPartition);
        }
        node.setMmkey(lhsWithRhs);
        return resultPartition;
    }

    /**
     * Computes the dependencies for the current level (level1).
     * <p>
     * one main precess of main loop
     */
    private void computeDependencies() {
        initializeCplusForLevel();

        // iterate through the combinations of the level
        for (BitSet X : level1.keySet()) {
            if (level1.get(X).isValid()) {
                // Build the intersection between X and C_plus(X)
                BitSet C_plus = level1.get(X).getRhsCandidates();
                BitSet intersection = (BitSet) X.clone();
                intersection.and(C_plus);
                // clone of X for usage in the following loop
                BitSet Xclone = (BitSet) X.clone();

                // iterate through all elements (A) of the intersection
                for (int A = intersection.nextSetBit(0); A >= 0; A = intersection.nextSetBit(A + 1)) {
                    if (!level1.get(X).getRhsCandidates().get(A)) {
                        continue;
                    }
                    Xclone.clear(A);//X\A
                    if (!this.checkSimilarity(Xclone, A)) {
                        // check if X\A -> A is valid
                        StrippedPartition spXwithoutA = getPartition(Xclone, level0.get(Xclone));
                        StrippedPartition spX = getPartition(Xclone, A, level1.get(X));
                        if (spX.getError() == spXwithoutA.getError()) {
                            boolean foundValid = false;
                            while (candidateTree.containsGeneralization(Xclone, A, 0)) {
                                BitSet gene = new BitSet();
                                candidateTree.getGeneralizationAndDelete(Xclone, A, candidateTree, 0, gene);
                                SimpleCombinationHelperkey geneNode = skippedNodes.get(gene);
                                StrippedPartition spXwithoutAG = getPartition(gene, geneNode);
                                StrippedPartition spXG = getPartition(gene, A, geneNode);
                                BitSet geneWithA = (BitSet) gene.clone();
                                geneWithA.set(A);
                                if (spXG.getError() == spXwithoutAG.getError()) {
                                    level1.get(X).getRhsCandidates().clear(A);
                                    // remove all B in R\X from C_plus(X)
                                    BitSet RwithoutX = new BitSet();
                                    // set to R
                                    RwithoutX.set(1, numberAttributes + 1);
                                    // remove X
                                    RwithoutX.andNot(geneWithA);
                                    for (int i = RwithoutX.nextSetBit(0); i >= 0; i = RwithoutX.nextSetBit(i + 1)) {
                                        level1.get(X).getRhsCandidates().clear(i);
                                    }
                                    foundValid = true;
                                    break;
                                }
                            }

                            if (!foundValid) {
                                // found Dependency
                                BitSet XwithoutA = (BitSet) Xclone.clone();
                                processFunctionalDependency(XwithoutA, A);
                                // remove A from C_plus(X)
                                level1.get(X).getRhsCandidates().clear(A);
                                // remove all B in R\X from C_plus(X)
                                BitSet RwithoutX = new BitSet();
                                // set to R
                                RwithoutX.set(1, numberAttributes + 1);
                                // remove X
                                RwithoutX.andNot(X);
                                for (int i = RwithoutX.nextSetBit(0); i >= 0; i = RwithoutX.nextSetBit(i + 1)) {
                                    level1.get(X).getRhsCandidates().clear(i);
                                }
                            }
                        }
                    } else {
                        skippedNodes.putIfAbsent((BitSet) Xclone.clone(), level0.get(Xclone));
                        skippedNodes.putIfAbsent((BitSet) X.clone(), level1.get(X));
                        candidateTree.addFunctionalDependency(Xclone, A);
                    }
                    Xclone.set(A);
                }
            }
        }
    }

    /**
     * Check if there is an specialization is ignored key
     */
    private boolean similarityKey(BitSet newK) {
        if (this.keyLHS.size() == 0) {
            return false;
        }

        for (int i = this.keyLHS.size(); i-- > 0; ) {
            BitSet k = (BitSet) this.keyLHS.get(i);
            BitSet lhs = (BitSet) k.clone();
            lhs.and(newK);
            double sim = lhs.cardinality();
            sim = (double) Math.round(sim / (newK.cardinality() + k.cardinality() - sim) * 100000) / 100000;
            sim = sim * this.w1;
            if ((w1 + w2 - sim) / (double) (w1 + w2) <= (threshold)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Prune the current level (level1) by removing all elements with no rhs candidates.
     * All keys are marked as invalid.
     * In case a key is found, minimal dependencies are added to the result receiver.
     */
    private void prune() {
        ObjectArrayList<BitSet> elementsToRemove = new ObjectArrayList<BitSet>();
        for (BitSet x : level1.keySet()) {
            if (level1.get(x).getRhsCandidates().isEmpty()) {
                elementsToRemove.add(x);
                continue;
            }
            // Check if x is a key. Thats the case, if the error is 0.
            // See definition of the error on page 104 of the TANE-99 paper.

            StrippedPartition xPar = mmCombinedPartitions.get(x);
            if (level1.get(x).isValid() && xPar != null) {

                if (xPar.getError() == 0) {
                    elementsToRemove.add(x);
                    if (similarityKey(x)) {
                        elementsToRemove.add(x);
                        continue;
                    }

                    // C+(X)\X
                    BitSet rhsXwithoutX = (BitSet) level1.get(x).getRhsCandidates().clone();
                    rhsXwithoutX.andNot(x);

                    if (rhsXwithoutX.cardinality() != 0) {
                        keyLHS.add(x);
                    }

                    for (int a = rhsXwithoutX.nextSetBit(0); a >= 0; a = rhsXwithoutX.nextSetBit(a + 1)) {
                        BitSet intersect = new BitSet();
                        intersect.set(1, numberAttributes + 1);

                        BitSet xUnionAWithoutB = (BitSet) x.clone();
                        xUnionAWithoutB.set(a);
                        for (int b = x.nextSetBit(0); b >= 0; b = x.nextSetBit(b + 1)) {
                            xUnionAWithoutB.clear(b);
                            SimpleCombinationHelperkey ch = level1.get(xUnionAWithoutB);
                            if (ch != null) {
                                intersect.and(ch.getRhsCandidates());
                            } else {
                                intersect = new BitSet();
                                break;
                            }
                            xUnionAWithoutB.set(b);
                        }

                        if (intersect.get(a)) {
                            BitSet lhs = (BitSet) x.clone();
                            boolean isMini = true;
                            if (isMini) {
                                processFunctionalDependency(lhs, a);
                                level1.get(x).getRhsCandidates().clear(a);
                                level1.get(x).setInvalid();
                                break;
                            }
                        }
                    }
                }
            }
        }
        for (BitSet x : elementsToRemove) {
            level1.remove(x);
        }
    }

    /**
     * Adds the FD lhs -> a to the resultReceiver and also prints the dependency.
     *
     * @param lhs: left-hand-side of the functional dependency
     * @param a:   dependent attribute. Possible values: 1 <= a <= maxAttributeNumber.
     */
    private void processFunctionalDependency(BitSet lhs, int a) {
        addDependencyToResultReceiver(lhs, a);
    }

    /**
     * Calculate the product of two stripped partitions and return the result as a new stripped partition.
     *
     * @param pt1: First StrippedPartition
     * @param pt2: Second StrippedPartition
     * @return A new StrippedPartition as the product of the two given StrippedPartitions.
     */
    public StrippedPartition multiply(StrippedPartition pt1, StrippedPartition pt2) {
        ObjectBigArrayBigList<LongBigArrayBigList> result = new ObjectBigArrayBigList<LongBigArrayBigList>();
        ObjectBigArrayBigList<LongBigArrayBigList> pt1List = pt1.getStrippedPartition();
        ObjectBigArrayBigList<LongBigArrayBigList> pt2List = pt2.getStrippedPartition();
        ObjectBigArrayBigList<LongBigArrayBigList> partition = new ObjectBigArrayBigList<LongBigArrayBigList>();
        long noOfElements = 0;
        // iterate over first stripped partition and fill tTable.
        for (long i = 0; i < pt1List.size64(); i++) {
            for (long tId : pt1List.get(i)) {
                tTable.set(tId, i);
            }
            partition.add(new LongBigArrayBigList());
        }
        // iterate over second stripped partition.
        for (long i = 0; i < pt2List.size64(); i++) {
            for (long t_id : pt2List.get(i)) {
                // tuple is also in an equivalence class of pt1
                if (tTable.get(t_id).longValue() != -1) {
                    partition.get(tTable.get(t_id).longValue()).add(t_id);
                }
            }
            for (long tId : pt2List.get(i)) {
                // if condition not in the paper;
                if (tTable.get(tId).longValue() != -1) {
                    if (partition.get(tTable.get(tId).longValue()).size64() > 1) {
                        LongBigArrayBigList eqClass = partition.get(tTable.get(tId).longValue());
                        result.add(eqClass);
                        noOfElements += eqClass.size64();
                    }
                    partition.set(tTable.get(tId).longValue(), new LongBigArrayBigList());
                }
            }
        }
        // cleanup tTable to reuse it in the next multiplication.
        for (long i = 0; i < pt1List.size64(); i++) {
            for (long tId : pt1List.get(i)) {
                tTable.set(tId, -1);
            }
        }
        return new StrippedPartition(result, noOfElements);
    }

    private int getLastSetBitIndex(BitSet bitset) {
        int lastSetBit = 0;
        for (int A = bitset.nextSetBit(0); A >= 0; A = bitset.nextSetBit(A + 1)) {
            lastSetBit = A;
        }
        return lastSetBit;
    }

    /**
     * Get prefix of BitSet by copying it and removing the last Bit.
     *
     * @param bitset
     * @return A new BitSet, where the last set Bit is cleared.
     */
    private BitSet getPrefix(BitSet bitset) {
        BitSet prefix = (BitSet) bitset.clone();
        prefix.clear(getLastSetBitIndex(prefix));
        return prefix;
    }

    /**
     * Build the prefix blocks for a level. It is a HashMap containing the
     * prefix as a key and the corresponding attributes as  the value.
     */
    private void buildPrefixBlocks() {
        this.prefix_blocks.clear();
        for (BitSet level_iter : level0.keySet()) {
            BitSet prefix = getPrefix(level_iter);

            if (prefix_blocks.containsKey(prefix)) {
                prefix_blocks.get(prefix).add(level_iter);
            } else {
                ObjectArrayList<BitSet> list = new ObjectArrayList<BitSet>();
                list.add(level_iter);
                prefix_blocks.put(prefix, list);
            }
        }
    }

    /**
     * Get all combinations, which can be built out of the elements of a prefix block
     *
     * @param list: List of BitSets, which are in the same prefix block.
     * @return All combinations of the BitSets.
     */
    private ObjectArrayList<BitSet[]> getListCombinations(ObjectArrayList<BitSet> list) {
        ObjectArrayList<BitSet[]> combinations = new ObjectArrayList<BitSet[]>();
        for (int a = 0; a < list.size(); a++) {
            for (int b = a + 1; b < list.size(); b++) {
                BitSet[] combi = new BitSet[2];
                combi[0] = list.get(a);
                combi[1] = list.get(b);
                combinations.add(combi);
            }
        }
        return combinations;
    }

    /**
     * Checks whether all subsets of X (with length of X - 1) are part of the last level.
     * Only if this check return true X is added to the new level.
     *
     * @param X
     * @return
     */
    private boolean checkSubsets(BitSet X) {
        boolean xIsValid = true;

        // clone of X for usage in the following loop
        BitSet Xclone = (BitSet) X.clone();

        for (int l = X.nextSetBit(0); l >= 0; l = X.nextSetBit(l + 1)) {
            Xclone.clear(l);
            if (!level0.containsKey(Xclone)) {
                xIsValid = false;
                break;
            }
            Xclone.set(l);
        }
        return xIsValid;
    }

    private void generateNextLevel() {
        level0 = level1;
        level1 = null;
        System.gc();
        Object2ObjectOpenHashMap<BitSet, SimpleCombinationHelperkey> new_level = new Object2ObjectOpenHashMap<BitSet,
                SimpleCombinationHelperkey>();
        buildPrefixBlocks();

        for (ObjectArrayList<BitSet> prefix_block_list : prefix_blocks.values()) {
            // continue only, if the prefix_block contains at least 2 elements
            if (prefix_block_list.size() < 2) {
                continue;
            }

            ObjectArrayList<BitSet[]> combinations = getListCombinations(prefix_block_list);

            for (BitSet[] c : combinations) {
                BitSet X = (BitSet) c[0].clone();
                X.or(c[1]);

                if (checkSubsets(X)) {
                    StrippedPartition st = null;
                    SimpleCombinationHelperkey ch = new SimpleCombinationHelperkey();
                    if (level0.get(c[0]).isValid() && level0.get(c[1]).isValid()) {
                        if (level0.get(c[0]).getMmkey().cardinality() == 0 && level0.get(c[1]).getMmkey().cardinality() == 0) {
                            ch.setMmkey(c[0]);
                        } else if (level0.get(c[0]).getMmkey().cardinality() > level0.get(c[1]).getMmkey().cardinality()) {
                            ch.setMmkey(level0.get(c[0]).getMmkey());
                        } else {
                            ch.setMmkey(level0.get(c[1]).getMmkey());
                        }

                    } else {
                        ch.setInvalid();
                    }
                    BitSet rhsCandidates = new BitSet();
                    ch.setRhsCandidates(rhsCandidates);
                    new_level.put(X, ch);
                }
            }
        }
        level1 = new_level;
    }

    /**
     * Add the functional dependency to the ResultReceiver.
     *
     * @param X: A BitSet representing the Columns of the determinant.
     * @param a: The number of the dependent column (starting from 1).
     */
    private void addDependencyToResultReceiver(BitSet X, int a) {
        result += 1;
        this.result_l.get(this.currentLevel).add(new FDResult(X, a));

        /**
         * store the result FDs as csv file
         */
        String[] str = new String[2];
        str[0] = String.valueOf(X.nextSetBit(0) - 1);
        for (int i = X.nextSetBit(X.nextSetBit(0) + 1); i >= 0; i = X.nextSetBit(i + 1)) {
            str[0] = str[0] + "," + (i - 1);
        }
        if ("-1".equals(str[0]) || "-2".equals(str[0])) {
            str[0] = "";
        }
        str[1] = String.valueOf(a - 1);
        csvWriter.writeNext(str);
    }

    private void setColumnIdentifiers() {
        this.columnIdentifiers = new ObjectArrayList<ColumnIdentifier>(this.columnNames.size());
        for (String column_name : this.columnNames) {
            columnIdentifiers.add(new ColumnIdentifier(this.tableName, column_name));
        }
    }

    public double compute() throws IOException, CsvException {
        String algorithm = "repFD";
        Representativeness rep = new Representativeness(this.filename, algorithm,
                this.filename + "_" + this.w1 + "-" + this.w2 + "-" + this.threshold);
        double re = rep.compute(w1, w2);
        System.out.println("The total distance of " + algorithm + " method on " + this.filename + "is : " + re);
        System.out.println("The average representativeness of " + algorithm + " method on " + this.filename + "is : " + (1 - re / (double) rep.fd_no));
        return (1 - re / (double) rep.fd_no);
    }

    public static void main(String[] args) throws CsvException, IOException {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        System.out.println(df.format(new Date()));

        String fileName = "adult";
        String filePath = "survey_data/" + fileName + ".csv";
        RepFDKey repFD = new RepFDKey(fileName, filePath, 0.4, 4, 1);
        double[] re = repFD.execute();
        double rep = repFD.compute();
    }
}