package RepFD;
//改用hamming距离

import RepFD.EaseCalculation.CombinedPartition;
import RepFD.EaseCalculation.SimpleCombinationHelperHM;
import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import com.opencsv.exceptions.CsvException;
import experiments.Representativeness;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongBigArrayBigList;
import it.unimi.dsi.fastutil.objects.*;
import tools.FDTree;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

public class RepFDHM {
    public String filename;
    private String path;
    private String tableName;
    double threshold;
    int w1, w2;
    private int numberAttributes;
    private long numberTuples;
    private List<String> columnNames;
    private ObjectArrayList<ColumnIdentifier> columnIdentifiers;
    private Object2ObjectOpenHashMap<BitSet, SimpleCombinationHelperHM> level0 = null;
    private Object2ObjectOpenHashMap<BitSet, SimpleCombinationHelperHM> level1 = null;
    private Object2ObjectOpenHashMap<BitSet, ObjectArrayList<BitSet>> prefix_blocks = null;
    private LongBigArrayBigList tTable;
    private int result;
    private List<List<FDResult>> result_l;//fds in level
    private List<FDResult> result_g;//non-minimal fd
    private FDTree candidateTree;//skipped candidate
    private List<BitSet> keyLHS;//keys
    private Object2ObjectOpenHashMap<BitSet, StrippedPartition> singlePartitions = null;
    private CombinedPartition mmCombinedPartitions;
    private int currentLevel = 0;
    public double runtime;
    public int fdNum;
    public CSVWriter csvWriter;
    public int similarityFunc;

    public RepFDHM(String filename, String path, double th, int w1, int w2) {
        this.threshold = th;
        this.w1 = w1;
        this.w2 = w2;
        this.filename = filename;
        this.path = path;
        this.similarityFunc = 3;
    }

    public double[] execute() throws IOException, CsvException {
        long t1 = System.currentTimeMillis();
        result = 0;
        keyLHS = new ArrayList();
//        tK=new TestKmedoids(this.numberAttributes,new ArrayList());
        result_g = new ArrayList<>();
        result_l = new ArrayList<>();
        result_l.add(new ArrayList());
        level0 = new Object2ObjectOpenHashMap<BitSet, SimpleCombinationHelperHM>();//lattice-level0
        level1 = new Object2ObjectOpenHashMap<BitSet, SimpleCombinationHelperHM>();//lattice-level1
        prefix_blocks = new Object2ObjectOpenHashMap<BitSet, ObjectArrayList<BitSet>>();

        // Get information about table from database or csv file
        ObjectArrayList<Object2ObjectOpenHashMap<Object, LongBigArrayBigList>> partitions = loadData();
        setColumnIdentifiers();
        numberAttributes = this.columnNames.size();
        //initialize candidateTree
        candidateTree = new FDTree(this.numberAttributes);
        singlePartitions = new Object2ObjectOpenHashMap<BitSet, StrippedPartition>();
        this.mmCombinedPartitions = new CombinedPartition(this.numberAttributes);//partition manager
        candidateTree.fds_num = 0;//skipped FDs

        // Initialize table used for stripped partition product
        tTable = new LongBigArrayBigList(numberTuples);
        for (long i = 0; i < numberTuples; i++) {
            tTable.add(-1);
        }

        // Initialize Level 0
       // System.out.println("Initialize Level 0");
        SimpleCombinationHelperHM chLevel0 = new SimpleCombinationHelperHM();//格中的node记录的信息，rhscandidates就是C+集
        BitSet rhsCandidatesLevel0 = new BitSet();
        rhsCandidatesLevel0.set(1, numberAttributes + 1);
        chLevel0.setRhsCandidates(rhsCandidatesLevel0);

        for (int A = rhsCandidatesLevel0.nextSetBit(0); A >= 0; A = rhsCandidatesLevel0.nextSetBit(A + 1)) {
            chLevel0.getSimiFDs().put(A, new ObjectOpenHashSet());
        }

        StrippedPartition spLevel0 = new StrippedPartition(numberTuples);
       // System.out.println("把level0的唯一一个node:{} 的partition加入mmCombinedPartitions");
        mmCombinedPartitions.addPartition(new BitSet(), spLevel0);//加入{}的StrippedPartition

        level0.put(new BitSet(), chLevel0);//level0加入唯一的node:  {bitset表示的fd candidate, 该节点相关信息，即rhscandidates,error等}

        String outPath = System.getProperty("user.dir") + "/repFDHM_result/" + filename + "_" + this.w1 + "-" + this.w2 + "-" + this.threshold + ".csv";
        csvWriter = new CSVWriter(new FileWriter(outPath));

        // Initialize Level 1
       // System.out.println("Initialize Level 1");
        for (int i = 1; i <= numberAttributes; i++) {
            BitSet combinationLevel1 = new BitSet();
            combinationLevel1.set(i);

            SimpleCombinationHelperHM chLevel1 = new SimpleCombinationHelperHM();
            BitSet rhsCandidatesLevel1 = new BitSet();
            rhsCandidatesLevel1.set(1, numberAttributes + 1);
            chLevel1.setRhsCandidates(rhsCandidatesLevel0);
            StrippedPartition spLevel1 = new StrippedPartition(partitions.get(i - 1));
            singlePartitions.put(combinationLevel1, spLevel1);
            mmCombinedPartitions.addPartition(combinationLevel1, spLevel1);
            //ch不再把spLevel1放进chLevel1，而是单独记录在了mm里
            level1.put(combinationLevel1, chLevel1);
        }

        // while loop (main part of TANE)
        int l = 1;
        while (!level1.isEmpty() && l <= numberAttributes) {
//            if (currentLevel >= 4 && this.result_l.get(currentLevel).size() == 0) {
//                if (this.result_l.get(currentLevel - 1).size() == 0) {
//                    if (this.result_l.get(currentLevel - 2).size() == 0 && this.result != 0) {
//                        break;
//                    }
//                }
//            }

            if (currentLevel >= 6 && this.filename == "hepatitis") {

                System.out.println("hepatitis提前结束！");//
                break;

            }

            result_l.add(new ArrayList());
            System.out.println("===== Level " + ++currentLevel + " =====");
            // compute dependencies for a level
          //  System.out.println("compute dependencies for a level");
            computeDependencies();
            // prune the search space
          //  System.out.println("prune the search space");
            prune();
            // compute the combinations for the next level：只生成node,不计算partition,不计算rhsCandidate
          //  System.out.println("generate next level without computing partitions");
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
    public boolean checkSimilarity(BitSet XX, int a, BitSet X) {
        int level = this.currentLevel;
        //if(this.result.size()==0){return false;}
        if (this.result == 0) {
            return false;
        }
        switch (this.similarityFunc) {
            case 1:
            case 2://jaccard_distance
                for (int i = this.result_l.get(level).size(); i-- > 0; ) {
                    FDResult fd = this.result_l.get(level).get(i);

                    BitSet l = fd.getLhs();
                    BitSet lhs = (BitSet) l.clone();
                    int rhs = fd.getRhs();
                    lhs.and(XX);

                    double sim = lhs.cardinality();
                    sim = (double) Math.round(sim / (XX.cardinality() + l.cardinality() - sim) * 100000) / 100000;//保留5位小数;
                    sim = sim * this.w1;
                    if (a == rhs) {
                        sim += 1 * this.w2;
                    }
                    if ((sim) / (double) (w1 + w2) > (threshold)) {
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
                    sim = (double) Math.round(sim / (XX.cardinality() + l.cardinality() - sim) * 100000) / 100000;//保留5位小数;
                    sim = sim * this.w1;

                    if (a == rhs) {
                        sim += 1 * this.w2;
                    }
                    if ((sim) / (double) (w1 + w2) > (threshold)) {
                        return true;
                    }
                }
                for (int i = this.result_g.size(); i-- > 0; ) {
                    FDResult fd = this.result_g.get(i);
                    BitSet l = fd.getLhs();
                    BitSet lhs = (BitSet) l.clone();
                    int rhs = fd.getRhs();
                    lhs.and(XX);

                    double sim = lhs.cardinality();
                    sim = (double) Math.round(sim / (XX.cardinality() + l.cardinality() - sim) * 100000) / 100000;//保留5位小数;
                    sim = sim * this.w1;

                    if (a == rhs) {
                        sim += 1 * this.w2;
                    }
                    if ((sim) / (double) (w1 + w2) > (threshold)) {
                        return true;
                    }
                }
                break;
            case 3://hamming distance  //需要传入X  //新的比较方式
                double maxSimi = Integer.MIN_VALUE;
                FDResult simifd = null;

                //比较当前层找到的rfd
                for (int i = this.result_l.get(level).size(); i-- > 0; ) {
                    FDResult fd = this.result_l.get(level).get(i);

                    BitSet l = fd.getLhs();
                    BitSet lhs = (BitSet) l.clone();
                    int rhs = fd.getRhs();
                    lhs.xor(XX);

                    double sim = lhs.cardinality();
                    sim = (double) Math.round((this.numberAttributes - sim) / this.numberAttributes * 100000) / 100000;//保留5位小数;
                    sim = sim * this.w1;
                    if (a == rhs) {
                        sim += 1 * this.w2;
                    }
                    sim = (sim) / (double) (w1 + w2);
                    if (sim > maxSimi) {
                        maxSimi = sim;
                        simifd = fd;
                    }
//                    if ((sim) / (double) (w1 + w2) > (threshold)) {
//                        return true;
//                    }
                }

                //比较simifds
                //Int2ObjectOpenHashMap<ObjectOpenHashSet<FDResult>> simiFDs = level1.get(X).getSimiFDs();
                ObjectOpenHashSet<FDResult> simiFDs = level1.get(X).getSimiFDs().get(a);
                int pos = 0;
                for (FDResult fd : simiFDs) {
                    pos = Math.max(pos, result_l.get(level - 1).indexOf(fd));
                    BitSet l = fd.getLhs();
                    BitSet lhs = (BitSet) l.clone();
                    int rhs = fd.getRhs();
                    lhs.xor(XX);

                    double sim = lhs.cardinality();
                    sim = (double) Math.round((this.numberAttributes - sim) / this.numberAttributes * 100000) / 100000;//保留5位小数;
                    sim = sim * this.w1;
                    if (a == rhs) {
                        sim += 1 * this.w2;
                    }
                    sim = (sim) / (double) (w1 + w2);
                    if (sim > maxSimi) {
                        maxSimi = sim;
                        simifd = fd;
                    }
                }

                //比较下一层simifds之后部分
                if (level >= 2 && this.result_l.get(level - 1).size() != 0) {
                    for (int i = this.result_l.get(level-1).size(); i-- > pos; ) {
                        FDResult fd = this.result_l.get(level-1).get(i);
                        BitSet l = fd.getLhs();
                        BitSet lhs = (BitSet) l.clone();
                        int rhs = fd.getRhs();
                        lhs.xor(XX);

                        double sim = lhs.cardinality();
                        sim = (double) Math.round((this.numberAttributes - sim) / this.numberAttributes * 100000) / 100000;//保留5位小数;
                        sim = sim * this.w1;
                        if (a == rhs) {
                            sim += 1 * this.w2;
                        }
                        sim = (sim) / (double) (w1 + w2);
                        if (sim > maxSimi) {
                            maxSimi = sim;
                            simifd = fd;
                        }
                    }
                }

                for (int i = this.result_g.size(); i-- > 0; ) {
                    FDResult fd = this.result_g.get(i);

                    BitSet l = fd.getLhs();
                    BitSet lhs = (BitSet) l.clone();
                    int rhs = fd.getRhs();
                    lhs.xor(XX);

                    double sim = lhs.cardinality();
                    sim = (double) Math.round((this.numberAttributes - sim) / this.numberAttributes * 100000) / 100000;//保留5位小数;
                    sim = sim * this.w1;
                    // 左手边右手边权重还没定，现在是2：1
                    if (a == rhs) {
                        sim += 1 * this.w2;
                    }
                    //breast-cancer:2---1.5
                    //abalone:2---0.6
                    //bridges:2---1.2

                    //double cou=Math.sqrt((l.cardinality()*XX.cardinality()+1)/(float)this.numberAttributes);
                    //double cou=Math.sqrt((l.cardinality()*XX.cardinality())/(float)this.numberAttributes/(float)this.numberAttributes);
                    //cou*1-1.2的时候,结果挺好,长的基本都因为相似度被剪枝
//                    if (((sim) / (double) (w1 + w2)) > (threshold)) {
//                        System.out.println(XX + " -> " + a + " 与已发现的fd " + l + " -> " + rhs + " 相似度过高,放进FDTree不检查, 相似度:" + (sim) / (double) (w1 + w2) + " ,距离：" + ((w1 + w2 - sim) / (double) (w1 + w2)));//
//                        //System.out.println(i+" / "+this.result_l.get(level).size());
//                        long en = System.currentTimeMillis();
//                        com_timefd += (double) (en - be) / 1000;
//                        return true;
//                    }
                    sim = (sim) / (double) (w1 + w2);
                    if (sim > maxSimi) {
                        maxSimi = sim;
                        simifd = fd;
                    }
                }

                //是否需要先clear再add
                level1.get(X).getSimiFDs().get(a).clear();
                level1.get(X).getSimiFDs().get(a).add(simifd);
                if (maxSimi > threshold) {
                    return true;
                }

                break;
            case 4://hamming 距离  //但不改比较方式（同case2）
                for (int i = this.result_l.get(level).size(); i-- > 0; ) {
                    FDResult fd = this.result_l.get(level).get(i);
                    BitSet l = fd.getLhs();
                    BitSet lhs = (BitSet) l.clone();
                    int rhs = fd.getRhs();
                    lhs.xor(XX);

                    double sim = lhs.cardinality();
                    sim = (double) Math.round((this.numberAttributes - sim) / this.numberAttributes * 100000) / 100000;//保留5位小数;
                    sim = sim * this.w1;

                    if (a == rhs) {
                        sim += 1 * this.w2;
                    }

                    if (((sim) / (double) (w1 + w2)) > (threshold)) {
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
                    lhs.xor(XX);

                    double sim = lhs.cardinality();
                    sim = (double) Math.round((this.numberAttributes - sim) / this.numberAttributes * 100000) / 100000;//保留5位小数;
                    sim = sim * this.w1;
                    if (a == rhs) {
                        sim += 1 * this.w2;
                    }

                    if (((sim) / (double) (w1 + w2)) > (threshold)) {

                        return true;
                    }
                }
                for (int i = this.result_g.size(); i-- > 0; ) {
                    FDResult fd = this.result_g.get(i);
                    BitSet l = fd.getLhs();
                    BitSet lhs = (BitSet) l.clone();
                    int rhs = fd.getRhs();
                    lhs.xor(XX);

                    double sim = lhs.cardinality();
                    sim = (double) Math.round((this.numberAttributes - sim) / this.numberAttributes * 100000) / 100000;//保留5位小数;
                    sim = sim * this.w1;

                    if (a == rhs) {
                        sim += 1 * this.w2;
                    }
                    if ((sim) / (double) (w1 + w2) > (threshold)) {
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
    private ObjectArrayList<Object2ObjectOpenHashMap<Object, LongBigArrayBigList>> loadData() throws IOException, CsvException {
        CSVReader csvReader = new CSVReader(new FileReader(path));
        List<String[]> tuples = csvReader.readAll();
        long t0 = System.currentTimeMillis();
        if (tuples.size() != 0) {
            columnNames = Arrays.asList(tuples.get(0));   //取出第一行属性
            numberAttributes = columnNames.size();
            tuples.remove(0);
            ObjectArrayList<Object2ObjectOpenHashMap<Object, LongBigArrayBigList>> partitions = new ObjectArrayList<Object2ObjectOpenHashMap<Object, LongBigArrayBigList>>(this.numberAttributes);
            for (int i = 0; i < this.numberAttributes; i++) {
                Object2ObjectOpenHashMap<Object, LongBigArrayBigList> partition = new Object2ObjectOpenHashMap<Object, LongBigArrayBigList>();
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
            Int2ObjectOpenHashMap<ObjectOpenHashSet<FDResult>> allSimiFDs = new Int2ObjectOpenHashMap<>();

            // clone of X for usage in the following loop
            BitSet Xclone = (BitSet) X.clone();
            for (int A = X.nextSetBit(0); A >= 0; A = X.nextSetBit(A + 1)) {
                Xclone.clear(A);
                BitSet CxwithoutA = level0.get(Xclone).getRhsCandidates();
                CxwithoutA_list.add(CxwithoutA);

                //initialize allSimiFDs
                Int2ObjectOpenHashMap<ObjectOpenHashSet<FDResult>> simiFDs = level0.get(Xclone).getSimiFDs();
                for (int c : simiFDs.keySet()) {
                    if (!allSimiFDs.containsKey(c)) {
                        allSimiFDs.putIfAbsent(c, new ObjectOpenHashSet());
                    }
                    ObjectOpenHashSet<FDResult> fds = simiFDs.get(c);
                    for (FDResult fd : fds) {
                        allSimiFDs.get(c).add(fd);
                    }
                }

                Xclone.set(A);
            }

            BitSet CforX = new BitSet();

            if (!CxwithoutA_list.isEmpty()) {
                CforX.set(1, numberAttributes + 1);
                for (BitSet CxwithoutA : CxwithoutA_list) {
                    CforX.and(CxwithoutA);
                }
            }

            SimpleCombinationHelperHM ch = level1.get(X);
            ch.setRhsCandidates(CforX);

            //initialize simiFDs
            Int2ObjectOpenHashMap<ObjectOpenHashSet<FDResult>> simiFDs = new Int2ObjectOpenHashMap<>();//ch.getSimiFDs();
            for (int A = CforX.nextSetBit(0); A >= 0; A = CforX.nextSetBit(A + 1)) {
                //allSimiFDs.remove(A);
                simiFDs.put(A, allSimiFDs.get(A));
            }
            ch.setSimiFDs(simiFDs);
        }
    }

    /**
     * implementation 1 of getPartition
     * <p>
     * get a StrippedPartition from mmCombinedPartitions by an FD candidate (node)
     *
     * @param key :an FD candidate (node)
     */
    private StrippedPartition getPartition(BitSet key) {

        BitSet k = (BitSet) key.clone();
        StrippedPartition resultPartition = null;
        resultPartition = mmCombinedPartitions.get(k);//直接通过key找
        if (resultPartition != null) {
            return resultPartition;
        }

        if (k.cardinality() == 1) {
            resultPartition = this.singlePartitions.get(k);
            mmCombinedPartitions.addPartition(k, resultPartition);
            return resultPartition;
        }

        //找合理的最佳s set of subsets的partition(这些subsets可以组成该key)
        ArrayList<StrippedPartition> subLhs = mmCombinedPartitions.getBestMatchingPartitions(k);
        resultPartition = subLhs.get(0);
        for (int i = 0, j = 1; i < subLhs.size() - 1; i++, j++) {
            resultPartition = multiply(resultPartition, subLhs.get(j));//new StrippedPartition as the product of the two given StrippedPartitions.
        }
        mmCombinedPartitions.addPartition(k, resultPartition);
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
    //返回lkey+rkey的partition
    private StrippedPartition getPartition(BitSet lkey, int rkey) {
        BitSet lhsWithRhs = (BitSet) lkey.clone();
        lhsWithRhs.set(rkey);
        BitSet rhs = new BitSet();
        rhs.set(rkey);
        StrippedPartition resultPartition = null;

        resultPartition = mmCombinedPartitions.get(lhsWithRhs);
        if (resultPartition != null) {
            return resultPartition;
        }

        if (lkey.cardinality() == 0) {
            resultPartition = this.singlePartitions.get(rhs);
            mmCombinedPartitions.addPartition(lhsWithRhs, resultPartition);
            return resultPartition;
        }

        if (lkey.cardinality() == 1) {
            resultPartition = multiply(singlePartitions.get(lkey), singlePartitions.get(rhs));
            mmCombinedPartitions.addPartition(lhsWithRhs, resultPartition);
            return resultPartition;
        }

        resultPartition = mmCombinedPartitions.get(lkey);
        if (resultPartition != null) {
            resultPartition = multiply(resultPartition, singlePartitions.get(rhs));
            mmCombinedPartitions.addPartition(lhsWithRhs, resultPartition);
            return resultPartition;
        }

        ArrayList<StrippedPartition> subLhs = mmCombinedPartitions.getBestMatchingPartitions(lkey);

        resultPartition = subLhs.get(0);
        for (int i = 0, j = i + 1; i < subLhs.size() - 1; i++) {
            resultPartition = multiply(resultPartition, subLhs.get(j));
        }

        mmCombinedPartitions.addPartition((BitSet) lkey.clone(), resultPartition);
        resultPartition = multiply(resultPartition, singlePartitions.get(rhs));
        mmCombinedPartitions.addPartition(lhsWithRhs, resultPartition);

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

                    if (!this.checkSimilarity(Xclone, A, X)) {
                        StrippedPartition spXwithoutA = getPartition(Xclone);
                        StrippedPartition spX = getPartition(Xclone, A);
                        //StrippedPartition spX = getPartition(X);//实现了两种getPartition

                        if (spX.getError() == spXwithoutA.getError()) {//成立
                            boolean foundValid = false;
                            int ma = 0;
                            while (candidateTree.containsGeneralization(Xclone, A, 0)) {
                                ma++;
                                BitSet gene = new BitSet();
                                candidateTree.getGeneralizationAndDelete(Xclone, A, candidateTree, 0, gene);
                                //检查gene+" -> "+A是否成立
                                StrippedPartition spXwithoutAG = getPartition(gene);
                                StrippedPartition spXG = getPartition(gene, A);

                                BitSet geneWithA = (BitSet) gene.clone();
                                geneWithA.set(A);

                                //如果generalization成立(且有相似)，就不用管当前candidate了，也不记录这个fd，剪枝后正常往下进行
                                if (spXG.getError() == spXwithoutAG.getError()) {
//                                    result_g.add(new FDResult(Xclone, A));
                                    result_g.add(new FDResult(gene, A));
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
                                level1.get(X).getRhsCandidates().clear(A);
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
                    } else {//相似度高，不判断，FDTRree记录未判断的candidate
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
            sim = (double) Math.round(sim / (newK.cardinality() + k.cardinality() - sim) * 100000) / 100000;//保留5位小数;
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
       // System.out.println("----- prune " + currentLevel + " -----");
        ObjectArrayList<BitSet> elementsToRemove = new ObjectArrayList<BitSet>();
        for (BitSet x : level1.keySet()) {
            if (level1.get(x).getRhsCandidates().isEmpty()) {
                elementsToRemove.add(x);
                continue;
            }
            StrippedPartition xPar = mmCombinedPartitions.get(x);
            if (level1.get(x).isValid() && xPar != null) {
                if (xPar.getError() == 0) {

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
                            SimpleCombinationHelperHM ch = level1.get(xUnionAWithoutB);
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
                            //key输出的fd需要判断是不是最小
                            while (candidateTree.containsGeneralization(lhs, a, 0)) {
                                BitSet lhsG = new BitSet();
                                candidateTree.getGeneralizationAndDelete(lhs, a, candidateTree, 0, lhsG);

                                StrippedPartition spXwithoutA = getPartition(lhsG);
                                BitSet geneWithA = (BitSet) lhsG.clone();
                                geneWithA.set(a);
                                StrippedPartition spX = getPartition(lhsG, a);
                                if (spX.getError() == spXwithoutA.getError()) {
//                                    result_g.add(new FDResult(lhs, a));
                                    result_g.add(new FDResult(lhsG, a));
                                    level1.get(lhs).getRhsCandidates().clear(a);
                                    // remove all B in R\X from C_plus(X)
                                    BitSet RwithoutX = new BitSet();
                                    // set to R
                                    RwithoutX.set(1, numberAttributes + 1);
                                    // remove X
                                    RwithoutX.andNot(geneWithA);
                                    for (int i = RwithoutX.nextSetBit(0); i >= 0; i = RwithoutX.nextSetBit(i + 1)) {
                                        level1.get(lhs).getRhsCandidates().clear(i);
                                    }
                                    isMini = false;
                                    break;
                                }
                            }

                            if (isMini && !this.checkSimilarity(lhs, a, x)) {
                                processFunctionalDependency(lhs, a);
                                level1.get(x).getRhsCandidates().clear(a);
                                level1.get(x).setInvalid();
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

        Object2ObjectOpenHashMap<BitSet, SimpleCombinationHelperHM> new_level = new Object2ObjectOpenHashMap<BitSet, SimpleCombinationHelperHM>();
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
                    //不再计算combanation的partition
                    StrippedPartition st = null;
                    SimpleCombinationHelperHM ch = new SimpleCombinationHelperHM();
                    if (!(level0.get(c[0]).isValid() && level0.get(c[1]).isValid())) {
                        ch.setInvalid();
                    }
                    BitSet rhsCandidates = new BitSet();
                    ch.setRhsCandidates(rhsCandidates);//这里加入的rhsCandidates是空的，还没计算
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
        //result.add(new FDResult(X, a));
        result += 1;

//        tK.addPoints(new DataPoint(new FDResult(X, a)));
//
//        if(tK.ca==null && result.size()>150 ){
//            long tt = System.currentTimeMillis();
//            tK.test(30);
//            long ee = System.currentTimeMillis();
//            double runt = (double)(ee - tt)/1000;
//            jl_time+=runt;
//            System.out.println("首次聚类时间: " + runt + "s");
//        }//第一次聚类形成8个中心点
//
//        if(tK.ca!=null && tK.newP==100 ){
//            long tt = System.currentTimeMillis();
//            tK.reTest(30);
//            long ee = System.currentTimeMillis();
//            double runt = (double)(ee - tt)/1000;
//            jl_time+=runt;
//        }//增量聚类
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
        String algorithm = "repFDHM";//clustering,rank,repFD
        Representativeness rep = new Representativeness(this.filename, algorithm, this.filename + "_" + this.w1 + "-" + this.w2 + "-" + this.threshold);
        double re = rep.computeHM(w1, w2, this.numberAttributes);
        System.out.println("The total similarity of " + algorithm + " method on " + this.filename + "is : " + re);
        System.out.println("The average representativeness of " + algorithm + " method on " + this.filename + "is : " + (re / (double) rep.fd_no));
        return (re / (double) rep.fd_no);
    }

    public static void main(String[] args) throws CsvException, IOException {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        System.out.println(df.format(new Date()));

        String fileName = "ncvoter";//repfd_example
        String filePath = "survey_data/" + fileName + ".csv";
        RepFDHM repFD = new RepFDHM(fileName, filePath, 0.8, 1, 1);
        double[] re = repFD.execute();
        double rep = repFD.compute();
//        repFD.csvWriter.writeNext(new String[]{"Results num","Runtime","Representativeness"});
//        repFD.csvWriter.writeNext(new String[]{re[0]+"",""+re[1],""+rep});
//        repFD.csvWriter.close();
        /*for (FDResult fdresult: repFD.result) {
            fdresult.printFD();
        }*/
    }
}