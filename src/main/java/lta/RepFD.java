package RepFD;
//用comninedpartition
//还是jarccard版本/hamming
//聚类

import RepFD.EaseCalculation.CombinedPartition;
import RepFD.EaseCalculation.SimpleCombinationHelper;
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

public class RepFD {
    public String filename;
    private String path;
    private String tableName;
    double threshold;
    int w1, w2;
    private int numberAttributes;
    private long numberTuples;
    private List<String> columnNames;
    private ObjectArrayList<ColumnIdentifier> columnIdentifiers;
    private Object2ObjectOpenHashMap<BitSet, SimpleCombinationHelper> level0 = null;
    private Object2ObjectOpenHashMap<BitSet, SimpleCombinationHelper> level1 = null;
    private Object2ObjectOpenHashMap<BitSet, ObjectArrayList<BitSet>> prefix_blocks = null;
    private LongBigArrayBigList tTable;
    //private List<FDResult> result;
    private int result;
    private List<List<FDResult>> result_l;//记录每个level找到的fd
    private List<FDResult> result_g;//记录验证的非最小fd
    private FDTree candidateTree;//记录未检查的candidate（只存最短的？）
    //    private Object2ObjectOpenHashMap<BitSet, StrippedPartition> candidateRecard = null;//candidateTree中未检查的lhs的partition
//    private TestKmedoids tK;
    private List<BitSet> keyLHS;//记录主键
    private Object2ObjectOpenHashMap<BitSet, StrippedPartition> singlePartitions = null;//存第一层单个属性的patitions
    //    private Object2ObjectOpenHashMap<BitSet, StrippedPartition> partitionRecard = null;
    private CombinedPartition mmCombinedPartitions;//记录计算过的partition
    private int currentLevel = 0;

    public double runtime;
    //    public double jl_time=0.0;
    public double com_timefd = 0.0;//比较相似度的时间
    public double com_timekey = 0.0;//比较相似度的时间
    public double cd_time = 0.0;
    public double nl_time = 0.0;
    public double ct_time1 = 0.0;
    public double ct_time2 = 0.0;
    public double gp_time1 = 0.0;
    public double gp_time2 = 0.0;
    public double gp_time3 = 0.0;
    public double pn_time = 0.0;//剪枝


    public int fdNum;
    public CSVWriter csvWriter;
    public int similarityFunc;//计算相似度的函数

    public RepFD(String filename, String path, double th, int w1, int w2) {
        this.threshold = th;
        this.w1 = w1;
        this.w2 = w2;
        this.filename = filename;
        this.path = path;
        this.similarityFunc = 2;//选择相似度计算方式  2和4相似度计算不一样，记得修改compute方法
    }

    public double[] execute() throws IOException, CsvException {

        long t1 = System.currentTimeMillis();

        //result = new ArrayList();
        result = 0;
        keyLHS = new ArrayList();
//        tK=new TestKmedoids(this.numberAttributes,new ArrayList());
        result_g = new ArrayList<>();
        result_l = new ArrayList<>();
        result_l.add(new ArrayList());
        level0 = new Object2ObjectOpenHashMap<BitSet, SimpleCombinationHelper>();//lattice中的level0
        level1 = new Object2ObjectOpenHashMap<BitSet, SimpleCombinationHelper>();//lattice中的level1
        // candidateRecard = new Object2ObjectOpenHashMap<BitSet, StrippedPartition>();
        prefix_blocks = new Object2ObjectOpenHashMap<BitSet, ObjectArrayList<BitSet>>();

        // Get information about table from database or csv file
        ObjectArrayList<Object2ObjectOpenHashMap<Object, LongBigArrayBigList>> partitions = loadData();
        setColumnIdentifiers();
        numberAttributes = this.columnNames.size();
        //初始化candidateTree——root结点下面的结点是每个属性
        // System.out.println("初始化candidateTree——root结点下面的结点是每个属性");//
        candidateTree = new FDTree(this.numberAttributes);
        singlePartitions = new Object2ObjectOpenHashMap<BitSet, StrippedPartition>();
        this.mmCombinedPartitions = new CombinedPartition(this.numberAttributes);//初始化partition manager

        candidateTree.fds_num = 0;//未检查的fd

//        BitSet ttem=new BitSet();
//        ttem.set(1);
//        ttem.set(2);
//        candidateTree.addFunctionalDependency((BitSet)ttem.clone(),7);
//        ttem.set(4);
//        candidateTree.addFunctionalDependency((BitSet)ttem.clone(),7);
//        BitSet tem=new BitSet();
//        tem.set(1);
//        tem.set(2);
//        tem.set(3);
//        candidateTree.addFunctionalDependency(tem,7);
//        BitSet temm=new BitSet();
//        temm.set(1);
//        temm.set(2);
//        temm.set(3);
//        temm.set(4);
//
//        BitSet gemm=new BitSet();
//        BitSet geem=new BitSet();
//        while (candidateTree.containsGeneralization(temm,7,0)){
//            BitSet gem=new BitSet();
//            candidateTree.getGeneralizationAndDelete(temm,7,candidateTree,0,gem);
//            candidateTree.printDependencies();
//        }


        // Initialize table used for stripped partition product
        tTable = new LongBigArrayBigList(numberTuples);
        for (long i = 0; i < numberTuples; i++) {
            tTable.add(-1);
        }

        // Initialize Level 0
        // System.out.println("Initialize Level 0");//
        SimpleCombinationHelper chLevel0 = new SimpleCombinationHelper();//格中的node记录的信息，rhscandidates就是C+集
        BitSet rhsCandidatesLevel0 = new BitSet();
        rhsCandidatesLevel0.set(1, numberAttributes + 1);
        chLevel0.setRhsCandidates(rhsCandidatesLevel0);
        StrippedPartition spLevel0 = new StrippedPartition(numberTuples);
        // System.out.println("把level0的唯一一个node:{} 的partition加入mmCombinedPartitions");//
        mmCombinedPartitions.addPartition(new BitSet(), spLevel0);//加入{}的StrippedPartition
//        partitionRecard.put(new BitSet(),spLevel0);

        //！不再在每个node记录其partition，用mmCombinedPartitions统一管理，所以注释掉下一行
        //chLevel0.setPartition(spLevel0);
        spLevel0 = null;
        level0.put(new BitSet(), chLevel0);//level0加入唯一的node:  {bitset表示的fd candidate, 该节点相关信息，即rhscandidates,error等}
        chLevel0 = null;

        String outPath = System.getProperty("user.dir") + "/repFD_result/" + filename + "_" + this.w1 + "-" + this.w2 + "-" + this.threshold + ".csv";
        csvWriter = new CSVWriter(new FileWriter(outPath));

        // Initialize Level 1
//        System.out.println("Initialize Level 1");//
//        System.out.println("计算level1的所有partition到mmCombinedPartitions");//
        for (int i = 1; i <= numberAttributes; i++) {
            BitSet combinationLevel1 = new BitSet();
            combinationLevel1.set(i);

            SimpleCombinationHelper chLevel1 = new SimpleCombinationHelper();
            BitSet rhsCandidatesLevel1 = new BitSet();
            rhsCandidatesLevel1.set(1, numberAttributes + 1);
            chLevel1.setRhsCandidates(rhsCandidatesLevel0);
            //初始化singlepartitions，循环加入每个单属性的sp
            StrippedPartition spLevel1 = new StrippedPartition(partitions.get(i - 1));
            singlePartitions.put(combinationLevel1, spLevel1);
            //如果这里加进去
            //这里重复记录了其实，mm里面也记录了所有单属性的sp
            mmCombinedPartitions.addPartition(combinationLevel1, spLevel1);
            //ch不再把spLevel1放进chLevel1，而是单独记录在了mm里
            level1.put(combinationLevel1, chLevel1);
        }
        partitions = null;//生成singlePartitions后，后面用不到了

        // while loop (main part of RepFD)
        int l = 1;
        while (!level1.isEmpty() && l <= numberAttributes) {

//            if (currentLevel >= 4 && this.result_l.get(currentLevel).size() == 0) {
//                if (this.result_l.get(currentLevel - 1).size() == 0) {
//                    if (this.result_l.get(currentLevel - 2).size() == 0 && this.result != 0) {
//                        System.out.println("提前结束！");//
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

            long tcd1 = System.currentTimeMillis();
            // compute dependencies for a level
            //先为当前层计算rhsCandidate，然后一一validate(不一定都计算partition)
            //  System.out.println("compute dependencies for a level");//
            computeDependencies();
            long tcd2 = System.currentTimeMillis();
            cd_time += (double) (tcd2 - tcd1) / 1000;

            long be = System.currentTimeMillis();
            // prune the search space
            //  System.out.println("prune the search space");//
            prune();
            long en = System.currentTimeMillis();
            pn_time += (double) (en - be) / 1000;

            long tnl1 = System.currentTimeMillis();//
            // compute the combinations for the next level
            //只生成node,不计算partition,不计算rhsCandidate
            //  System.out.println("generate next level without computing partitions");//
            generateNextLevel();
            long tnl2 = System.currentTimeMillis();
            nl_time += (double) (tnl2 - tnl1) / 1000;
            // System.out.println("next level 生成的节点 ： " + level1.keySet());////
            l++;
        }
        long end = System.currentTimeMillis();
        runtime = (double) (end - t1) / 1000;
        //fdNum = this.result.size();
        fdNum = this.result;

        double cp_time = com_timefd + com_timekey;

        for (int i = 0; i < this.result_l.size(); i++) {
            for (FDResult fd : this.result_l.get(i)) {
                System.out.println(fd.getLhs() + " -> " + fd.getRhs());
            }
        }
//        for(FDResult fd: this.result){
//            System.out.println(fd.getLhs()+" -> "+fd.getRhs());
//        }
        System.out.println("Fds num: " + fdNum);
        System.out.println("Total Time: " + runtime + "s");
        System.out.println("computedependency Time: " + cd_time + "s"); //" / "+(cd_time-com_timefd-ct_time1-gp_time3) +
        System.out.println("prune时间: " + pn_time + "s");//+" / "+(pn_time-com_timekey-ct_time2)
        System.out.println("nextlevel Time: " + nl_time + "s");
        ct_time1 -= gp_time1;
        ct_time2 -= gp_time2;
        double ct_time = ct_time1 + ct_time2;
        System.out.println("candidatetree Time: " + ct_time + "s");
////        System.out.println("聚类时间: " + jl_time + "s");
        System.out.println("比较时间: " + cp_time + "s");
        System.out.println("getPartition(partition计算)时间: " + (gp_time2 + gp_time1 + gp_time3) + "s");

//        for(BitSet k: this.keyLHS){
//            System.out.println(k);
//        }
//        System.out.println("Key Lhs: " + this.keyLHS.size());

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

        long be = System.currentTimeMillis();
        int level = this.currentLevel;
        //if(this.result.size()==0){return false;}
        if (this.result == 0) {
            return false;
        }

//        String X=XX.toString();
//        X=X.replace("{","");
//        X=X.replace("}","");
//        X=X.replaceAll(",","");
//        X=X.replaceAll(" ","");
//        char[] x= X.toCharArray();
        switch (this.similarityFunc) {
            case 1:
                //edit_distance
//                for(FDResult fd: this.result){
//                    String l=fd.getLhs().toString();
//                    int rhs=fd.getRhs();
//                    l=l.replace("{","");
//                    l=l.replace("}","");
//                    l=l.replaceAll(",","");
//                    l=l.replaceAll(" ","");
//                    l=l.trim();
//                    char[] lhs= l.toCharArray();
//                    //calculate similaruty
//                    float dist_counter = 0;
//                    char[] s1,s2;
//                    int s1Len,s2Len;
//                    if(x.length>lhs.length) {
//                        s1 = x;
//                        s2 = lhs;
//                    } else {
//                        s2 = x;
//                        s1 = lhs;
//                    }
//                    s1Len = s1.length;
//                    s2Len = s2.length;
//
//                    }
            case 2://jaccard_distance
//                for(int i = this.result.size(); i-- > 0;){
//                    FDResult fd=this.result.get(i);
//
//                    BitSet l=fd.getLhs();
//                    BitSet lhs=(BitSet)l.clone();
//                    int rhs=fd.getRhs();
//                    lhs.and(XX);
//
//                    float sim=lhs.cardinality();
//                    sim=sim/(XX.cardinality()+l.cardinality()-sim);
//                    sim=sim*2;
//                    // 左手边右手边权重还没定，现在是2：1
//                    if (a==rhs) {
//                        sim+=1;
//                    }
//                    float cou=l.cardinality()/(float)this.numberAttributes;
//
//                    if((3-sim)<(cou>=0.4?1.4:1)){
//
//                        System.out.println(XX+" -> "+a+" 与已发现的fd " +l+" -> "+rhs+ " 相似度过高,不检查, 相似度:"+sim+" ,距离："+(3-sim));
//                        System.out.println(i+" / "+this.result.size());
//
//
//
//                        return true;
//                    }
//                    }


                //对比聚类
//                if (this.tK.ca!=null){
//                    for(FDResult fd:this.tK.medoids){
//                        BitSet l=fd.getLhs();
//                        BitSet lhs=(BitSet)l.clone();
//                        int rhs=fd.getRhs();
//                        lhs.and(XX);
//
//                        float sim=lhs.cardinality();
//                        sim=sim/(XX.cardinality()+l.cardinality()-sim);
//                        sim=sim*2;
//                        // 左手边右手边权重还没定，现在是2：1
//                        if (a==rhs) {
//                            sim+=1;
//                        }
//                        double cou=Math.sqrt((XX.cardinality()*l.cardinality()+1)/(float)this.numberAttributes);
//
//                        if((3-sim)<(cou*1)){
//
//                            //System.out.println(XX+" -> "+a+" 与已发现的fd " +l+" -> "+rhs+ " 相似度过高,不检查, 相似度:"+sim+" ,距离："+(3-sim));
//                            long en = System.currentTimeMillis();
//                            com_timefd += (double)(en - be)/1000;
//                            return true;
//
//
//                        }
//                    }
//                    if(this.tK.newP!=0){
//                        for(DataPoint f:this.tK.dataPoints.subList(this.tK.dataPoints.size()-tK.newP,this.tK.dataPoints.size())){
//                            FDResult fd=f.getDimensioin();
//                            BitSet l=fd.getLhs();
//                            BitSet lhs=(BitSet)l.clone();
//                            int rhs=fd.getRhs();
//                            lhs.and(XX);
//
//                            float sim=lhs.cardinality();
//                            sim=sim/(XX.cardinality()+l.cardinality()-sim);
//                            sim=sim*2;
//                            // 左手边右手边权重还没定，现在是2：1
//                            if (a==rhs) {
//                                sim+=1;
//                            }
//                            double cou=Math.sqrt((XX.cardinality()*l.cardinality()+1)/(float)this.numberAttributes);
//
//                            if((3-sim)<(cou*1)){
//
//                                //System.out.println(XX+" -> "+a+" 与已发现的fd " +l+" -> "+rhs+ " 相似度过高,不检查, 相似度:"+sim+" ,距离："+(3-sim));
//                                long en = System.currentTimeMillis();
//                                com_timefd += (double)(en - be)/1000;
//                                return true;
//
//
//                            }
//
//
//                        }
//                    }
//                }else{
//                    for(FDResult fd:this.result){
//                        BitSet l=fd.getLhs();
//                        BitSet lhs=(BitSet)l.clone();
//                        int rhs=fd.getRhs();
//                        lhs.and(XX);
//
//                        float sim=lhs.cardinality();
//                        sim=sim/(XX.cardinality()+l.cardinality()-sim);
//                        sim=sim*2;
//                        // 左手边右手边权重还没定，现在是2：1
//                        if (a==rhs) {
//                            sim+=1;
//                        }
//                        double cou=Math.sqrt((XX.cardinality()*l.cardinality()+1)/(float)this.numberAttributes);
//
//                        if((3-sim)<(cou*1)){
//
//                            //System.out.println(XX+" -> "+a+" 与已发现的fd " +l+" -> "+rhs+ " 相似度过高,不检查, 相似度:"+sim+" ,距离："+(3-sim));
//                            long en = System.currentTimeMillis();
//                            com_timefd += (double)(en - be)/1000;
//                            return true;
//                        }
//
//                    }
//                }


                //对比全部


//                while(this.result_l.get(level).size()==0){level--;}
//                while(level>=1){
//                    if(this.result_l.get(level).size()==0){
//                        level--;
//                        continue;
//                    }
//                    for(int i = this.result_l.get(level).size(); i-- > 0;){
//                        FDResult fd=this.result_l.get(level).get(i);
//
//                        BitSet l=fd.getLhs();
//                        BitSet lhs=(BitSet)l.clone();
//                        int rhs=fd.getRhs();
//                        lhs.and(XX);
//
//                        double sim=lhs.cardinality();
//                        sim=(double)Math.round(sim/(XX.cardinality()+l.cardinality()-sim)*100000)/ 100000;//保留5位小数;
//                        sim=sim*this.w1;
//                        // 左手边右手边权重还没定，现在是2：1
//                        if (a==rhs) {
//                            sim+=1*this.w2;
//                        }
//                        //breast-cancer:2---1.5
//                        //abalone:2---0.6
//                        //bridges:2---1.2
//
//                        //double cou=Math.sqrt((l.cardinality()*XX.cardinality()+1)/(float)this.numberAttributes);
//                       // double cou=Math.sqrt((l.cardinality()*XX.cardinality())/(float)this.numberAttributes/(float)this.numberAttributes);
//                        //cou*1-1.2的时候,结果挺好,长的基本都因为相似度被剪枝
//                        if((w1+w2- sim)/(double) (w1+w2)<(threshold)){
//                            //System.out.println(XX+" -> "+a+" 与已发现的fd " +l+" -> "+rhs+ " 相似度过高,放进FDTree不检查, 相似度:"+sim+" ,距离："+(3-sim));
//                            //System.out.println(i+" / "+this.result_l.get(level).size());
//                            long en = System.currentTimeMillis();
//                            com_timefd += (double)(en - be)/1000;
//                            return true;
//                        }
//                    }
//                    level--;
//                }

                // 对比倒数2层

                //对比最后一个有fd的level
                for (int i = this.result_l.get(level).size(); i-- > 0; ) {
                    FDResult fd = this.result_l.get(level).get(i);

                    BitSet l = fd.getLhs();
                    BitSet lhs = (BitSet) l.clone();
                    int rhs = fd.getRhs();
                    lhs.and(XX);

                    double sim = lhs.cardinality();
                    sim = (double) Math.round(sim / (XX.cardinality() + l.cardinality() - sim) * 100000) / 100000;//保留5位小数;
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
                    if (((sim) / (double) (w1 + w2)) > (threshold)) {
                        // System.out.println(XX + " -> " + a + " 与已发现的fd " + l + " -> " + rhs + " 相似度过高,放进FDTree不检查, 相似度:" + (sim) / (double) (w1 + w2) + " ,距离：" + ((w1 + w2 - sim) / (double) (w1 + w2)));//
                        //System.out.println(i+" / "+this.result_l.get(level).size());
                        long en = System.currentTimeMillis();
                        com_timefd += (double) (en - be) / 1000;
                        return true;
                    }
                }
                //倒数2层  不比较，FD多的数据集，有可能变快且代表性变高（因为比较少了，留下的fd也多了）
                while (level >= 2 && this.result_l.get(level - 1).size() == 0) {
                    level--;
                }
                if (level == 1) {
                    long en = System.currentTimeMillis();
                    com_timefd += (double) (en - be) / 1000;
                    return false;
                }
                for (int i = this.result_l.get(level - 1).size(); i-- > 0; ) {
                    FDResult fd = this.result_l.get(level - 1).get(i);

                    BitSet l = fd.getLhs();
                    BitSet lhs = (BitSet) l.clone();
                    int rhs = fd.getRhs();
                    lhs.and(XX);

                    double sim = lhs.cardinality();
                    //sim=sim/(XX.cardinality()+l.cardinality()-sim);
                    sim = (double) Math.round(sim / (XX.cardinality() + l.cardinality() - sim) * 100000) / 100000;//保留5位小数;

                    sim = sim * this.w1;
                    // 左手边右手边权重还没定，现在是2：1
                    if (a == rhs) {
                        sim += 1 * this.w2;
                    }
                    //double cou=Math.sqrt((l.cardinality()*XX.cardinality()+1)/(float)this.numberAttributes);
                    //double cou=Math.sqrt((l.cardinality()*XX.cardinality())/(float)this.numberAttributes);

                    if (((sim) / (double) (w1 + w2)) > (threshold)) {
                        // System.out.println(XX + " -> " + a + " 与已发现的fd " + l + " -> " + rhs + " 相似度过高,放进FDTree不检查, 相似度:" + (w1 + w2 - sim) / (double) (w1 + w2) + " ,距离：" + ((sim) / (double) (w1 + w2)));//
                        //System.out.println(i+" / "+this.result_l.get(level-1).size());
                        long en = System.currentTimeMillis();
                        com_timefd += (double) (en - be) / 1000;
                        return true;
                    }
                }
                //比较result_g
                for (int i = this.result_g.size(); i-- > 0; ) {
                    FDResult fd = this.result_g.get(i);

                    BitSet l = fd.getLhs();
                    BitSet lhs = (BitSet) l.clone();
                    int rhs = fd.getRhs();
                    lhs.and(XX);

                    double sim = lhs.cardinality();
                    //sim=sim/(XX.cardinality()+l.cardinality()-sim);
                    sim = (double) Math.round(sim / (XX.cardinality() + l.cardinality() - sim) * 100000) / 100000;//保留5位小数;

                    sim = sim * this.w1;
                    // 左手边右手边权重还没定，现在是2：1
                    if (a == rhs) {
                        sim += 1 * this.w2;
                    }
                    //double cou=Math.sqrt((l.cardinality()*XX.cardinality()+1)/(float)this.numberAttributes);
                    //double cou=Math.sqrt((l.cardinality()*XX.cardinality())/(float)this.numberAttributes);

                    if (((sim) / (double) (w1 + w2)) > (threshold)) {
                        // System.out.println(XX + " -> " + a + " 与已发现的fd " + l + " -> " + rhs + " 相似度过高,放进FDTree不检查, 相似度:" + (w1 + w2 - sim) / (double) (w1 + w2) + " ,距离：" + ((sim) / (double) (w1 + w2)));//
                        //System.out.println(i+" / "+this.result_l.get(level-1).size());
                        long en = System.currentTimeMillis();
                        com_timefd += (double) (en - be) / 1000;
                        return true;
                    }
                }
                break;

            case 3:
            case 4:
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
                    if ((sim) / (double) (w1 + w2) > (threshold )) {
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
                    if ((sim) / (double) (w1 + w2) > (threshold )) {
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
                    if ((sim) / (double) (w1 + w2) > (threshold )) {
                        return true;
                    }
                }
                break;
        }


        long en = System.currentTimeMillis();
        com_timefd += (double) (en - be) / 1000;
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

//            //
//            BitSet ttt=new BitSet();
//            ttt.set(3);
//            ttt.set(6);
//            ttt.set(8);
//            ttt.set(9);
//            ttt.set(13);
//            ttt.set(16);
//            if(X.equals(ttt)){
//                BitSet tt=new BitSet();
//            }
//            //

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

            SimpleCombinationHelper ch = level1.get(X);
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
    private StrippedPartition getPartition(BitSet key) {

        BitSet k = (BitSet) key.clone();
//        BitSet qis=new BitSet();
//        qis.set(6);
//        qis.set(20);
//        qis.set(21);
        StrippedPartition resultPartition = null;
        resultPartition = mmCombinedPartitions.get(k);//直接通过key找
        if (resultPartition != null) {
            return resultPartition;
        }

        //直接通过key没找到，即mmCombinedPartitions中没有这个node的sp
        //如果是单属性，直接从singlePartitions取（其实这里不需要，单属性应该一定在mmCombinedPartitions有）
        if (k.cardinality() == 1) {
            resultPartition = this.singlePartitions.get(k);
            mmCombinedPartitions.addPartition(k, resultPartition);

            return resultPartition;
        }
//            if(k.equals(qis)){
//                System.out.println(" ");
//            }

        //直接通过key没找到，即mmCombinedPartitions中没有这个node的sp
        //找合理的最佳s set of subsets的partition(这些subsets可以组成该key)
        ArrayList<StrippedPartition> subLhs = mmCombinedPartitions.getBestMatchingPartitions(k);
//        System.out.println(k);
        resultPartition = subLhs.get(0);

//        System.out.println(subLhs.size());

        for (int i = 0, j = 1; i < subLhs.size() - 1; i++, j++) {
            resultPartition = multiply(resultPartition, subLhs.get(j));//new StrippedPartition as the product of the two given StrippedPartitions.
        }
        //resultPartition=multiply(subLhs.get(0),subLhs.get(1));
        mmCombinedPartitions.addPartition(k, resultPartition);
        if (resultPartition == null) {
            System.out.println(" !!!!!!!!!!!!!!!!!!!!!!!!!!!");
        }
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
//        BitSet qis=new BitSet();
//        qis.set(6);
//        qis.set(20);
//        qis.set(21);
        //BitSet lhs=(BitSet)  lkey.clone();
        BitSet lhsWithRhs = (BitSet) lkey.clone();
        lhsWithRhs.set(rkey);
        BitSet rhs = new BitSet();
        rhs.set(rkey);
        StrippedPartition resultPartition = null;

        resultPartition = mmCombinedPartitions.get(lhsWithRhs);//先尝试直接找
        if (resultPartition != null) {
            return resultPartition;
        }

        if (lkey.cardinality() == 0) {
            resultPartition = this.singlePartitions.get(rhs);
            mmCombinedPartitions.addPartition(lhsWithRhs, resultPartition);
            return resultPartition;
        }
//        if(this.mmCombinedPartitions.get(lhsWithRhs.cardinality()).containsKey(lhsWithRhs)){
//            return mmCombinedPartitions.get(lhsWithRhs);
//        }

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
//        if(this.mmCombinedPartitions.get(lkey.cardinality()).containsKey(lkey)){
//            return multiply(mmCombinedPartitions.get(lkey),singlePartitions.get(rhs));
//        }

        //不存在，且lhs长度大于1

        ArrayList<StrippedPartition> subLhs = mmCombinedPartitions.getBestMatchingPartitions(lkey);

        resultPartition = subLhs.get(0);
        for (int i = 0, j = i + 1; i < subLhs.size() - 1; i++) {
            resultPartition = multiply(resultPartition, subLhs.get(j));
        }
        //resultPartition=multiply(subLhs.get(0),subLhs.get(1));
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

//        long tcd1 = System.currentTimeMillis();

        initializeCplusForLevel();//统一为该层的node生成rhsCandidates

        // iterate through the combinations of the level
        for (BitSet X : level1.keySet()) {
//            BitSet qis=new BitSet();
//            qis.set(2);
//            qis.set(3);
//            qis.set(4);
//            qis.set(5);
//            qis.set(6);
//            qis.set(7);
//            qis.set(10);
//            qis.set(11);
//            if(X.equals(qis)){
//                System.out.println(" ");
//            }
            if (level1.get(X).isValid()) {
//                //
//                BitSet ttt=new BitSet();
//                ttt.set(3);
//                ttt.set(6);
//                ttt.set(8);
//                ttt.set(9);
//                ttt.set(13);
//                ttt.set(16);
//                if(X.equals(ttt)){
//                    BitSet tt=new BitSet();
//                }
//                //

                // System.out.println("检查Lattice " + X + "， rhsCandidates有:" + level1.get(X).getRhsCandidates());//

                // Build the intersection between X and C_plus(X) --即要作为rhs检查的元素
                BitSet C_plus = level1.get(X).getRhsCandidates();
                BitSet intersection = (BitSet) X.clone();
                intersection.and(C_plus);
                // System.out.println("intersection " + intersection + "(作为rhs)");//

                // clone of X for usage in the following loop
                BitSet Xclone = (BitSet) X.clone();

                // iterate through all elements (A) of the intersection
                for (int A = intersection.nextSetBit(0); A >= 0; A = intersection.nextSetBit(A + 1)) {

                    if (!level1.get(X).getRhsCandidates().get(A)) {
                        continue;
                    }
                    Xclone.clear(A);//X\A

                    //检查相似度__只同层比较————d+比较不比较全部
                    // System.out.println("检查 " + Xclone + " -> " + A + " 相似度");//
                    if (!this.checkSimilarity(Xclone, A)) {
//                        if(Xclone.equals(qis)){
//                            System.out.println(" ");
//                        }


                        //___________________________________________________________检查fd是否成立
                        //  System.out.println("检查 " + Xclone + " -> " + A + " 是否成立");//
                        // check if X\A -> A is valid
                        long be = System.currentTimeMillis();

                        StrippedPartition spXwithoutA = getPartition(Xclone);
                        StrippedPartition spX = getPartition(Xclone, A);//这个和下面一行哪个好
                        //StrippedPartition spX = getPartition(X);//实现了两种getPartition
                        long en = System.currentTimeMillis();
                        gp_time3 += (double) (en - be) / 1000;

                        if (spX.getError() == spXwithoutA.getError()) {//成立

                            boolean foundValid = false;

                            //无相似，如果成立再找gene，看有没有成立的gene
                            long tct1 = System.currentTimeMillis();

                            int ma = 0;
                            while (candidateTree.containsGeneralization(Xclone, A, 0)) {
                                ma++;
                                BitSet gene = new BitSet();
                                candidateTree.getGeneralizationAndDelete(Xclone, A, candidateTree, 0, gene);
                                //candidateTree.getGeneralization(Xclone, A, candidateTree, 0, gene);
                                //geneList.add(gene);
                                // System.out.println("存在" + Xclone + "->" + A + "的generalization" + gene + " -> " + A);//
//                        }
//                        if(geneList.size()>1){
//                            geneList.sort((o1, o2) -> o1.cardinality()-o2.cardinality());
//                        }
//
//                        for(BitSet gene:geneList){
                                //检查gene+" -> "+A是否成立
                                long bef = System.currentTimeMillis();

                                StrippedPartition spXwithoutAG = getPartition(gene);
                                StrippedPartition spXG = getPartition(gene, A);
                                long enf = System.currentTimeMillis();
                                gp_time1 += (double) (enf - bef) / 1000;

                                BitSet geneWithA = (BitSet) gene.clone();
                                geneWithA.set(A);

                                //StrippedPartition spXG = getPartition(geneWithA);

                                //如果generalization成立(且有相似)，就不用管当前candidate了，也不记录这个fd，剪枝后正常往下进行
                                if (spXG.getError() == spXwithoutAG.getError()) {
                                  //  result_g.add(new FDResult(Xclone, A));
                                    result_g.add(new FDResult(gene, A));
                                    //gene->A   valid--判断是否最小
//                                BitSet geneHelper=(BitSet)gene.clone();
//                                while(candidateTree.containsGeneralization(geneHelper,A,0)){
//                                    BitSet geneG=new BitSet();
//                                    candidateTree.getGeneralizationAndDelete(geneHelper,A,candidateTree,0,geneG);
//                                    StrippedPartition spGeneG = candidateRecard.get(geneG);
//                                    BitSet geneGWithA = (BitSet)geneG.clone();
//                                    geneGWithA.set(A);
//                                    StrippedPartition spGX = candidateRecard.get(geneGWithA);
//                                    //如果geneG->A不成立，说明gene->A是最小的;反之，……
//                                    if (spGX.getError() == spGeneG.getError()) {
//                                        gene=geneG;
//                                        geneWithA = (BitSet)gene.clone();
//                                        geneWithA.set(A);
//                                        geneHelper=(BitSet)gene.clone();;
//                                    }
//                                }

                                    // found Dependency
                                    //BitSet XwithoutA = (BitSet) gene.clone();//好像不需要，直接用gene就行
                                    //System.out.println("Lattice " + X + "(即节点X)中 发现fd：");
                                    //System.out.println(gene+" -> " + A );
                                    //processFunctionalDependency(gene, A);
                                    //System.out.println("remove " +A+"  from C_plus(X):"+ level1.get(X).getRhsCandidates());
                                    // remove A from C_plus(X)
                                    level1.get(X).getRhsCandidates().clear(A);

                                    // remove all B in R\X from C_plus(X)
                                    BitSet RwithoutX = new BitSet();
                                    // set to R
                                    RwithoutX.set(1, numberAttributes + 1);
                                    // remove X
                                    RwithoutX.andNot(geneWithA);
                                    //System.out.println( "remove all B in R/X :  " +RwithoutX+"  from C_plus(X):"+level1.get(X).getRhsCandidates() );

                                    for (int i = RwithoutX.nextSetBit(0); i >= 0; i = RwithoutX.nextSetBit(i + 1)) {
                                        level1.get(X).getRhsCandidates().clear(i);
                                    }
                                    //System.out.println("Lattice " + X + " (即节点X) 目前的rhdCandidate : "+level1.get(X).getRhsCandidates());
                                    foundValid = true;
//                                if(geneList.indexOf(gene)+1!=geneList.size()){
//                                    for(BitSet aa:geneList.subList(geneList.indexOf(gene)+1,geneList.size())){
//                                        candidateTree.addFunctionalDependency(aa,A);
//                                    }
//                                }
                                    break;
                                }

                                //这里找generation的循环怎么才能减少提高效率
//                                if(ma>3){
//                                    break;
//
//                                }
                            }

                            long tct2 = System.currentTimeMillis();
                            ct_time1 += (double) (tct2 - tct1) / 1000;


                            //没有成立的gene，才记录这个
                            if (!foundValid) {

                                // found Dependency
                                BitSet XwithoutA = (BitSet) Xclone.clone();
                                // System.out.println("Lattice " + X + "(即节点X)中 发现fd：");//
                                //System.out.println("Lattice " + X + ":");
                                processFunctionalDependency(XwithoutA, A);
                                //System.out.println("remove " +A+"  from C_plus(X):"+ level1.get(X).getRhsCandidates());
                                // remove A from C_plus(X)
                                level1.get(X).getRhsCandidates().clear(A);

                                // remove all B in R\X from C_plus(X)
                                BitSet RwithoutX = new BitSet();
                                // set to R
                                RwithoutX.set(1, numberAttributes + 1);
                                // remove X
                                RwithoutX.andNot(X);
                                //System.out.println( "remove all B in R/X :  " +RwithoutX+"  from C_plus(X):"+level1.get(X).getRhsCandidates() );

                                for (int i = RwithoutX.nextSetBit(0); i >= 0; i = RwithoutX.nextSetBit(i + 1)) {
                                    level1.get(X).getRhsCandidates().clear(i);
                                }
                                //System.out.println("Lattice " + X + " (即节点X) 目前的rhdCandidate : "+level1.get(X).getRhsCandidates());
                            }
                        }


//                        boolean foundValid=false;
//                        //如果有generalization，先检查generalization
//                        //ArrayList<BitSet> geneList=new ArrayList<BitSet>();
//
//                        long tct1 = System.currentTimeMillis();
//
//                        while( candidateTree.containsGeneralization(Xclone,A,0)){
//                            BitSet gene=new BitSet();
//                            candidateTree.getGeneralizationAndDelete(Xclone,A,candidateTree,0,gene);
//                            //geneList.add(gene);
//                            //System.out.println("存在"+Xclone+"->" +A+"的generalization"+gene+" -> "+A);
////                        }
////                        if(geneList.size()>1){
////                            geneList.sort((o1, o2) -> o1.cardinality()-o2.cardinality());
////                        }
////
////                        for(BitSet gene:geneList){
//                            //检查gene+" -> "+A是否成立
//                            StrippedPartition spXwithoutA = candidateRecard.get(gene);
//                            BitSet geneWithA = (BitSet)gene.clone();
//                            geneWithA.set(A);
//                            StrippedPartition spX = candidateRecard.get(geneWithA);
//
//                            //如果generalization成立，就不用管当前candidate了，也不记录这个fd，剪枝后正常往下进行
//                            if (spX.getError() == spXwithoutA.getError()) {
//                                //gene->A   valid--判断是否最小
////                                BitSet geneHelper=(BitSet)gene.clone();
////                                while(candidateTree.containsGeneralization(geneHelper,A,0)){
////                                    BitSet geneG=new BitSet();
////                                    candidateTree.getGeneralizationAndDelete(geneHelper,A,candidateTree,0,geneG);
////                                    StrippedPartition spGeneG = candidateRecard.get(geneG);
////                                    BitSet geneGWithA = (BitSet)geneG.clone();
////                                    geneGWithA.set(A);
////                                    StrippedPartition spGX = candidateRecard.get(geneGWithA);
////                                    //如果geneG->A不成立，说明gene->A是最小的;反之，……
////                                    if (spGX.getError() == spGeneG.getError()) {
////                                        gene=geneG;
////                                        geneWithA = (BitSet)gene.clone();
////                                        geneWithA.set(A);
////                                        geneHelper=(BitSet)gene.clone();;
////                                    }
////                                }
//
//                                // found Dependency
//                                //BitSet XwithoutA = (BitSet) gene.clone();//好像不需要，直接用gene就行
//                                //System.out.println("Lattice " + X + "(即节点X)中 发现fd：");
//                                System.out.println(gene+" -> " + A );
//                                //processFunctionalDependency(gene, A);
//                                //System.out.println("remove " +A+"  from C_plus(X):"+ level1.get(X).getRhsCandidates());
//                                // remove A from C_plus(X)
//                                level1.get(X).getRhsCandidates().clear(A);
//
//                                // remove all B in R\X from C_plus(X)
//                                BitSet RwithoutX = new BitSet();
//                                // set to R
//                                RwithoutX.set(1, numberAttributes + 1);
//                                // remove X
//                                RwithoutX.andNot(geneWithA);
//                                //System.out.println( "remove all B in R/X :  " +RwithoutX+"  from C_plus(X):"+level1.get(X).getRhsCandidates() );
//
//                                for (int i = RwithoutX.nextSetBit(0); i >= 0; i = RwithoutX.nextSetBit(i + 1)) {
//                                    level1.get(X).getRhsCandidates().clear(i);
//                                }
//                                //System.out.println("Lattice " + X + " (即节点X) 目前的rhdCandidate : "+level1.get(X).getRhsCandidates());
//                                foundValid=true;
////                                if(geneList.indexOf(gene)+1!=geneList.size()){
////                                    for(BitSet aa:geneList.subList(geneList.indexOf(gene)+1,geneList.size())){
////                                        candidateTree.addFunctionalDependency(aa,A);
////                                    }
////                                }
//                                break;
//                            }
//                        }
//
//                        long tct2 = System.currentTimeMillis();
//                        ct_time1 += (double)(tct2 - tct1)/1000;
//
//
//                        if(!foundValid){
//                            //___________________________________________________________检查fd是否成立
//                            //System.out.println("检查 " + Xclone+" -> "+A+" 是否成立");
//                            // check if X\A -> A is valid
//                            StrippedPartition spXwithoutA = level0.get(Xclone).getPartition();
//                            StrippedPartition spX = level1.get(X).getPartition();
//
//                            if (spX.getError() == spXwithoutA.getError()) {
//                                // found Dependency
//                                BitSet XwithoutA = (BitSet) Xclone.clone();
//                                //System.out.println("Lattice " + X + "(即节点X)中 发现fd：");
//                                System.out.println("Lattice " + X + ":");
//                                processFunctionalDependency(XwithoutA, A);
//                                //System.out.println("remove " +A+"  from C_plus(X):"+ level1.get(X).getRhsCandidates());
//                                // remove A from C_plus(X)
//                                level1.get(X).getRhsCandidates().clear(A);
//
//                                // remove all B in R\X from C_plus(X)
//                                BitSet RwithoutX = new BitSet();
//                                // set to R
//                                RwithoutX.set(1, numberAttributes + 1);
//                                // remove X
//                                RwithoutX.andNot(X);
//                                //System.out.println( "remove all B in R/X :  " +RwithoutX+"  from C_plus(X):"+level1.get(X).getRhsCandidates() );
//
//                                for (int i = RwithoutX.nextSetBit(0); i >= 0; i = RwithoutX.nextSetBit(i + 1)) {
//                                    level1.get(X).getRhsCandidates().clear(i);
//                                }
//                                //System.out.println("Lattice " + X + " (即节点X) 目前的rhdCandidate : "+level1.get(X).getRhsCandidates());
//                            }
//                        }
                    } else {//相似度高，不判断，FDTRree记录未判断的candidate
                        // System.out.println(Xclone + "-> " + A + "不判断, 放进FDTRree");//

                        long tct1 = System.currentTimeMillis();

                        candidateTree.addFunctionalDependency(Xclone, A);
//                        StrippedPartition spXwithoutA = getPartition(Xclone);
//                        StrippedPartition spX = getPartition(X);
//                        candidateRecard.put((BitSet) Xclone.clone(),spXwithoutA);
//                        candidateRecard.put((BitSet) X.clone(),spX);

                        long tct2 = System.currentTimeMillis();
                        ct_time1 += (double) (tct2 - tct1) / 1000;
//                        if(candidateTree.fds_num<=7){candidateTree.printDependencies();}
//                        else{System.out.println("记录fd数目："+candidateTree.fds_num);}

//                        //直接作为有效的最小fd处理(直接剪枝了):速度好,肺癌数据结果好,数据集大的话,长的fd舍弃太多,短的留太多,不均匀
//                        BitSet XwithoutA = (BitSet) Xclone.clone();
//                        // System.out.println("Lattice " + X + "(即节点X)中 发现fd：");
//                        //processFunctionalDependency(XwithoutA, A);
//                        // System.out.println("remove " +A+"  from C_plus(X):"+ level1.get(X).getRhsCandidates());
//                        // remove A from C_plus(X)
//                        level1.get(X).getRhsCandidates().clear(A);
//
//                        // remove all B in R\X from C_plus(X)
//                        BitSet RwithoutX = new BitSet();
//                        // set to R
//                        RwithoutX.set(1, numberAttributes + 1);
//                        // remove X
//                        RwithoutX.andNot(X);
//                        // System.out.println( "remove all B in R/X :  " +RwithoutX+"  from C_plus(X):"+level1.get(X).getRhsCandidates() );
//
//                        for (int i = RwithoutX.nextSetBit(0); i >= 0; i = RwithoutX.nextSetBit(i + 1)) {
//                            level1.get(X).getRhsCandidates().clear(i);
//                        }
//                        // System.out.println("Lattice " + X + " (即节点X) 目前的rhdCandidate : "+level1.get(X).getRhsCandidates());
                    }

                    Xclone.set(A);
                }
            } else {
                //System.out.println("Lattice " + X + "无效不检查，rhsCandidate有 : "+level1.get(X).getRhsCandidates());
            }
        }

//        long tcd2 = System.currentTimeMillis();
//        cd_time += (double)(tcd2 - tcd1)/1000;

    }

    /**
     * Check if there is an specialization is ignored key
     *///被忽略的key其实仅仅是不输出（不用在输出的时候判断是否最小），都已经找到了，没有起到很多节省时间的作用，这里的判断好像还增加了时间
    private boolean similarityKey(BitSet newK) {
        long be = System.currentTimeMillis();
        if (this.keyLHS.size() == 0) {
            return false;
        }

        for (int i = this.keyLHS.size(); i-- > 0; ) {//遍历所有key

            BitSet k = (BitSet) this.keyLHS.get(i);
            BitSet lhs = (BitSet) k.clone();
            lhs.and(newK);//重合部分（相同的属性）

            double sim = lhs.cardinality();
            //sim=sim/(newK.cardinality()+k.cardinality()-sim);
            sim = (double) Math.round(sim / (newK.cardinality() + k.cardinality() - sim) * 100000) / 100000;//保留5位小数;

            sim = sim * this.w1;

            if ((w1 + w2 - sim) / (double) (w1 + w2) <= (threshold)) {
                //if(sim>this.threshold-0.1){
                //System.out.println("Key similarity check "+XX+" -> "+a+" 与已发现的fd " +l+" -> "+rhs+ " 相似度过高,放进FDTree不检查, 相似度:"+sim+" ,距离："+(3-sim));
                //System.out.println(i+" / "+this.result_l.get(level).size());
                return true;
            }
        }

        long en = System.currentTimeMillis();
        com_timekey += (double) (en - be) / 1000;
        return false;
    }

    /**
     * Prune the current level (level1) by removing all elements with no rhs candidates.
     * All keys are marked as invalid.
     * In case a key is found, minimal dependencies are added to the result receiver.
     */
    private void prune() {
//        long be = System.currentTimeMillis();
        // System.out.println("----- prune " + currentLevel + " -----");//

        ObjectArrayList<BitSet> elementsToRemove = new ObjectArrayList<BitSet>();
        for (BitSet x : level1.keySet()) {
            if (level1.get(x).getRhsCandidates().isEmpty()) {
                //  System.out.println(x + "getRhsCandidates空，删除节点，不参与generatenextlevel");//
                elementsToRemove.add(x);
                continue;
            }
            // Check if x is a key. Thats the case, if the error is 0.
            // See definition of the error on page 104 of the TANE-99 paper.

            StrippedPartition xPar = mmCombinedPartitions.get(x);
            if (level1.get(x).isValid() && xPar != null) {

                if (xPar.getError() == 0) {
                    //System.out.print(x + "是key.  ");//
                    //elementsToRemove.add(x);
                    //是错的？？如果是相似的key,就直接删除，而不仅仅是invalid
//                    if(similarityKey(x)){
//                        //elementsToRemove.add(x);
//                        continue;
//                    }

                    // C+(X)\X
                    BitSet rhsXwithoutX = (BitSet) level1.get(x).getRhsCandidates().clone();
                    rhsXwithoutX.andNot(x);

//                //
//                BitSet ttt=new BitSet();
//                //ttt.set(3);
//                ttt.set(6);
//                ttt.set(8);
//                ttt.set(9);
//                ttt.set(13);
//                ttt.set(16);
//                if(x.equals(ttt)){
//                    BitSet tt=new BitSet();
//                }
//                //

                    //
                    if (rhsXwithoutX.cardinality() != 0) {
                        keyLHS.add(x);
                    }//
                    //System.out.println("rhs+集为：" + rhsXwithoutX);//

                    for (int a = rhsXwithoutX.nextSetBit(0); a >= 0; a = rhsXwithoutX.nextSetBit(a + 1)) {
                        BitSet intersect = new BitSet();
                        intersect.set(1, numberAttributes + 1);

                        BitSet xUnionAWithoutB = (BitSet) x.clone();
                        xUnionAWithoutB.set(a);
                        for (int b = x.nextSetBit(0); b >= 0; b = x.nextSetBit(b + 1)) {
                            xUnionAWithoutB.clear(b);
                            SimpleCombinationHelper ch = level1.get(xUnionAWithoutB);
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

                            long tct1 = System.currentTimeMillis();

                            //key输出的fd需要判断是不是最小

                            int ma = 0;
                            while (candidateTree.containsGeneralization(lhs, a, 0)) {
                                ma++;
                                BitSet lhsG = new BitSet();
                                candidateTree.getGeneralizationAndDelete(lhs, a, candidateTree, 0, lhsG);
//                                candidateTree.getGeneralization(lhs, a, candidateTree, 0, lhsG);

                                long be2 = System.currentTimeMillis();

                                StrippedPartition spXwithoutA = getPartition(lhsG);
                                BitSet geneWithA = (BitSet) lhsG.clone();
                                geneWithA.set(a);
                                StrippedPartition spX = getPartition(lhsG, a);
                                long en2 = System.currentTimeMillis();
                                gp_time2 += (double) (en2 - be2) / 1000;
                                //如果generalization成立，就不用管当前candidate了，也不记录这个fd，剪枝后正常往下进行
                                if (spX.getError() == spXwithoutA.getError()) {
                                  //  result_g.add(new FDResult(lhs, a));
                                    result_g.add(new FDResult(lhsG, a));
                                    //gene->A   valid--判断是否最小
//                                BitSet geneHelper=(BitSet)gene.clone();
//                                while(candidateTree.containsGeneralization(geneHelper,A,0)){
//                                    BitSet geneG=new BitSet();
//                                    candidateTree.getGeneralizationAndDelete(geneHelper,A,candidateTree,0,geneG);
//                                    StrippedPartition spGeneG = candidateRecard.get(geneG);
//                                    BitSet geneGWithA = (BitSet)geneG.clone();
//                                    geneGWithA.set(A);
//                                    StrippedPartition spGX = candidateRecard.get(geneGWithA);
//                                    //如果geneG->A不成立，说明gene->A是最小的;反之，……
//                                    if (spGX.getError() == spGeneG.getError()) {
//                                        gene=geneG;
//                                        geneWithA = (BitSet)gene.clone();
//                                        geneWithA.set(A);
//                                        geneHelper=(BitSet)gene.clone();;
//                                    }
//                                }

                                    // found Dependency
                                    //BitSet XwithoutA = (BitSet) gene.clone();//好像不需要，直接用gene就行
                                    //System.out.println("Lattice " + X + "(即节点X)中 发现fd：");
                                    //System.out.println(gene+" -> " + A );
                                    //processFunctionalDependency(gene, A);
                                    //System.out.println("remove " +A+"  from C_plus(X):"+ level1.get(X).getRhsCandidates());
                                    // remove A from C_plus(X)
                                    level1.get(lhs).getRhsCandidates().clear(a);

                                    // remove all B in R\X from C_plus(X)
                                    BitSet RwithoutX = new BitSet();
                                    // set to R
                                    RwithoutX.set(1, numberAttributes + 1);
                                    // remove X
                                    RwithoutX.andNot(geneWithA);
                                    //System.out.println( "remove all B in R/X :  " +RwithoutX+"  from C_plus(X):"+level1.get(X).getRhsCandidates() );

                                    for (int i = RwithoutX.nextSetBit(0); i >= 0; i = RwithoutX.nextSetBit(i + 1)) {
                                        level1.get(lhs).getRhsCandidates().clear(i);
                                    }
                                    //System.out.println("Lattice " + X + " (即节点X) 目前的rhdCandidate : "+level1.get(X).getRhsCandidates());
                                    isMini = false;
                                    //  System.out.println("key： " + lhs + " -> " + a + "！！不最小");//
                                    break;
                                }
//                            if(ma>3){
//                                break;
//
//                            }
                            }

                            long tct2 = System.currentTimeMillis();
                            ct_time2 += (double) (tct2 - tct1) / 1000;

                            if (isMini && !this.checkSimilarity(lhs, a)) {
                                //System.out.println("prune key " + x + ":");
                                //  System.out.println("key： " + lhs + " -> " + a + "是最小");//
                                processFunctionalDependency(lhs, a);
                                level1.get(x).getRhsCandidates().clear(a);
                                //System.out.println(x+"的RhsCandidates删去"+a+"设置无效");
                                level1.get(x).setInvalid();

                            }
                        }
                    }
                }
            }

            //——————————————————_________________________________这里可以检查节点，remove右手边
        }
        for (BitSet x : elementsToRemove) {
            level1.remove(x);
        }

//        long en = System.currentTimeMillis();
//        pn_time += (double)(en - be)/1000;
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
        // System.out.println("currunt level 的节点 ： " + level1.keySet());//
//        for (BitSet bitSet : level1.keySet()) {//
//          //  System.out.println(level1.get(bitSet).getRhsCandidates());//
//        }//
        level0 = level1;
        level1 = null;
        System.gc();

        Object2ObjectOpenHashMap<BitSet, SimpleCombinationHelper> new_level = new Object2ObjectOpenHashMap<BitSet, SimpleCombinationHelper>();

//        long tnl1=System.currentTimeMillis();//

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
                    SimpleCombinationHelper ch = new SimpleCombinationHelper();
                    if (level0.get(c[0]).isValid() && level0.get(c[1]).isValid()) {
                        //st = multiply(level0.get(c[0]).getPartition(), level0.get(c[1]).getPartition());
//                        ch.setParent(c[0],c[1]);
                    } else {
                        ch.setInvalid();
                    }
                    BitSet rhsCandidates = new BitSet();

                    //ch.setPartition(st);
                    ch.setRhsCandidates(rhsCandidates);//这里加入的rhsCandidates是空的，还没计算

                    new_level.put(X, ch);
                }
            }

        }

//        long tnl2 = System.currentTimeMillis();
//        nl_time += (double)(tnl2 - tnl1)/1000;

        level1 = new_level;
    }


    /**
     * Add the functional dependency to the ResultReceiver.
     *
     * @param X: A BitSet representing the Columns of the determinant.
     * @param a: The number of the dependent column (starting from 1).
     */
    private void addDependencyToResultReceiver(BitSet X, int a) {
//        System.out.print(X);//
//        System.out.println(" -> " + a);//
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

    public void serialize_attribute(BitSet bitset, CombinationHelper ch) {
        String file_name = bitset.toString();
        ObjectOutputStream oos;
        try {
            oos = new ObjectOutputStream(new FileOutputStream(file_name));
            oos.writeObject(ch);
            oos.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public CombinationHelper deserialize_attribute(BitSet bitset) {
        String file_name = bitset.toString();
        ObjectInputStream is = null;
        CombinationHelper ch = null;
        try {
            is = new ObjectInputStream(new FileInputStream(file_name));
            ch = (CombinationHelper) is.readObject();
            is.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return ch;
    }


    public double compute() throws IOException, CsvException {
        String algorithm = "repFD";//clustering,rank,repFD,repFDBase,repFDHM
        Representativeness rep = new Representativeness(this.filename, algorithm, this.filename + "_" + this.w1 + "-" + this.w2 + "-" + this.threshold);
//        double re = rep.compute(w1, w2);
        double re = rep.computeHM(w1, w2, this.numberAttributes);
        System.out.println("The total similarity of " + algorithm + " method on " + this.filename + "is : " + re);
        System.out.println("The average representativeness of " + algorithm + " method on " + this.filename + "is : " + (re / (double) rep.fd_no));
        return (re / (double) rep.fd_no);

    }

    public static void main(String[] args) throws CsvException, IOException {

        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        System.out.println(df.format(new Date()));

        String fileName = "hepatitis";//repfd_example
        String filePath = "survey_data/" + fileName + ".csv";
        RepFD repfd = new RepFD(fileName, filePath, 0.4, 4, 1);
        double[] re = repfd.execute();
        double rep = repfd.compute();
//        repfd.csvWriter.writeNext(new String[]{"Results num","Runtime","Representativeness"});
//        repfd.csvWriter.writeNext(new String[]{re[0]+"",""+re[1],""+rep});
//        repfd.csvWriter.close();

        /*for (FDResult fdresult: repfd.result) {
            fdresult.printFD();
        }*/
    }
}
