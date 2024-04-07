package RepFD.EaseCalculation;

import RepFD.StrippedPartition;

import java.util.BitSet;

public class CanTree extends CanTreeElement {
    public int fds_num;

    public CanTree(int maxAttributeNumber) {
        super(maxAttributeNumber);
    }

    public void addMostGeneralDependencies() {
        this.rhsAttributes.set(1, maxAttributeNumber + 1);
        for (int i = 0; i < maxAttributeNumber; i++) {
            isfd[i] = true;
            System.out.println("+ {} → " + (i + 1));
            fds_num++;
        }
    }

    public void addPartitionforLHS(BitSet lhs, StrippedPartition par) {
        CanTreeElement fdTreeEl;
        // update root vertex
        CanTreeElement currentNode = this;
        currentNode.hasMore = true;
        //currentNode.addRhsAttribute(a);

        for (int i = lhs.nextSetBit(0); i >= 0; i = lhs.nextSetBit(i + 1)) {
            if (currentNode.children[i - 1] == null) {
                fdTreeEl = new CanTreeElement(maxAttributeNumber);
                currentNode.children[i - 1] = fdTreeEl;
            }
            // update vertex to add attribute
            currentNode = currentNode.getChild(i - 1);
            currentNode.hasMore = true;
            //currentNode.addRhsAttribute(a);
        }
        // mark the last element
        currentNode.addPartition(par);
        //currentNode.markAsLastVertex(a - 1);
        this.fds_num++;
    }

    public void addFunctionalDependency(BitSet lhs, int a) {
        CanTreeElement fdTreeEl;
        // update root vertex
        CanTreeElement currentNode = this;
        currentNode.addRhsAttribute(a);

        for (int i = lhs.nextSetBit(0); i >= 0; i = lhs.nextSetBit(i + 1)) {
            if (currentNode.children[i - 1] == null) {
                fdTreeEl = new CanTreeElement(maxAttributeNumber);
                currentNode.children[i - 1] = fdTreeEl;
            }
            // update vertex to add attribute
            currentNode = currentNode.getChild(i - 1);
            currentNode.addRhsAttribute(a);
        }
        // mark the last element
        currentNode.markAsLastVertex(a - 1);
        this.fds_num++;
    }

    public boolean isEmpty() {
        return (rhsAttributes.cardinality() == 0);
    }

    public void filterSpecializations() {
        BitSet activePath = new BitSet();
        CanTree filteredTree = new CanTree(maxAttributeNumber);
        this.filterSpecializations(filteredTree, activePath);
        // 用filterTree替换原本的Tree，只留下最specialize的FD
        this.children = filteredTree.children;
        this.isfd = filteredTree.isfd;
        this.fds_num = filteredTree.fds_num;
    }

    public void filterGeneralizations() {
        BitSet activePath = new BitSet();
        CanTree filteredTree = new CanTree(maxAttributeNumber);
        this.filterGeneralizations(filteredTree, activePath);
        this.children = filteredTree.children;
    }

    public void printDependencies() {
        BitSet activePath = new BitSet();
        this.printDependencies(activePath);
    }
}