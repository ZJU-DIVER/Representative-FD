package RepFD;

import java.util.BitSet;
import java.util.Objects;

public class FDResult {
    private BitSet lhs;
    private int rhs;

    public FDResult(BitSet lhs, int rhs) {
        this.lhs = lhs;
        this.rhs = rhs;
    }

    public BitSet getLhs() {
        return lhs;
    }

    public int getRhs() {
        return rhs;
    }

    public void printFD() {
        System.out.println(lhs + " —→ " + rhs);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FDResult fdResult = (FDResult) o;
        return rhs == fdResult.rhs && Objects.equals(lhs, fdResult.lhs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(lhs, rhs);
    }
}