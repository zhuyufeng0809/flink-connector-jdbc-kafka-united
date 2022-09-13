package oh.awesome.flink.split;

import com.google.common.base.Objects;

public class Range {
    private Long lowerBound;
    private Long upperBound;

    public Range() {
    }

    public Range(Long lowerBound, Long upperBound) {
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
    }

    public Long getLowerBound() {
        return lowerBound;
    }

    public Long getUpperBound() {
        return upperBound;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Range range = (Range) o;
        return Objects.equal(lowerBound, range.lowerBound) && Objects.equal(upperBound, range.upperBound);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(lowerBound, upperBound);
    }

    @Override
    public String toString() {
        return String.join("-", lowerBound.toString(), upperBound.toString());
    }
}
