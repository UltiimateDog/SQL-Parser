package ed.inf.adbs.blazedb;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;
import java.util.Arrays;

/**
 * Represents a row (tuple) in a database table.
 */
@Data
public class Tuple {
    private List<String> values; // Stores column values as a list

    /**
     * Alternative constructor to initialize a Tuple from an array of values.
     * @param values An array of attribute values.
     */
    public Tuple(String[] values) {
        this.values = Arrays.asList(values);
    }

    /**
     * Gets a value from the tuple at a given column index.
     * @param index The column index.
     * @return The value at the given index.
     */
    public String getValue(int index) {
        return values.get(index);
    }

    public String toCSV() {
        return String.join(",", values);
    }
}
