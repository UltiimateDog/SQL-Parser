package ed.inf.adbs.blazedb;

import lombok.Data;
import java.util.List;
import java.util.Arrays;

/**
 * Represents a row (tuple) in a database table.
 * Each tuple stores its attribute values as a list of strings.
 */
@Data
public class Tuple {
    private List<String> values; // Stores column values as an immutable list

    /**
     * Constructs a Tuple from an array of attribute values.
     * The values are stored as a list using Arrays.asList(), which creates a fixed-size list.
     *
     * @param values An array of attribute values.
     */
    public Tuple(String[] values) {
        this.values = Arrays.asList(values);
    }

    /**
     * Retrieves the value at the specified column index.
     * Note: This method removes all whitespace from the retrieved value.
     *
     * @param index The column index (zero-based).
     * @return The value at the given index with all whitespace removed.
     * @throws IndexOutOfBoundsException if the index is out of range.
     */
    public String getValue(int index) {
        return values.get(index).replaceAll("\\s", "");
    }

    /**
     * Converts the tuple into a CSV-formatted string.
     * Each value is joined by a comma without additional formatting.
     *
     * @return A string representation of the tuple in CSV format.
     */
    public String toCSV() {
        return String.join(",", values);
    }
}
