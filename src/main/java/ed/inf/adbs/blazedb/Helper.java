package ed.inf.adbs.blazedb;
import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

public class Helper {
    public static List<List<Integer>> CSV_Parser(String filePath) throws IOException {
        List<List<Integer>> data = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                List<Integer> row = Arrays.stream(line.split(",\\s*")) // Split by comma, ignoring spaces
                        .map(Integer::parseInt)   // Convert each value to an Integer
                        .collect(Collectors.toList());
                data.add(row);
            }
        }
        return data;
    }

    public static boolean CSV_Equals(String filePath1, String filePath2) throws IOException {
        List<List<Integer>> data1 = CSV_Parser(filePath1);
        List<List<Integer>> data2 = CSV_Parser(filePath2);

        if (data1.size() != data2.size()) {
            return false;
        }

        for (int i = 0; i < data1.size(); i++) {
            if (!data1.get(i).equals(data2.get(i))) { // Compare lists of integers
                return false;
            }
        }

        return true;
    }

}

