package ed.inf.adbs.blazedb;
import java.io.*;
import java.util.*;

public class Helper {
    public static List<String[]> CSV_Parser(String filePath) throws IOException {
        List<String[]> data = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                data.add(line.split(","));
            }
        }
        return data;
    }

    public static boolean CSV_Equals(String filePath1, String filePath2) throws IOException {
        List<String[]> data1 = CSV_Parser(filePath1);
        List<String[]> data2 = CSV_Parser(filePath2);

        if (data1.size() != data2.size()) {
            return false;
        }

        for (int i = 0; i < data1.size(); i++) {
            if (!Arrays.equals(data1.get(i), data2.get(i))) {
                return false;
            }
        }

        return true;
    }
}

