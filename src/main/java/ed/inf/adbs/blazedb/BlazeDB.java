package ed.inf.adbs.blazedb;

import java.io.FileWriter;
import java.io.BufferedWriter;
import java.io.IOException;

import ed.inf.adbs.blazedb.utility.Parser;
import ed.inf.adbs.blazedb.operator.Operator;
import ed.inf.adbs.blazedb.utility.Planner;

/**
 * Lightweight in-memory database system.
 * <p>
 * Feel free to modify/move the provided functions. However, you must keep
 * the existing command-line interface, which consists of three arguments.
 *
 */
public class BlazeDB {

	public static void main(String[] args) throws IOException {

		if (args.length != 3) {
			System.err.println("Usage: BlazeDB database_dir input_file output_file");
			return;
		}

		String databaseDir = args[0];
		String inputFile = args[1];
		String outputFile = args[2];

		DatabaseCatalog catalog = DatabaseCatalog.getInstance(databaseDir);
		Parser parser = new Parser(inputFile);
		Planner planner = new Planner(parser);
		execute(planner.buildQueryPlan(), outputFile);
	}

	/**
	 * Executes the provided query plan by repeatedly calling `getNextTuple()`
	 * on the root object of the operator tree. Writes the result to `outputFile`.
	 *
	 * @param root The root operator of the operator tree (assumed to be non-null).
	 * @param outputFile The name of the file where the result will be written.
	 */
	public static void execute(Operator root, String outputFile) {
		try {
			// Create a BufferedWriter
			BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile));

			// Iterate over the tuples produced by root
			Tuple tuple = root.getNextTuple();
			while (tuple != null) {
				writer.write(tuple.toCSV());
				writer.newLine();
				tuple = root.getNextTuple();
			}

			// Close the writer
			writer.close();
		}
		catch (IOException e) {
			e.printStackTrace();
		}
	}
}
