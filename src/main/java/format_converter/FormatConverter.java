package format_converter;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;
import java.util.logging.Logger;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class FormatConverter {
    private static final Logger logger = Logger.getLogger("FormatConverter class");
    public static Set<String> supportedFormats = new HashSet<>(Arrays.asList("csv", "json", "parquet", "orc", "avro"));

    private static String formatDateTime(LocalDateTime instantDateTime) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd hh:mm:ss");
        String formattedDateTime = instantDateTime.format((formatter));
        
        return formattedDateTime;
    }

    private static void validateSourceFormat(String formatSource) {
        if (!(FormatConverter.supportedFormats.contains(formatSource))) {
            throw new IllegalArgumentException("Invalid format '" + formatSource + "', as allowed formats are 'csv', 'json', 'parquet', 'orc', and 'avro'.");
        }
    }
    public static void main(String[] args) {   
        Scanner scanner = new Scanner(System.in);

        System.out.print("The source format (as supported by the DataFrame API): ");
        String formatSource = scanner.nextLine();
        try {
            validateSourceFormat(formatSource);
        } catch (IllegalArgumentException e) {
            FormatConverter.logger.severe("Error in format source:" + "\n" + e.getMessage());
        }

        System.out.print("The sink format (as supported by the DataFrame API): ");
        String formatSink = scanner.nextLine();

        scanner.close();

        FormatConverter formatConverter = new FormatConverter();
        formatConverter.start(formatSource, formatSink);
    }

    private void start(String formatSource, String formatSink) {
        LocalDateTime instantDateTime = LocalDateTime.now();
        String formattedDateTime = formatDateTime(instantDateTime);
        String infoMessage = String.format("Spark parallel format converter initialized at %s", formattedDateTime);
        FormatConverter.logger.info(infoMessage);

        SparkSession spark = SparkSession.builder()
            .appName("Demo app loading csv data to dataframe")
            .master("local[*]")
            .getOrCreate();

        Dataset<Row> df = spark.read()
                               .format("csv")
                               .option("header", "false") //may be omitted if no header
                               .load("src/main/data/coin_sequences.txt");
        
        df.show();
    }
}
