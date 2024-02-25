package format_converter_cli;

import java.io.FileNotFoundException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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

public class FormatConverterCLI {
    private static final Logger logger = Logger.getLogger("FormatConverter class");
    public static Set<String> supportedFormats = new HashSet<>(Arrays.asList("csv", "json", "parquet", "orc", "avro"));

    public static void main(String[] args) {   
        Scanner scanner = new Scanner(System.in);

        boolean isValid = false;
        String formatSource;
        do {
            System.out.print("The source format (as supported by the DataFrame API): ");
            formatSource = scanner.nextLine();
            try {
                validateFormat(formatSource);
                isValid = true;
            } catch (IllegalArgumentException e) {
                FormatConverterCLI.logger.warning("Unsupported format for source!"
                 + "\n" + e.getMessage() + "\n");
            }
        } while (isValid != true);

        boolean hasHeader = false;
        if (formatSource == "csv") {
            isValid = false;
            do {
                System.out.println("WARNING: a false positive to the following question will result in data loss!");
                System.out.print("Is the first row a header? (Y/N) ");
                String isFirstRowHeader = scanner.nextLine();
                    if (isFirstRowHeader == "Y") {
                        hasHeader = true;
                    } else if (isFirstRowHeader == "N") {
                        hasHeader = false;
                    } else {
                        FormatConverterCLI.logger.warning("Choose between Y/N!" + "\n");
                    }
            } while (isValid != true);
        }

        isValid = false;
        String formatSink;
        do {
            System.out.print("The sink format (as supported by the DataFrame API): ");
            formatSink = scanner.nextLine();
            try {
                validateFormat(formatSink);
                isValid = true;
            } catch (IllegalArgumentException e) {
                FormatConverterCLI.logger.warning("Error in sink source!"
                + "\n" + e.getMessage() + "\n");
                isValid = false;
            }
        } while (isValid != true);

        isValid = false;
        String pathSource;
        do {
            System.out.print("The relative/absolute path to the source file: ");
            pathSource = scanner.nextLine();
            try {
                validateSourcePath(pathSource);
                isValid = true;
            } catch (FileNotFoundException e) {
                FormatConverterCLI.logger.warning("Source file not found!"
                + "\n" + e.getMessage() + "\n");
                isValid = false;
            }
        } while (isValid != true);
            

        scanner.close();

        FormatConverterCLI formatConverter = new FormatConverterCLI();
        formatConverter.start(formatSource, formatSink, hasHeader);
    }

    private void start(String formatSource, String formatSink, boolean hasHeader) {
        LocalDateTime instantDateTime = LocalDateTime.now();
        String formattedDateTime = formatDateTime(instantDateTime);
        String infoMessage = String.format("Spark parallel format converter initialized at %s", formattedDateTime);
        FormatConverterCLI.logger.info(infoMessage);

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

    private static String formatDateTime(LocalDateTime instantDateTime) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd hh:mm:ss");
        String formattedDateTime = instantDateTime.format((formatter));
        
        return formattedDateTime;
    }

    private static void validateFormat(String format) throws IllegalArgumentException {
        if (!(FormatConverterCLI.supportedFormats.contains(format))) {
            throw new IllegalArgumentException("Invalid format '" + format +
             "', as allowed formats are 'csv', 'json', 'parquet', 'orc', and 'avro'.");
        }
    }

    private static void validateSourcePath(String pathFile) throws FileNotFoundException {
        Path path = Paths.get(pathFile);

        if (!(Files.exists(path))) {
            throw new FileNotFoundException("The source file at "+ path + " was not found!");
        }
    }
}