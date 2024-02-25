package format_converter_cli;

import java.io.FileNotFoundException;
import java.io.IOException;
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
        
        String[] validatedInput = FormatConverterCLI.readAndValidateInput();
        String formatSource = validatedInput[0];
        String isFirstRowHeader = validatedInput[1];
        String formatSink = validatedInput[2];
        String pathSource = validatedInput[3];
        String pathSink = validatedInput[4];

        FormatConverterCLI formatConverter = new FormatConverterCLI();
        formatConverter.start(formatSource, isFirstRowHeader, formatSink, pathSource, pathSink);
    }

    private void start(String formatSource,  String isFirstRowHeader, String formatSink, String pathSource, String pathSink) {
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

    private static String[] readAndValidateInput() {
        Scanner scanner = new Scanner(System.in);

        boolean isValid = false;
        String formatSource;
        do {
            System.out.print("The source format (as supported by Apache Spark's DataFrame API): ");
            formatSource = scanner.nextLine();
            try {
                validateFormat(formatSource);
                isValid = true;
            } catch (IllegalArgumentException e) {
                FormatConverterCLI.logger.warning("Unsupported format for source!"
                 + "\n" + e.getMessage() + "\n");
            }
        } while (isValid != true);

        String isFirstRowHeader = "n";
        if (formatSource == "csv") {
            isValid = true;
            do {
                logger.warning("A false positive to the following question will result in data loss!");
                System.out.print("\n" + "Is the first row a header? (Y/N) ");
                isFirstRowHeader = scanner.nextLine().toLowerCase();

                if (!(isFirstRowHeader == "y" || isFirstRowHeader == "n")) {
                    FormatConverterCLI.logger.warning("Choose between Y/N!" + "\n");
                    isValid = false;
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
        
        isValid = false;
        String pathSink;
        do {
            System.out.print("The relative/absolute path to the sink file: ");
            pathSink = scanner.nextLine();
            try {
                validateSinkPath(pathSink);
                isValid = true;
            } catch (IOException e) {
                FormatConverterCLI.logger.warning("Sink file and/or directory tree failed the validation"
                + "\n" + e.getMessage() + "\n");
                isValid = false;
            }
        } while (isValid != true);


        int numberCoresLocal;

        System.out.print("The number of CPU cores you want to use for the conversion: ");
        numberCoresLocal = scanner.nextInt();
        try {
            FormatConverterCLI.validateCores(numberCoresLocal);
            isValid = true;
        } catch (ExcessiveCoresException e) {
            FormatConverterCLI.logger.warning("CPU Available Resources: " +
                 "\n" + e.getMessage() + "\n");
        }

        scanner.close();

        String[] validatedInput =  {formatSource, isFirstRowHeader, formatSink, pathSource, pathSink};

        return validatedInput;
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

    private static void validateSourcePath(String pathSourceFile) throws FileNotFoundException {
        Path path = Paths.get(pathSourceFile);
        if (Files.notExists(path)) {
            throw new FileNotFoundException("The source file could not be found at " + pathSourceFile);
        }
    }

    private static void validateSinkPath(String pathSinkFile) throws IOException {
        Path path = Paths.get(pathSinkFile);
        Path parentDirectories = path.getParent();

        try {
            Files.createDirectories(parentDirectories);
        } catch (IOException e) {
            logger.severe("Directory tree creation failed for " + path);
            throw e;
        }

        try {
            if (Files.notExists(path)) {
                Files.createFile(path);
            }
        } catch (IOException e) {
            logger.severe("File creation failed for " + path);
            throw e;
        }
    }

    final static class ExcessiveCoresException extends Exception {
            public ExcessiveCoresException(String message) {
                super(message);
            }
        }

    private static void validateCores(int numberCores) throws ExcessiveCoresException {
        int numberAvailableCores = Runtime.getRuntime().availableProcessors();
        
        if (numberCores > numberAvailableCores) {
            logger.warning("Number of available cores is " + numberAvailableCores + ", using them all..");
            throw new FormatConverterCLI.ExcessiveCoresException("The number of cores " + numberCores + 
            " exceeds the available resources, setting to maximum...");
        }
    }
}
