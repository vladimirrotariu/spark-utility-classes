package format_converter;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Scanner;
import java.util.logging.Logger;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class FormatConverter 
{
    private static final Logger logger = Logger.getLogger("FormatConverter class");

    private static String formatDateTime(LocalDateTime instant_date_time)
    {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd hh:mm:ss");
        String formatted_date_time = instant_date_time.format((formatter));
        
        return formatted_date_time;
    }
    public static void main(String[] args)
    {
        FormatConverter.logger.info("Spark parallel format converter initialized...");
        Scanner scanner = new Scanner(System.in);



        scanner.close();

        FormatConverter app = new FormatConverter();
        app.start();
    }

    private void start()
    {
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
