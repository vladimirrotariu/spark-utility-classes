package csv_to_dataframe_loader;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class CsvToDataframeLoader 
{
    public static void main(String[] args)
    {
        CsvToDataframeLoader app = new CsvToDataframeLoader();
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
