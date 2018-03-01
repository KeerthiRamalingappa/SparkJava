package sparkproject.assignment1;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class Assignment1 {

    private SparkSession spark;

    public Assignment1(String master,String warehouseDir){
        this.spark=SparkSession.builder().master(master).appName("assignment1")
                .config("spark.sql.warehouse.dir", warehouseDir).getOrCreate();;
    }

    public static void main(String[] args){
        Assignment1 op= new Assignment1("local","C:\\Users\\Keerthi\\spark");
        op.execute();

    }

    public void execute() {
        /*
         here programatically specified schema can be directly applied
         */
        /*
        all the operations returns the values to this method.
         */

        /*
        JavaRDD<Row> returns a file which has set of rows
        otherwise JavaRDD<String> shall return a text file which is same as csv.
        for JavaRDD, JavaSparkContext Object has to be used.
         */

        JavaRDD<Row> inputRdd = readFile("C:\\Users\\Keerthi\\Documents\\SparkAssignments\\assignment_1accountdata.csv");
        JavaRDD<Row> filteredRdd=filterOpr(inputRdd);
        //filteredRdd.saveAsTextFile("C:\\Users\\Keerthi\\Documents\\SparkAssignments\\output6.csv");
        writeFile(filteredRdd, "C:\\Users\\Keerthi\\Documents\\SparkAssignments\\Assignment_1_closed_accounts.txt");
        JavaRDD<Row> splittedRow= splitRow(inputRdd);
        //writeFile(splittedRow, "C:\\Users\\Keerthi\\Documents\\SparkAssignments\\loan_balanceAndIsPremiumAccount.txt");

        JavaRDD<Row> modifiedRows= addFeilds(splittedRow);
        JavaRDD<Row> reducedRowByKey= reduce(modifiedRows);

    }




    public JavaRDD<Row> readFile(String path) {
        Dataset<Row> inputFileDS = spark.read().format("csv")
          //      .option("inferschema", "true")
                .option("header", "true")
                .schema(createSchema())
                .load(path);
        inputFileDS.printSchema();
        JavaRDD<Row> inputRdd = inputFileDS.toJavaRDD();
        return inputRdd;
    }

    public void writeFile(JavaRDD<Row> outputRdd, String outputPath){
        outputRdd.saveAsTextFile(outputPath);
    }

    public StructType createSchema(){
        StructType schema= DataTypes.createStructType(new org.apache.spark.sql.types.StructField[]{
                DataTypes.createStructField("id", DataTypes.IntegerType, true),
                DataTypes.createStructField("code", DataTypes.StringType, true),
                DataTypes.createStructField("subvalues", DataTypes.StringType, true),
                DataTypes.createStructField("comment", DataTypes.StringType, true),
                DataTypes.createStructField("custname", DataTypes.StringType, true),
                DataTypes.createStructField("datadt", DataTypes.IntegerType, true)

        });

        return schema;

    }
    public JavaRDD<Row> filterOpr(JavaRDD<Row> inputRdd){
        JavaRDD<Row> outputRdd=inputRdd.filter(new FilterOperation());
        return (outputRdd);
    }

    public JavaRDD<Row> splitRow(JavaRDD<Row> inputRdd){
        JavaRDD<Row> flattenedRow=inputRdd.flatMap(new FlatMapOperation());
        return (flattenedRow);
    }

    public JavaRDD<Row> addFeilds(JavaRDD<Row> splittedRow) {
        JavaRDD<Row> modifiedRows= splittedRow.map(new MapOperation());
        return modifiedRows;
    }
//    public JavaRDD<Row> reduce(JavaRDD<Row> modifiedRows){
//        JavaRDD<Row> reducedRows=modifiedRows.
//        return
//    }

    public SparkSession getSpark() {
        return spark;
    }
}
