package sparkproject.assignment1;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Assignment1Test {

    /*
    global test object
     */
    Assignment1 testClass;
    SparkSession spark =testClass.getSpark();


    @org.junit.Before
    public void setUp() throws Exception {
        testClass=new Assignment1("local","C:\\Users\\Keerthi\\spark");

    }

    @org.junit.After
    public void tearDown() throws Exception {
        testClass.getSpark().stop();
    }

    /*
    here we have successfully created a spark object and stopped it.
     */

    @org.junit.Test
    public void execute() throws Exception {
        testClass.execute();
    }

    @org.junit.Test
    public void createSchema() throws Exception {
        testClass.createSchema();
    }

    @org.junit.Test
    public void filterOpr() throws Exception {

        JavaRDD<Row> inputTestRdd = testClass.filterOpr(rawTestData());

    }


    public StructType schema() {
        StructType schema = DataTypes.createStructType(new org.apache.spark.sql.types.StructField[]{
                DataTypes.createStructField("id", DataTypes.IntegerType, true),
                DataTypes.createStructField("code", DataTypes.StringType, true),
                DataTypes.createStructField("subvalues", DataTypes.StringType, true),
                DataTypes.createStructField("comment", DataTypes.StringType, true),
                DataTypes.createStructField("custname", DataTypes.StringType, true),
                DataTypes.createStructField("datadt", DataTypes.IntegerType, true)

        });
        return schema;
    }

    public JavaRDD<Row> rawTestData(){

        List<Row> inputrow = new ArrayList<Row>();
        JavaRDD<Row> inputRdd;
        //List<String> feilds= Arrays.asList("id", "code", "subvalues","comment","custname", "datadt");
                List<Object[]> values = Arrays.asList(

                        new Object[]{"2", "A", "34", "done", "Krishma","87595"},
                        new Object[]{"4", "C", " ", "comment","kareena","426373"},
                        new Object[]{"8", "D", "43","Dummy","", "92354"},
                        new Object[]{"","","","",""},
                        new Object[]{"A", "63", "Dummy", " ", "Mama"}

                );;


        for (int i =0; i<values.size();i++){
            Row row = new GenericRowWithSchema(values.get(i), schema());
            inputrow.add(row);
        }
        /*
         conversion of rows into JavaRDD
         */
         JavaRDD<Row> inputRDD = new JavaSparkContext(spark.sparkContext()).parallelize(inputrow,1);

        return inputRDD;
        }


}






