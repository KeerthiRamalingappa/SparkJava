package sparkproject.assignment1;

import org.apache.avro.generic.GenericData;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.apache.spark.api.java.JavaRDD;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;


public class FilterOperationTest {
    FilterOperation filterOperation;

    @Before
    public void setUp() throws Exception {
        filterOperation= new FilterOperation();
    }

    @After
    public void tearDown() throws Exception {

    }

    @Test
    public void testTrueWithGoodData() throws Exception {
        //Create Input data Row
        Row row = new GenericRowWithSchema(new Object[]{"2", "A", "34", "done", "Krishma","87595"}, schema());
        //Call Test value and get Result back
        Boolean result=filterOperation.call(row);
        //Compare test return value with hard coded expeted value using assertEquals
        assertEquals("Actual Value doesn't match execpted",true, result);

    }

    @Test
    public void testFalseWithBadData() throws Exception {
        //Create Input data Row
        Row row1 = new GenericRowWithSchema(new Object[]{"4", "c", "  ", "done", "Krishma","87595"}, schema());
        //Call Test value and get Result back
        Boolean result=filterOperation.call(row1);
        //Compare test return value with hard coded expeted value using assertEquals
        assertEquals("Actual Value doesn't match execpted", false, result);

        Row row2= new GenericRowWithSchema(new Object[]{"","c", "  ", "", "", ""}, schema());
        Boolean result2= filterOperation.call(row2);
        assertEquals("Actual value dosent't match expected", false, result2);
    }

    @Test
    public void call() throws Exception {

        List<Row> testrow= rawTestData();
        for(int i=0;i<testrow.size(); i++) {
            System.out.print(testrow.get(i));
            Boolean result= filterOperation.call(testrow.get(i));
            System.out.println(result);
        }

    }

    public List<Row> rawTestData(){

        List<Row> inputrow = new ArrayList<Row>();
        List<Object[]> values = Arrays.asList(

                new Object[]{"2", "A", "34", "done", "Krishma","87595"},
                new Object[]{"4", "c"," ","comment","kareena","426373"},
                new Object[]{"8", "D", "43","Dummy","", "92354"},
                new Object[]{"","c"," ","",""},
                new Object[]{"A", "63", "Dummy", " ", "Mama"}

        );;


        for (int i =0; i<values.size();i++){
            Row row = new GenericRowWithSchema(values.get(i), schema());
            inputrow.add(row);
        }

        return inputrow;
    }

    public StructType schema() {
        StructType schema = DataTypes.createStructType(new org.apache.spark.sql.types.StructField[]{
                DataTypes.createStructField("id", DataTypes.IntegerType, true),
                DataTypes.createStructField("code", DataTypes.StringType, true),
                DataTypes.createStructField("subvalues", DataTypes.IntegerType, true),
                DataTypes.createStructField("comment", DataTypes.StringType, true),
                DataTypes.createStructField("custname", DataTypes.StringType, true),
                DataTypes.createStructField("datadt", DataTypes.IntegerType, true)

        });
        return schema;
    }


}