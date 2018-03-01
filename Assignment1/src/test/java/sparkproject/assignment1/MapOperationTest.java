package sparkproject.assignment1;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class MapOperationTest {
    MapOperation mapopr;
    @Before
    public void setUp() throws Exception {
        mapopr= new MapOperation();
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testMultipleGoodValues() throws Exception {
        Row testrow1= new GenericRowWithSchema(new Object[]{"4","D","done","kapoor,Krishma","87595","78",false,true}, alteredSchema());
        Row testrow2=new GenericRowWithSchema(new Object[]{"4","C","done","N/A","87595","78000000",true,true}, alteredSchema());
        Row testrow3= new GenericRowWithSchema(new Object[]{"4","A","done","ram,keerthi","87595","0",false,true}, alteredSchema());

        Row testinputrow1= new GenericRowWithSchema(new Object[]{"4","D","done","Krishma _ kapoor","87595","78",false}, old_Schema());
        Row testinputrow2= new GenericRowWithSchema(new Object[]{"4","C","done","","87595","78000000",true}, old_Schema());
        Row testinputrow3= new GenericRowWithSchema(new Object[]{"4","A","done","ram,keerthi","87595","0",false}, old_Schema());

        Row result1=mapopr.call(testinputrow1);
        Row result2=mapopr.call(testinputrow2);
        Row result3=mapopr.call(testinputrow3);

        System.out.println("ouptut rows " +result1
        +result2+result3);

        assertEquals("values of two rows doesn't match: ", true, result1.toString().equals(testrow1.toString()));
        assertEquals("values of two rows doesn't match: ", true, result2.toString().equals(testrow2.toString()));
        assertEquals("values of two rows doesn't match: ", true, result3.toString().equals(testrow3.toString()));
    }


    @Test
    public void testMutipleBadValues() throws Exception{
        Row testrow1= new GenericRowWithSchema(new Object[]{"4","b","done","Krishma","87595","78",false,false}, alteredSchema());
        Row testrow2=new GenericRowWithSchema(new Object[]{"4","D","done","Krishma","87595","0",true,false}, alteredSchema());
        Row testrow3= new GenericRowWithSchema(new Object[]{"4","C","done","Krishma","87595","0",false,false}, alteredSchema());

        Row testinputrow1= new GenericRowWithSchema(new Object[]{"4","b","done","Krishma","87595","78",false}, old_Schema());
        Row testinputrow2= new GenericRowWithSchema(new Object[]{"4","D","done","Krishma","87595","0",true}, old_Schema());
        Row testinputrow3= new GenericRowWithSchema(new Object[]{"4","C","done","Krishma","87595","0",false}, old_Schema());

        Row result1=mapopr.call(testinputrow1);
        Row result2=mapopr.call(testinputrow2);
        Row result3=mapopr.call(testinputrow3);

        assertEquals("values of two rows doesn't match: ", true, result1.equals(testrow1));
        assertEquals("values of two rows doesn't match: ", true, result2.equals(testrow2));
        assertEquals("values of two rows doesn't match: ", true, result3.equals(testrow3));
    }

    public StructType alteredSchema() {
        StructType alteredSchema = DataTypes.createStructType(new org.apache.spark.sql.types.StructField[]{
                DataTypes.createStructField("id", DataTypes.IntegerType, true),
                DataTypes.createStructField("code", DataTypes.StringType, true),
                DataTypes.createStructField("comment", DataTypes.StringType, true),
                DataTypes.createStructField("custname", DataTypes.StringType, true),
                DataTypes.createStructField("datadt", DataTypes.IntegerType, true),
                DataTypes.createStructField("loan_balance", DataTypes.IntegerType, false),
                DataTypes.createStructField("isPremiumAccount", DataTypes.BooleanType, false),
                DataTypes.createStructField("isActiveFlag", DataTypes.BooleanType, false)
        });
        return alteredSchema;
    }

    public StructType old_Schema() {
        StructType old_Schema = DataTypes.createStructType(new org.apache.spark.sql.types.StructField[]{
                DataTypes.createStructField("id", DataTypes.IntegerType, true),
                DataTypes.createStructField("code", DataTypes.StringType, true),
                DataTypes.createStructField("comment", DataTypes.StringType, true),
                DataTypes.createStructField("custname", DataTypes.StringType, true),
                DataTypes.createStructField("datadt", DataTypes.IntegerType, true),
                DataTypes.createStructField("loan_balance", DataTypes.IntegerType, false),
                DataTypes.createStructField("isPremiumAccount", DataTypes.BooleanType, false)
        });
        return old_Schema;
    }


}