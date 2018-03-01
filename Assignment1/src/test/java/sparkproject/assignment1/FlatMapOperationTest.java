package sparkproject.assignment1;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.util.*;

import static java.util.Arrays.*;
import static org.junit.Assert.*;


public class FlatMapOperationTest {
    FlatMapOperation flatopr;

    @Before
    public void setUp() throws Exception {
        flatopr= new FlatMapOperation();
    }

    @After
    public void tearDown() throws Exception {

    }

    @Test
    public void testMultipleGoodValues() throws Exception {

        Boolean result;
        Row row= new GenericRowWithSchema(new Object[]{"4","c","78;86;21;23;90;34;90000000","done","Krishma","87595"}, schema());
        List<Row> rows= new ArrayList<Row>();
        List<Object[]> flatrows= Arrays.asList(
                                new Object[]{"4","c","done","Krishma","87595","78",false},
                                new Object[]{"4","c","done","Krishma","87595","86",false},
                                new Object[]{"4","c","done","Krishma","87595","21",false},
                                new Object[]{"4","c","done","Krishma","87595","23",false},
                                new Object[]{"4","c","done","Krishma","87595","90",false},
                                new Object[]{"4","c","done","Krishma","87595","34",false},
                                new Object[]{"4","c","done","Krishma","87595","90000000",true}

        );;
        for(int i=0; i<flatrows.size(); i++){
            Row r= new GenericRowWithSchema(flatrows.get(i), alteredSchema());
            rows.add(r);
        }
        Iterator<Row> i=flatopr.call(row);
        Iterator<Row> j= rows.iterator();
        while(i.hasNext()){
            Row iRow=i.next();
            Row jRow=j.next();
            assertTrue("values of two iterators match:"+iRow.toString() + jRow.toString(),iRow.toString().contains(jRow.toString()));
        }
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

    public StructType alteredSchema() {
        StructType alteredSchema = DataTypes.createStructType(new org.apache.spark.sql.types.StructField[]{
                DataTypes.createStructField("id", DataTypes.IntegerType, true),
                DataTypes.createStructField("code", DataTypes.StringType, true),
                DataTypes.createStructField("comment", DataTypes.StringType, true),
                DataTypes.createStructField("custname", DataTypes.StringType, true),
                DataTypes.createStructField("datadt", DataTypes.IntegerType, true),
                DataTypes.createStructField("loan_balance", DataTypes.IntegerType, false)


        });
        return alteredSchema;
    }


}