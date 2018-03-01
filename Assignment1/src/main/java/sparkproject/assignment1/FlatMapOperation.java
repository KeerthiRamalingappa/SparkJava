/*
loan_balance and isPremiumAccount is added to the rows in this class.
 */

package sparkproject.assignment1;

import org.apache.commons.lang.ArrayUtils;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class FlatMapOperation implements FlatMapFunction<Row, Row>{
public final Integer PREMIUM_VALUE= Integer.valueOf("10000000");

public Iterator<Row> call(Row row) throws Exception {

        Integer fieldIndexSubvalues= (Integer) row.schema().getFieldIndex("subvalues").get();
        Integer[] fixedFieldIndex= {
                (Integer)row.schema().getFieldIndex("id").get(),
                (Integer)row.schema().getFieldIndex("code").get(),
                (Integer)row.schema().getFieldIndex("comment").get(),
                (Integer)row.schema().getFieldIndex("custname").get(),
                (Integer)row.schema().getFieldIndex("datadt").get()
        };
        String s= row.getString(fieldIndexSubvalues);
        List<String> ls= Arrays.asList(s.trim().split(";"));
        List<Row> rows =new ArrayList<Row>();
        /*
        propogated values and modified values
         */

        Object[] propogatedValues = ArrayUtils.EMPTY_OBJECT_ARRAY;
        Object[] modifiedValues = ArrayUtils.EMPTY_OBJECT_ARRAY;


        for(int i=0;i<fixedFieldIndex.length;i++){
            propogatedValues =ArrayUtils.add(propogatedValues, row.get(fixedFieldIndex[i]));
        }
/*
here ls contains String subvalues, which are later split and changed the column name into
loan_balance.

Now here if loan_balanace is greater than 10 million, isPremium column is added and set to true.

 */
        for(int i=0; i<ls.size();i++){
            if(ls.get(i).trim().equals("")){
            modifiedValues =ArrayUtils.add(propogatedValues, ls.get(i));
            modifiedValues =ArrayUtils.add(modifiedValues, false);
            }
            else if(PREMIUM_VALUE <Integer.parseInt(ls.get(i))) {
                modifiedValues = ArrayUtils.add(propogatedValues, ls.get(i));
                modifiedValues =ArrayUtils.add(modifiedValues,true);
            }
            else {
                modifiedValues = ArrayUtils.add(propogatedValues, ls.get(i));
                modifiedValues =ArrayUtils.add(modifiedValues,false);
            }
            Row modifiedRow = new GenericRowWithSchema(modifiedValues, alteredSchema());
            rows.add(modifiedRow);
            System.out.println("new rows are "+ modifiedRow);
        }

        return rows.iterator();
    }

    public StructType alteredSchema() {
        StructType alteredSchema = DataTypes.createStructType(new org.apache.spark.sql.types.StructField[]{
                DataTypes.createStructField("id", DataTypes.IntegerType, true),
                DataTypes.createStructField("code", DataTypes.StringType, true),
                DataTypes.createStructField("comment", DataTypes.StringType, true),
                DataTypes.createStructField("custname", DataTypes.StringType, true),
                DataTypes.createStructField("datadt", DataTypes.IntegerType, true),
                DataTypes.createStructField("loan_balance", DataTypes.IntegerType, false),
                DataTypes.createStructField("isPremiumAccount", DataTypes.BooleanType, false)
        });
        return alteredSchema;
    }

}


