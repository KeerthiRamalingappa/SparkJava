package sparkproject.assignment1;

import com.sun.istack.internal.NotNull;
import org.apache.commons.lang.ArrayUtils;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MapOperation implements Function<Row, Row> {

    public Row call(Row row) throws Exception {

        Integer index_loan_balance = (Integer) row.schema().getFieldIndex("loan_balance").get();
        Integer index_code = (Integer) row.schema().getFieldIndex("code").get();
        Integer index_custname= (Integer) row.schema().getFieldIndex("custname").get();
        String loan_balance_value= (String) row.get(index_loan_balance);
        String custname_value= (String)row.get(index_custname);

        custname_value=custname_value.replaceAll("[^a-zA-Z0-9, ]", "");
        custname_value=custname_value.trim();
        String full_name="";

        System.out.println("custname: "+custname_value);
        if(custname_value.equals("")){
            full_name="N/A";
        }
        else if(custname_value.contains(",")) {
            List<String> newCustname = Arrays.asList(custname_value.split(","));
            String first_name = newCustname.get(1);
            String last_name = newCustname.get(0);
            full_name = last_name.concat(",").concat(first_name);
        }
        else if(custname_value.contains(" ")||custname_value.contains("  ")){
            List<String> newCustname=Arrays.asList(custname_value.split(" "));
                String first_name = newCustname.get(0);
                String last_name = newCustname.get(newCustname.size()-1);
            full_name = last_name.concat(",").concat(first_name);
        }
        else{
            full_name=custname_value;
        }

        Integer[] fixedIndex = {
                (Integer) row.schema().getFieldIndex("id").get(),
                (Integer) row.schema().getFieldIndex("code").get(),
                (Integer) row.schema().getFieldIndex("comment").get(),
                (Integer) row.schema().getFieldIndex("custname").get(),
                (Integer) row.schema().getFieldIndex("datadt").get(),
                (Integer) row.schema().getFieldIndex("loan_balance").get(),
                (Integer) row.schema().getFieldIndex("isPremiumAccount").get()
        };

        Object[] propogatedValues = ArrayUtils.EMPTY_OBJECT_ARRAY;
        Object[] modifiedValues=ArrayUtils.EMPTY_OBJECT_ARRAY;

        for (int i = 0; i<fixedIndex.length; i++) {
            if(fixedIndex[i]==index_custname){
                   propogatedValues=ArrayUtils.add(propogatedValues, full_name);
              }
            else
                propogatedValues=ArrayUtils.add(propogatedValues, row.get(fixedIndex[i]));
        }
        if(row.getString(index_code).equals("A")){
            modifiedValues=ArrayUtils.add(propogatedValues,true);
        }
        else if(((row.getString(index_code).equals("C"))||
                (row.getString(index_code).equals("D")))&&((Integer.parseInt(loan_balance_value)>0))){
            modifiedValues = ArrayUtils.add(propogatedValues, true);
        }
        else
        {
            modifiedValues=ArrayUtils.add(propogatedValues,false);
        }
        Row newrow = new GenericRowWithSchema(modifiedValues, alteredSchema());
        return newrow;
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
}
