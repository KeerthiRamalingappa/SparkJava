package sparkproject.assignment1;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;

public class FilterOperation implements Function<Row, Boolean> {


    /*create test for every class separately
      This part of the code is for filtering out the records which have code="c" value and subvalues is null

     */
    public Boolean call(Row s) throws Exception {

        Integer fieldPosition1=(Integer)s.schema().getFieldIndex("code").get();
        Integer fieldPosition2= (Integer) s.schema().getFieldIndex("subvalues").get();

        if ((s.getString(fieldPosition1).equals("c"))&&(s.getString(fieldPosition2).trim().equals(""))){
            return false;

        }
        else {
            return true;
        }
    }
}
