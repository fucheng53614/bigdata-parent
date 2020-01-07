package net.myvst.v2;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.util.StringUtils;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.parse.FunctionParseNode;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PVarchar;

import java.io.DataInput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 去重复，以逗号连接
 * CREATE FUNCTION DISTINCT_CONCAT(VARCHAR,VARCHAR) returns varchar as 'net.myvst.v2.DistinctConcatFunc' using jar 'hdfs://fuch0:9000/hbase/lib/bigdata-phoenix-1.0-SNAPSHOT.jar';
 * SELECT DISTINCT_CONCAT(c4,'20191021,20191022') FROM test;
 */
@FunctionParseNode.BuiltInFunction(
        name = "DISTINCT_CONCAT",
        args = {@FunctionParseNode.Argument(
                allowedTypes = {PVarchar.class}
        ), @FunctionParseNode.Argument(
                allowedTypes = {PVarchar.class}
        )}
)
public class DistinctConcatFunc extends org.apache.phoenix.expression.function.ScalarFunction {
    public static final String NAME = "DISTINCT_CONCAT";
    private String literalSourceStr = null;
    private String literalSearchStr = null;

    public DistinctConcatFunc(List<Expression> children) {
        super(children);
        this.init();
    }

    private void init() {
        this.literalSourceStr = this.maybeExtractLiteralString((Expression)this.getChildren().get(0));
        this.literalSearchStr = this.maybeExtractLiteralString((Expression)this.getChildren().get(1));
    }

    private String maybeExtractLiteralString(Expression expr) {
        return expr instanceof LiteralExpression ? (String)((LiteralExpression)expr).getValue() : null;
    }


    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        String sourceStr = this.literalSourceStr;
        if (sourceStr == null) {
            Expression child = (Expression)this.getChildren().get(0);
            if (!child.evaluate(tuple, ptr)) {
                return false;
            }

            if (ptr.getLength() == 0) {
                return true;
            }

            sourceStr = (String)PVarchar.INSTANCE.toObject(ptr, child.getSortOrder());
        }

        String searchStr = this.literalSearchStr;
        if (searchStr == null) {
            Expression child = (Expression)this.getChildren().get(1);
            if (!child.evaluate(tuple, ptr)) {
                return false;
            }

            if (ptr.getLength() == 0) {
                return true;
            }

            searchStr = (String)PVarchar.INSTANCE.toObject(ptr, child.getSortOrder());
        }

        String[] sourceValue = sourceStr.split(",");
        List<String> newList = new ArrayList<>();
        for (String s : sourceValue) {
            if (!newList.contains(s) && !"".equals(s)) newList.add(s);
        }

        String[] searchValue = searchStr.split(",");
        for (String s : searchValue) {
            if (!newList.contains(s) && !"".equals(s)) newList.add(s);
        }
        Collections.sort(newList);

        String newValue = StringUtils.join(",", newList);
        ptr.set(PVarchar.INSTANCE.toBytes(newValue));
        return true;
    }

    @Override
    public String getName() {
        return "DISTINCT_CONCAT";
    }

    @Override
    public PDataType getDataType() {
        return PVarchar.INSTANCE;
    }

    public void readFields(DataInput input) throws IOException {
        super.readFields(input);
        this.init();
    }
}
