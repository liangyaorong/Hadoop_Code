import com.sun.xml.internal.ws.policy.privateutil.PolicyUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.jruby.RubyProcess;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;


/**
 * Created by leon on 3/6/17.
 */
public class HBaseAdvanceApi{
    private static Configuration conf =null;
    static{
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "localhost");//要设置好zk的位置，不然在没有添加hbase-site.xml的情况下无法定位
    }


    public static void printRowResult(Result result) throws IOException{
        for (Cell cell : result.rawCells()){
            String rowkey = new String(CellUtil.cloneRow(cell));
            String family = new String(CellUtil.cloneFamily(cell));
            String qual = new String(CellUtil.cloneQualifier(cell));
            String val = new String(CellUtil.cloneValue(cell));
            System.out.println(rowkey + " " + family + " " + qual + " " + val);
        }
    }

    //-------------------高级特性--------------------------------------------------------------------------------------------


    public static void scanWithFilter(String tableName, List<String[]> FamilyAndQualList) throws IOException {
        HTable table = new HTable(conf,tableName);
        Scan scan = new Scan();
        if (FamilyAndQualList != null){  //若想进行全表查询，则把要查询的列族与列名设为null
            for(String[] FamilyAndQual :FamilyAndQualList) {
                scan.addColumn(Bytes.toBytes(FamilyAndQual[0]), Bytes.toBytes(FamilyAndQual[1]));
            }
        }

        /**
         * HBase提供的过滤器非常丰富，除下面所列的之外还有很多过滤器
         * 如KeyOnlyFilter,PageFilter,InclusiveStopFilter等
         * 还有附加过滤器如跳转过滤器、全匹配过滤器和FilterList这样的组合过滤器
         * 更多可查看HBase权威指南第四章4.1
         */

        //行键过滤器
        //对行键进行大小比较筛选
//        Filter filter = new RowFilter(CompareFilter.CompareOp.GREATER,
//                new BinaryComparator(Bytes.toBytes("2014212911")));
//        scan.setFilter(filter);

        //对行键进行正则表达式匹配筛选
        Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL,
                new RegexStringComparator(".*911"));
        scan.setFilter(filter);

        //对行键进行子集匹配筛选
//        Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL,
//                new SubstringComparator("911"));
//        scan.setFilter(filter);

        //列族过滤器(与行键过滤器一样，只是把RowFilter改为FamilyFilter)
//        Filter filter = new FamilyFilter(CompareFilter.CompareOp.GREATER,
//                new BinaryComparator(Bytes.toBytes("info")));
//        scan.setFilter(filter);

        //列名过滤器(与行键过滤器一样，只是把RowFilter改为QualifierFilter)
//        Filter filter = new QualifierFilter(CompareFilter.CompareOp.GREATER,
//                new BinaryComparator(Bytes.toBytes("room")));
//        scan.setFilter(filter);

        //值过滤器(与行键过滤器一样，只是把RowFilter改为ValueFilter)
//        Filter filter = new ValueFilter(CompareFilter.CompareOp.GREATER,
//                new BinaryComparator(Bytes.toBytes("room")));
//        scan.setFilter(filter);

        //单列值过滤器(根据某列的特定值进行筛选，不符合过滤器的不显示；其他列的信息不作处理，直接显示)
//        Filter filter = new SingleColumnValueFilter(
//                Bytes.toBytes("info"),
//                Bytes.toBytes("room"),
//                CompareFilter.CompareOp.EQUAL,
//                new BinaryComparator(Bytes.toBytes("east")));
//        scan.setFilter(filter);

        //前缀过滤器(对行名前缀进行过滤，符合前缀的显示)
//        Filter filter = new PrefixFilter(Bytes.toBytes("2014"));
//        scan.setFilter(filter);

        ResultScanner scanner = table.getScanner(scan);
        for(Result result : scanner){
            printRowResult(result);
        }
        scanner.close();
    }


    /**
     * 计数器
     */

    //单计数器。要获取当前count的值，可将将amount设为0
    // 或在shell中 get_counter 'counters','20170101', 'daily:hit'
    public static void incSingleColumnVal(String tableName, String rowKey, String[] FamilyNQual, long amount) throws IOException{
        HTable table = new HTable(conf, tableName);
        long nowCount = table.incrementColumnValue(
                Bytes.toBytes(rowKey),
                Bytes.toBytes(FamilyNQual[0]),
                Bytes.toBytes(FamilyNQual[1]), amount);
        System.out.print(nowCount);
    }

    //多计数器
    public static void incMutipleColumnVal(String tableName, String rowKey) throws IOException{
        HTable table = new HTable(conf, tableName);
        Increment increment = new Increment(Bytes.toBytes(rowKey));
        increment.addColumn(Bytes.toBytes("daily"),Bytes.toBytes("hit"), 1);
        increment.addColumn(Bytes.toBytes("daily"),Bytes.toBytes("click"), 1);
        increment.addColumn(Bytes.toBytes("weekly"),Bytes.toBytes("hit"), 10);
        Result rs = table.increment(increment);

        for (Cell cell : rs.rawCells()){
            String rowkey = new String(CellUtil.cloneRow(cell));
            String family = new String(CellUtil.cloneFamily(cell));
            String qual = new String(CellUtil.cloneQualifier(cell));
            Long val = Bytes.toLong(CellUtil.cloneValue(cell));
            System.out.println(rowkey + " " + family + " " + qual + " " + val);
        }
    }


    public static void main(String[] args) throws IOException{
//        List FNQList = new ArrayList<>();
//        String[] scanFNQ1 = {"info","add"};
//        String[] scanFNQ2 = {"info","room"};
//        FNQList.add(scanFNQ1);
//        FNQList.add(scanFNQ2);
//        scanWithFilter("Twitter", null);

//        //先在shell中create一个表: create 'counters','daily','weekly'
//        String[] FamNQual = {"daily", "hit"};
//        incSingleColumnVal("counters","20170101", FamNQual, 1);

//        incMutipleColumnVal("counters", "20170101");
    }
}
