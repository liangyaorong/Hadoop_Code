import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;



/**
 * Created by leon on 3/4/17.
 */


public class HBaseBasicOperate {
    private static Configuration conf =null;
    static{
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "localhost");//要设置好zk的位置，不然在没有添加hbase-site.xml的情况下无法定位
    }

    //------------------基础操作----------------------------------------------------------------------------------------------------------

    public static void printRowResult(Result result) throws IOException{
        for (Cell cell : result.rawCells()){
            String rowkey = new String(CellUtil.cloneRow(cell));
            String family = new String(CellUtil.cloneFamily(cell));
            String qual = new String(CellUtil.cloneQualifier(cell));
            String val = new String(CellUtil.cloneValue(cell));
            System.out.println(rowkey + " " + family + " " + qual + " " + val);
        }
    }


    public static void createTable(String tableName, String[] cfs) throws IOException{
        HBaseAdmin admin = new HBaseAdmin(conf);
        if (admin.tableExists(tableName)){
            System.out.println("table has already extist!");
        }
        else {
            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
            for (int i=0; i<cfs.length; i++) {
                tableDescriptor.addFamily(new HColumnDescriptor(cfs[i]));
            }
            admin.createTable(tableDescriptor);
            System.out.printf("create table %s successfully!\n", tableName);
        }
        admin.close();
    }


    public static void deleteTable(String tableName) throws IOException{
        HBaseAdmin admin = new HBaseAdmin(conf);
        admin.disableTable(tableName);
        admin.deleteTable(tableName);
        System.out.printf("Delete table %s successfully!\n", tableName);
        admin.close();
    }


    /**
     * 若关闭了自动写缓冲区(table.setAutoFlush(false, false);)，就要手动强制提交(table.flushCommits();)
     * 不然会有部分提交停留在缓冲区没有写入
     * 若不关闭，每次put都会自动提交
     */
    public static void put(String tableName, List<String[]> putsList) throws IOException{
        HTable table = new HTable(conf, tableName);
//        table.setAutoFlush(false, false);
        List<Put> puts = new ArrayList<>();
        for (String[] put :putsList) {
            Put now_put = new Put(Bytes.toBytes(put[0]));
            now_put.add(
                    Bytes.toBytes(put[1]),
                    Bytes.toBytes(put[2]),
                    Bytes.toBytes(put[3]));
            puts.add(now_put);
        }
        table.put(puts);
//       table.flushCommits();//与上面的table.setAutoFlush(false)配合使用。
        System.out.println("Put successfully!");
    }


    /**
     * 原子性操作(CAS,compare and set)
     * 当check的值与已有的完全一致时，写入put。(用null表示不存在)
     * 当check的rowKey与put的rowKey不一致时，抛出异常。即只能检查并插入相同rowKey的
     * 先比较原值再插入新值。这能保证之前没有其他客户端做过同样的事情。例如账户结余时若金额已经改变，就不能重复扣费
     **/
    public static void checkAndPut(String tableName, String[] toCheckRow, String[] toPutRow) throws IOException{
        HTable table = new HTable(conf, tableName);
        Put put = new Put(Bytes.toBytes(toPutRow[0]));
        put.add(Bytes.toBytes(toPutRow[1]),Bytes.toBytes(toPutRow[2]),Bytes.toBytes(toPutRow[3]));
        if (toCheckRow[3] != null){
            Boolean res = table.checkAndPut(Bytes.toBytes(toCheckRow[0]),
                    Bytes.toBytes(toCheckRow[1]),
                    Bytes.toBytes(toCheckRow[2]),
                    Bytes.toBytes(toCheckRow[3]), put);
            System.out.println("put applied: " + res);
        }
        else{
            Boolean res = table.checkAndPut(Bytes.toBytes(toCheckRow[0]),
                    Bytes.toBytes(toCheckRow[1]),
                    Bytes.toBytes(toCheckRow[2]),
                    null, put);
            System.out.println("put applied: " + res);
        }
    }


    public static void get(String tableName, String[] rowKeys) throws IOException{
        HTable table = new HTable(conf, tableName);
        List<Get> gets = new ArrayList<>();
        for(int i=0; i<rowKeys.length; i++){
            Get get = new Get(Bytes.toBytes(rowKeys[i]));
//            get.addColumn(Bytes.toBytes(family),.....)
            gets.add(get);
        }
        Result[] results = table.get(gets);
        for(Result result : results){
            printRowResult(result);
            System.out.print("\n");
        }
    }


    /**
     * 查找全表某一特定列族与列的行
     */
    public static void scan(String tableName, List<String[]> FamilyAndQualList) throws IOException{
        HTable table = new HTable(conf,tableName);
        Scan scan = new Scan();
        for(String[] FamilyAndQual :FamilyAndQualList) {
            scan.addColumn(Bytes.toBytes(FamilyAndQual[0]), Bytes.toBytes(FamilyAndQual[1]));
        }
        ResultScanner scanner = table.getScanner(scan);
        for(Result result : scanner){
            printRowResult(result);
        }
    }


    public static void main(String[] args) throws IOException{
//        createTable("Twitter", new String[]{"info", "age"});

//        ArrayList<String[]> puts = new ArrayList<>();
//        String[] string1 = {"2014212911","info","add", "502"};
//        puts.add(string1);
//        String[] string2 = {"2014212912","info","room", "north"};
//        puts.add(string2);
//        put("Twitter", puts);

//        String[] toCheckRow = {"2014212910", "info", "room2", "1"};
//        String[] toPutRow = {"2014212910","info","room2","3"};
//        checkAndPut("Twitter", toCheckRow, toPutRow);

//        String[] rowKeys = {"2014212910","2014212911"};
//        get("Twitter",rowKeys);

        List FNQList = new ArrayList<>();
        String[] scanFNQ1 = {"info","add"};
        String[] scanFNQ2 = {"info","room"};
        FNQList.add(scanFNQ1);
        FNQList.add(scanFNQ2);
        scan("Twitter", FNQList);
    }
}