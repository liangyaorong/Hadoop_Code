import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by leon on 3/7/17.
 */

/**
 * 当多线程时，为每次请求创建HTable实例是不实际的
 * 因此统一使用一个连接，并用getTable方法获取table
 * table的类为HTableInterface。足够轻量化
 */
class ConnectionPoolTest {
    private static Configuration conf = null;
    private static HConnection conn = null;
    static{
        try {
            conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "localhost");
            conn = HConnectionManager.createConnection(conf);
        }
        catch (IOException error) {
            error.printStackTrace();
        }
    }


    public static void get(String tableName, String[] rowKeys) throws IOException{
        HTableInterface table = conn.getTable(tableName);
        List<Get> gets = new ArrayList<>();
        for(int i=0; i<rowKeys.length; i++){
            Get get = new Get(Bytes.toBytes(rowKeys[i]));
//            get.addColumn(Bytes.toBytes(family),.....)
            gets.add(get);
        }
        Result[] results = table.get(gets);
        for(Result result : results){
            for (Cell cell : result.rawCells()){
                String rowkey = new String(CellUtil.cloneRow(cell));
                String family = new String(CellUtil.cloneFamily(cell));
                String qual = new String(CellUtil.cloneQualifier(cell));
                String val = new String(CellUtil.cloneValue(cell));
                System.out.println(rowkey + " " + family + " " + qual + " " + val);
            }
            System.out.print("\n");
        }
        table.close();
    }

    public static void main(String[] args) throws IOException {
        String[] rowKeys = {"2014212910", "2014212911"};
        get("Twitter", rowKeys);

        //......各种操作

        conn.close();
    }
}
