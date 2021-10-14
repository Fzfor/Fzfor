package fz.hbase.util;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


/**
 * @author fzfor
 * @date 11:07 2021/08/17
 */
public class Java2HBaseUtil {
    static Configuration conf = null;
    static Connection conn = null;

    static {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104");
        conf.set("hbase.zookeeper.property.client", "2181");

        //实际项目中可能会加的配置
        //conf.set("hbase.zookeeper.quorum", quorum)
        //conf.set("hbase.zookeeper.property.clientPort", port)
        //conf.set("zookeeper.znode.parent", "/hbase")
        //conf.set("hbase.security.authentication", "kerberos")
        //conf.set("hadoop.security.authentication", "kerberos")
        //conf.set("hbase.master.kerberos.principal", "hbase/_HOST@ZJDB.COM")
        //conf.set("hbase.regionserver.kerberos.principal", "hbase/_HOST@ZJDB.COM")
        //conf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
        //conf.set("mapreduce.output.fileoutputformat.outputdir", "/tmp")
        try {
            conn = ConnectionFactory.createConnection(conf);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static void main(String[] args) throws Exception {
//        createTableNoPartition("test", "sdf");
//        String[] arr = new String[]{"000|", "001|", "002|", "003|", "004|", "005|", "006|", "007|", "008|", "009|", "010|"};
//        createTableWithPartition("test",arr, "info");
//        insertMany();
//        deleteData("test","rowKey4");
//        deleteData("test","rowKey2","info2");
//        deleteData("test","rowKey1","info2","name2");
//        updateData("test","row","info","name","xiaoming");
//        getResult("test", "rowKey1");
//        System.out.println();
//        scanTable("test");
//        deleteTable("test");

//        createTableNoPartition("student","info");
//        addFamily("student", "info2");
//        listTables();
//        deleteFamily("student", "info2");
//        listTables();

        System.out.println(getSplitForRadix(3, 10, "00", "11").toString());
    }

    /**
     * 列出所有的表名 列名
     */
    public static void listTables(){
        try {
            Admin admin = conn.getAdmin();
            TableName[] tableNames = admin.listTableNames();
            System.out.println("数据库中的表如下：");
            System.out.println("************************");
            for (TableName tableName : tableNames) {
                HTableDescriptor tableDescriptor = admin.getTableDescriptor(tableName);
                System.out.println("***********");
                System.out.println("***表名："+tableName);
                System.out.println("***列族：");
                for(HColumnDescriptor hcp : tableDescriptor.getFamilies()){
                    System.out.println("  " + hcp.getNameAsString());
                }
                System.out.println("***********");

            }
            System.out.println("************************");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    /**
     * 向目标表添加列族
     * @param tableNameStr
     * @param familyName
     * @throws IOException
     */
    public static void addFamily(String tableNameStr,String familyName) throws IOException {
        Logger logger = LoggerFactory.getLogger(Java2HBaseUtil.class);
        logger.info("获取连接");
        Admin admin = conn.getAdmin();
        TableName tableName=TableName.valueOf(tableNameStr);
        logger.info("disable 表");
        admin.disableTable(tableName);
        HTableDescriptor tableDescriptor = admin.getTableDescriptor(tableName);
        HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(familyName);
        tableDescriptor.addFamily(hColumnDescriptor);
        logger.info("添加列族");
        admin.modifyTable(tableName,tableDescriptor);
        admin.enableTable(tableName);
        admin.close();
    }

    /**
     * 创建一个没有预分区的表
     *
     * @param tableNameStr 表名
     * @param families     列族名 可以传入多个
     * @throws Exception
     */
    public static void createTableNoPartition(String tableNameStr, String... families) throws Exception {
        Admin admin = conn.getAdmin();
        if (!admin.tableExists(TableName.valueOf(tableNameStr))) {
            TableName tableName = TableName.valueOf(tableNameStr);
            //表描述器构造器
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
            for (String family : families) {
                //列族描述器构造器
                HColumnDescriptor info = new HColumnDescriptor(family);
                //添加列族
                hTableDescriptor.addFamily(info);
            }
            //创建表
            admin.createTable(hTableDescriptor);

        } else {
            System.out.println("表已存在");
        }
        //关闭连接
        admin.close();
    }


    /**
     * 创建一个含预分区的表
     * @param tableNameStr 表名
     * @param partitionKeyArr 预分区策略
     * @param families 列族名
     * @throws Exception
     */
    public static void createTableWithPartition(String tableNameStr, String[] partitionKeyArr, String... families) throws Exception {
        //String[] partitionKeyArr = new String[]{"000|", "001|", "002|", "003|", "004|", "005|", "006|", "007|", "008|", "009|", "010|"};
        Admin admin = conn.getAdmin();
        if (!admin.tableExists(TableName.valueOf(tableNameStr))) {
            TableName tableName = TableName.valueOf(tableNameStr);
            //表描述器构造器
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
            for (String family : families) {
                //列族描述器构造器
                HColumnDescriptor info = new HColumnDescriptor(family);
                //添加列族
                hTableDescriptor.addFamily(info);
            }

            //创建byte类型二维数组
            byte[][] splitKeys = new byte[partitionKeyArr.length][];
            TreeSet<byte[]> rows = new TreeSet(Bytes.BYTES_COMPARATOR);
            //升序排序
            for (String key : partitionKeyArr) {
                rows.add(Bytes.toBytes(key));
            }
            Iterator<byte[]> iterator = rows.iterator();
            int index = 0;
            while (iterator.hasNext()) {
                byte[] temRow = iterator.next();
                iterator.remove();
                splitKeys[index] = temRow;
                index ++;
            }

            //创建表
            admin.createTable(hTableDescriptor,splitKeys);

        } else {
            System.out.println("表已存在");
        }
        //关闭连接
        admin.close();
    }

    /**
     * 添加数据（多个rowKey，多个列族）
     * @throws Exception
     */
    public static void insertMany() throws Exception{
        Table table = conn.getTable(TableName.valueOf("test"));
        List<Put> puts = new ArrayList<Put>();
        Put put1 = new Put(Bytes.toBytes("rowKey1"));
        put1.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("wd"));
        put1.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes("123"));
        put1.addColumn(Bytes.toBytes("info"), Bytes.toBytes("sex"), Bytes.toBytes("男"));

        Put put2 = new Put(Bytes.toBytes("rowKey2"));
        put2.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("James"));
        put2.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes("25"));
        put2.addColumn(Bytes.toBytes("info"), Bytes.toBytes("job"), Bytes.toBytes("ball"));

        Put put3 = new Put(Bytes.toBytes("rowKey3"));
        put3.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("kd"));
        put3.addColumn(Bytes.toBytes("info"), Bytes.toBytes("weight"), Bytes.toBytes("60kg"));

        Put put4 = new Put(Bytes.toBytes("rowKey4"));
        put4.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("fz"));
        put4.addColumn(Bytes.toBytes("info"), Bytes.toBytes("school"), Bytes.toBytes("MN"));

        puts.add(put1);
        puts.add(put2);
        puts.add(put3);
        puts.add(put4);
        table.put(puts);
        table.close();
    }

    /**
     * 根据rowKey删除一行数据
     * @param tableNameStr
     * @param rowKey
     * @throws Exception
     */
    public static void deleteData(String tableNameStr, String rowKey) throws Exception{
        Table table = conn.getTable(TableName.valueOf(tableNameStr));
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        //根据rowKey删除一行数据
        table.delete(delete);
        table.close();
    }

    /**
     * 根据rowKey删除某一行的某个列簇
     * @param tableNameStr
     * @param rowKey
     * @throws Exception
     */
    public static void deleteData(String tableNameStr, String rowKey,String columnFamily) throws Exception{
        Table table = conn.getTable(TableName.valueOf(tableNameStr));
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        //删除某一行的某一个列簇内容
        delete.addFamily(Bytes.toBytes(columnFamily));
        table.delete(delete);
        table.close();
    }

    /**
     * 根据rowKey删除某一行某个列簇某列
     * @param tableNameStr
     * @param rowKey
     * @throws Exception
     */
    public static void deleteData(String tableNameStr, String rowKey,String columnFamily, String columnName) throws Exception{
        Table table = conn.getTable(TableName.valueOf(tableNameStr));
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        //删除某一行某个列簇某列的值
        delete.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName));
        table.delete(delete);
        table.close();
    }

    /**
     * 根据RowKey , 列簇， 列名修改值
     * @param tableNameStr
     * @param rowKey
     * @param columnFamily
     * @param columnName
     * @param columnValue
     * @throws Exception
     */
    public static void updateData(String tableNameStr, String rowKey, String columnFamily, String columnName, String columnValue) throws Exception{
        Table table = conn.getTable(TableName.valueOf(tableNameStr));
        Put put1 = new Put(Bytes.toBytes(rowKey));
        put1.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName), Bytes.toBytes(columnValue));
        table.put(put1);
        table.close();
    }

    /**
     * 根据rowKey查询数据
     * @param tableNameStr
     * @param rowKey
     * @throws Exception
     */
    public static void getResult(String tableNameStr, String rowKey) throws Exception{
        Table table = conn.getTable(TableName.valueOf(tableNameStr));
        //获得一行
        Get get = new Get(Bytes.toBytes(rowKey));
        Result set = table.get(get);
        Cell[] cells = set.rawCells();
        for (Cell cell: cells){
            System.out.println(Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()) + "::" +
                    Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
            System.out.println("-------------------");
            System.out.println(Bytes.toString(CellUtil.cloneRow(cell)) + ":" +Bytes.toString(CellUtil.cloneFamily(cell)) + ":" +
                    Bytes.toString(CellUtil.cloneQualifier(cell)) + ":" + Bytes.toString(CellUtil.cloneValue(cell)));

            System.out.println("");
        }
        table.close();
    }

    //过滤器 LESS <  LESS_OR_EQUAL <=   EQUAL =   NOT_EQUAL <>   GREATER_OR_EQUAL >=   GREATER >   NO_OP 排除所有

    /**
     * @param tableNameStr
     * @throws Exception
     */
    public static void scanTable(String tableNameStr) throws Exception{
        Table table = conn.getTable(TableName.valueOf(tableNameStr));

        //①全表扫描
        Scan scan1 = new Scan();
        ResultScanner rscan1 = table.getScanner(scan1);

        //②rowKey过滤器
        Scan scan2 = new Scan();
        //str$ 末尾匹配，相当于sql中的 %str  ^str开头匹配，相当于sql中的str%
        //RowFilter filter1 = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator("Key2$"));
        RowFilter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator("^row"));
        scan2.setFilter(filter);
        //scan2.setFilter(filter1);
        ResultScanner rscan2 = table.getScanner(scan2);

        //③列值过滤器
        Scan scan3 = new Scan();
        //下列参数分别为列族，列名，比较符号，值
        SingleColumnValueFilter filter3 = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("name"),
                CompareFilter.CompareOp.EQUAL, Bytes.toBytes("James"));
        scan3.setFilter(filter3);
        ResultScanner rscan3 = table.getScanner(scan3);

        //列名前缀过滤器
        Scan scan4 = new Scan();
        ColumnPrefixFilter filter4 = new ColumnPrefixFilter(Bytes.toBytes("name"));
        //列名只有前缀也可以过滤得出结果
        //ColumnPrefixFilter filter4 = new ColumnPrefixFilter(Bytes.toBytes("name"));
        scan4.setFilter(filter4);
        ResultScanner rscan4 = table.getScanner(scan4);

        //过滤器集合
        Scan scan5 = new Scan();
        FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        SingleColumnValueFilter filter51 = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("name"),
                CompareFilter.CompareOp.EQUAL, Bytes.toBytes("James"));
        ColumnPrefixFilter filter52 = new ColumnPrefixFilter(Bytes.toBytes("age"));
        list.addFilter(filter51);
        list.addFilter(filter52);
        scan5.setFilter(list);
        ResultScanner rscan5 = table.getScanner(scan5);

        for (Result rs : rscan5){
            //获取rowKey
            String rowKey = Bytes.toString(rs.getRow());
            System.out.println("row key :" + rowKey);
            Cell[] cells = rs.rawCells();
            for (Cell cell: cells){
                //两种获取cell值的方法
                System.out.println(Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()) + "::"
                        + Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()) + "::"
                        + Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));

                System.out.println(Bytes.toString(CellUtil.cloneFamily(cell)) + "::" +
                        Bytes.toString(CellUtil.cloneQualifier(cell)) + "::" +
                        Bytes.toString(CellUtil.cloneValue(cell)));
            }
            System.out.println("-------------------------------------------");
        }
        table.close();
    }

    /**
     * 删除表
     * @param tableNameStr
     * @throws Exception
     */
    public static void deleteTable(String tableNameStr) throws Exception{
        Admin admin = conn.getAdmin();
        TableName tableName = TableName.valueOf(tableNameStr);
        if (admin.tableExists(tableName)) {
            //disable表
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        } else {
            System.out.println(tableNameStr + "表不存在");
        }
        admin.close();
    }

    /**
     * 删除指定表的一个列族
     * @param tableNameStr
     * @param deleteFamily
     */
    public static void deleteFamily(String tableNameStr, String deleteFamily){
        try {
            Admin admin = conn.getAdmin();
            TableName tableName = TableName.valueOf(tableNameStr);
            if(!admin.tableExists(tableName)) throw new Exception("所要删除的表不存在");
            if(admin.isTableEnabled(tableName)){
                admin.disableTable(tableName);
                System.out.println("disable table");
                admin.deleteColumn(tableName, Bytes.toBytes(deleteFamily));
                admin.enableTable(tableName);
                System.out.println("enable table");
            }else {
                System.out.println("delete deleteFamily !");
                admin.deleteColumn(tableName, Bytes.toBytes(deleteFamily));
                admin.enableTable(tableName);
                System.out.println("enable table !");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 预分区工具
     * @param region hbase region server的节点数
     * @param radix 进制 10|16
     * @param start 开始 比如：00
     * @param end 结束 比如：ff
     * @return
     */
    public static List<String> getSplitForRadix(int region, int radix, String start, String end) {
        Integer s = Integer.parseInt(start);
        Integer e = Long.valueOf(end, radix).intValue() + 1;
        return IntStream
                .range(s, e)
                .filter(value -> (value % ((e - s) / region)) == 0)
                .mapToObj(value -> {
                    if (radix == 16) {
                        return Integer.toHexString(value);
                    } else {
                        return String.valueOf(value);
                    }
                })
                .skip(1)
                .collect(Collectors.toList());
    }

    /**
     * 关闭资源
     * @throws Exception
     */
    public static void close() throws Exception{
        if (conn != null) {
            System.out.println("关闭连接");
            conn.close();
        }
    }


}
