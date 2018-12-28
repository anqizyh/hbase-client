package com.sbsn.mmp.hbase.demo;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.sbsn.mmp.hbase.base.HBaseColumn;



/**
 * 〈一句话功能简述〉<br>
 * 〈功能详细描述〉
 *
 * @author 
 * @see [相关类/方法]（可选）
 * @since [产品/模块版本] （可选）
 */
public class HBaseTestDemo {

    public static Configuration configuration;
    private static HBaseAdmin hBaseAdmin;

    static {
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.property.clientPort", "2015");
        configuration.set("hbase.zookeeper.quorum", "namenode1-sit.cnsuning.com,namenode2-sit.cnsuning.com,slave01-sit.cnsuning.com");
        // configuration.set("hbase.master", "192.168.1.100:600000");
     

        // fall back to the system/user-global env variable
      /*  if (home == null) {
          home = System.getenv("HADOOP_HOME");
     
        }*/
        System.setProperty("HADOOP_USER_NAME", "");
        System.setProperty("HADOOP_GROUP_NAME", "");
      
    }

    public static void main(String[] args) {
        // createTable("test2");

        try {
            insertData("ns_sospdm:mpds_member_info");
            try {
            //   QueryByCondition1("ns_sospdm:mpds_member_info","");
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        // QueryByCondition1("test1");
        // QueryByCondition2("test1");
        // QueryByCondition3("test1");
        // deleteRow("test1","abcdef");
        // deleteByCondition("test1","abcdef");
    }

    /**
     * 创建表
     * 
     * @param tableName
     */
    public static void createTable(String tableName) {
        System.out.println("start create table ......");
        try {

            hBaseAdmin = new HBaseAdmin(configuration);

            if (hBaseAdmin.tableExists(tableName)) {// 如果存在要创建的表，那么先删除，再创建
                hBaseAdmin.disableTable(tableName);
                hBaseAdmin.deleteTable(tableName);
                System.out.println(tableName + " is exist,detele....");
            }
            HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
            tableDescriptor.addFamily(new HColumnDescriptor("column1"));
            tableDescriptor.addFamily(new HColumnDescriptor("column2"));
            tableDescriptor.addFamily(new HColumnDescriptor("column3"));
            hBaseAdmin.createTable(tableDescriptor);
        } catch (MasterNotRunningException e) {
            e.printStackTrace();
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {

        }
        System.out.println("end create table ......");
    }

    /**
     * 插入数据
     * 
     * @param tableName
     * @throws IOException
     * @throws ZooKeeperConnectionException
     * @throws MasterNotRunningException
     */
    public static void insertData(String tableName)
            throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
        HBaseAdmin admin = new HBaseAdmin(configuration);
        HTable table = null;
        try {

            if (admin.tableExists(tableName)) {
                String s="";
               
                table = new HTable(configuration, tableName);
                Put put = new Put(Bytes.toBytes(StringUtils.reverse("123456789")));
                put.add(Bytes.toBytes("info"), Bytes.toBytes("CUST_NAME"), Bytes.toBytes("乐乐"));
                put.add(Bytes.toBytes("info"), Bytes.toBytes("CUST_LEVEL"), Bytes.toBytes("v1"));
                put.add(Bytes.toBytes("info"), Bytes.toBytes("SHOP_FREQUENCY"), Bytes.toBytes("111"));
                put.add(Bytes.toBytes("info"), Bytes.toBytes("AGE"), Bytes.toBytes("20"));
                put.add(Bytes.toBytes("info"), Bytes.toBytes("GENDER"), Bytes.toBytes("女"));
                put.add(Bytes.toBytes("info"), Bytes.toBytes("REGISTER_DATE"), Bytes.toBytes("2016-01-08"));
                table.put(put);
                table.flushCommits();// 立即生效写入集群，可以配置该参数

                table.close();
            } else {

            }
            admin.close();
        } catch (MasterNotRunningException e) {
            table.close();
            e.printStackTrace();
        } catch (ZooKeeperConnectionException e) {
            table.close();
            e.printStackTrace();
        } catch (IOException e) {
            table.close();
            e.printStackTrace();
        }
    }

    /**
     * 删除一张表
     * 
     * @param tableName
     */
    public static void dropTable(String tableName) {
        try {
            HBaseAdmin admin = new HBaseAdmin(configuration);
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        } catch (MasterNotRunningException e) {
            e.printStackTrace();
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * 根据 rowkey删除一条记录
     * 
     * @param tablename
     * @param rowkey
     */
    public static void deleteRow(String tablename, String rowkey) {
        try {
            HTable table = new HTable(configuration, tablename);
            List list = new ArrayList();
            Delete d1 = new Delete(rowkey.getBytes());
            list.add(d1);

            table.delete(list);
            System.out.println("删除行成功!");

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * 组合条件删除
     * 
     * @param tablename
     * @param rowkey
     */
    public static void deleteByCondition(String tablename, String rowkey) {
        // 目前还没有发现有效的API能够实现 根据非rowkey的条件删除 这个功能能，还有清空表全部数据的API操作

    }

    /**
     * 查询所有数据
     * 
     * @param tableName
     * @throws IOException
     */
    public static void QueryAll(String tableName) throws IOException {
        HTable table = new HTable(configuration, tableName);
        try {
            ResultScanner rs = table.getScanner(new Scan());
            for (Result r : rs) {
                System.out.println("获得到rowkey:" + new String(r.getRow()));
                for (KeyValue keyValue : r.raw()) {
                    System.out.println("列：" + new String(keyValue.getFamily()) + "====值:"
                            + new String(keyValue.getValue()) + "====时间戳" + keyValue.getTimestamp() + "====版本号"
                            + keyValue.getMvccVersion() + "====序号" + keyValue.getKeyString());
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            table.close();
        }
    }

    /**
     * 单条件查询,根据rowkey查询唯一一条记录
     * 
     * @param tableName
     * @throws Exception 
     */
    public static void QueryByCondition1(String tableName, String rowKey)
            throws Exception {
        HTable table = null;
        HBaseAdmin admin = new HBaseAdmin(configuration);
        try {
            if (!admin.tableExists(tableName)) {
                 throw new Exception("table noexistes");
            }
            table = new HTable(configuration, tableName);
            Get get = new Get(Bytes.toBytes(rowKey));
            Result result = table.get(get);

            for (Cell cell : result.rawCells()) {
             
                System.out.println("RowName:" + new String(CellUtil.cloneRow(cell)) + " ");
                System.out.println("Timetamp:" + cell.getTimestamp() + " ");
                System.out.println("column Family:" + new String(CellUtil.cloneFamily(cell)) + " ");
                System.out.println("row Name:" + new String(CellUtil.cloneQualifier(cell)) + " ");
                System.out.println("value:" + new String(CellUtil.cloneValue(cell)) + " ");
                System.out.println("###########################" );
            }
            MemberInfoDTO dto=paresToObject(result,MemberInfoDTO.class);
            System.out.println("age="+dto.getAge());
            
            admin.close();
        } catch (IOException e) {

        }
    }
    private static <T> T paresToObject(Result result,  Class<T> cls) throws InstantiationException, IllegalAccessException{
        T resultObject = null;  
        resultObject = cls.newInstance(); 
        Field[] fields = cls.getDeclaredFields();
        for (Cell cell : result.rawCells()) {
            //列名称
            String rowName=new String(CellUtil.cloneQualifier(cell));
            //值
            String value=new String(CellUtil.cloneValue(cell));
            for (Field field : fields) {
                HBaseColumn hbaseColumn = field.getAnnotation(HBaseColumn.class);
                String family = hbaseColumn.family();
                String qualifier = hbaseColumn.qualifier();
                if(rowName.equals(qualifier)){
                     //打开javabean的访问权限  
                    field.setAccessible(true); 
                    field.set(resultObject, value);  
                }
            }
        }
        
        return    resultObject;    
    }
    
    /**
     * 单条件按查询，查询多条记录
     * 
     * @param tableName
     */
    public static void QueryByCondition2(String tableName) {

        try {
            HTablePool pool = new HTablePool(configuration, 1000);
            HTable table = (HTable) pool.getTable(tableName);
            Filter filter = new SingleColumnValueFilter(Bytes.toBytes("column1"), null, CompareOp.EQUAL,
                    Bytes.toBytes("aaa")); // 当列column1的值为aaa时进行查询
            Scan s = new Scan();
            s.setFilter(filter);
            ResultScanner rs = table.getScanner(s);
            for (Result r : rs) {
                System.out.println("获得到rowkey:" + new String(r.getRow()));
                for (KeyValue keyValue : r.raw()) {
                    System.out.println(
                            "列：" + new String(keyValue.getFamily()) + "====值:" + new String(keyValue.getValue()));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * 组合条件查询
     * 
     * @param tableName
     */
    public static void QueryByCondition3(String tableName) {

        try {
            HTablePool pool = new HTablePool(configuration, 1000);
            HTable table = (HTable) pool.getTable(tableName);

            List<Filter> filters = new ArrayList<Filter>();

            Filter filter1 = new SingleColumnValueFilter(Bytes.toBytes("column1"), null, CompareOp.EQUAL,
                    Bytes.toBytes("aaa"));
            filters.add(filter1);

            Filter filter2 = new SingleColumnValueFilter(Bytes.toBytes("column2"), null, CompareOp.EQUAL,
                    Bytes.toBytes("bbb"));
            filters.add(filter2);

            Filter filter3 = new SingleColumnValueFilter(Bytes.toBytes("column3"), null, CompareOp.EQUAL,
                    Bytes.toBytes("ccc"));
            filters.add(filter3);

            FilterList filterList1 = new FilterList(filters);

            Scan scan = new Scan();
            scan.setFilter(filterList1);
            ResultScanner rs = table.getScanner(scan);
            for (Result r : rs) {
                System.out.println("获得到rowkey:" + new String(r.getRow()));
                for (KeyValue keyValue : r.raw()) {
                    System.out.println(
                            "列：" + new String(keyValue.getFamily()) + "====值:" + new String(keyValue.getValue()));
                }
            }
            rs.close();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
