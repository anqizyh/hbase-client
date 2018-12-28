package com.ssbsn.mmp.hbase.client;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sbsn.mmp.hbase.base.HBaseColumn;
import com.sbsn.mmp.hbase.base.HBaseConfFactory;


/**
 * 〈一句话功能简述〉<br>
 * 〈功能详细描述〉
 *
 * @author 
 * @see [相关类/方法]（可选）
 * @since [产品/模块版本] （可选）
 */
public class HbaseClientImpl implements HbaseClient {

    /**
     * 日志
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(HbaseClientImpl.class);

    /**
     * /* (non-Javadoc)
     * 
     * @see com.suning.mpds.hbase.client.HbaseClient#queryForObject(java.lang.String, java.lang.String, java.lang.Class)
     */
    @Override
    public <T> T queryForObject(String tableName, String rowKey, Class<T> requiredType) {

        T resultObject = null;
        HConnection conn = null;
        HTableInterface htable = null;
        try {
            conn = HBaseConfFactory.getInstance();
            htable = conn.getTable(tableName);
            Get get = new Get(Bytes.toBytes(rowKey));
            Result result = htable.get(get);
            resultObject = paresToObject(result, requiredType);
        } catch (InstantiationException | IllegalAccessException | IOException e) {
            LOGGER.error(e.getMessage());
        } finally {  
            if (null != htable) {
                try {
                    htable.close();
                } catch (IOException e) {
                    LOGGER.error(e.getMessage());
                }

            }
        }
        return resultObject;
    }

    private static <T> T paresToObject(Result result, Class<T> cls)
            throws InstantiationException, IllegalAccessException {
        T resultObject = null;
        resultObject = cls.newInstance();
        Field[] fields = cls.getDeclaredFields();
        for (Cell cell : result.rawCells()) {
            // 列名称
            String rowName = new String(CellUtil.cloneQualifier(cell));
            // 值
            String value = new String(CellUtil.cloneValue(cell));
            for (Field field : fields) {
                HBaseColumn hbaseColumn = field.getAnnotation(HBaseColumn.class);
                // String family = hbaseColumn.family();
                if (null != hbaseColumn) {
                    String qualifier = hbaseColumn.qualifier();
                    if (qualifier.equals(rowName)) {
                        // 打开javabean的访问权限
                        field.setAccessible(true);
                        field.set(resultObject, value);
                    }
                }
            }
        }

        return resultObject;
    }

	@Override
	public <T> List<T> queryForList(String tableName, String startRow, String endRow,Filter filter , Class<T> requiredType) {
		T resultObject = null;
        HConnection conn = null;
        HTableInterface htable = null;
        List<T> resultList =null;
        try {
            conn = HBaseConfFactory.getInstance();
            htable = conn.getTable(tableName);
            Scan scan=new Scan();
            scan.setMaxVersions();
            //指定最多返回的Cell数目。用于防止一行中有过多的数据，导致OutofMemory错误。
            scan.setBatch(1000);
            if (null!=startRow)
            	scan.setStartRow(Bytes.toBytes(startRow));
            if(null!=endRow)
            	scan.setStopRow(Bytes.toBytes(endRow));
            if(null!=filter)
            	scan.setFilter(filter);
            ResultScanner results=htable.getScanner(scan);
            resultList=new ArrayList<T>();
            for(Result result:results){
            	 resultObject = paresToObject(result, requiredType);
            	 resultList.add(resultObject);
            }
        } catch (InstantiationException | IllegalAccessException | IOException e) {
            LOGGER.error(e.getMessage());
        } finally {  
            if (null != htable) {
                try {
                    htable.close();
                } catch (IOException e) {
                    LOGGER.error(e.getMessage());
                }

            }
        }
        return resultList;
	}
    
    

}
