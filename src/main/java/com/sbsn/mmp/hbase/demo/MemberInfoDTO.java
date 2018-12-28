package com.sbsn.mmp.hbase.demo;

import com.sbsn.mmp.hbase.base.HBaseColumn;

/**
 * 〈一句话功能简述〉<br> 
 * 〈功能详细描述〉
 *
 * @author 
 * @see [相关类/方法]（可选）
 * @since [产品/模块版本] （可选）
 */
public class MemberInfoDTO {
    
    
    @HBaseColumn(qualifier="AGE")
    private String age;
    
    @HBaseColumn(qualifier="CUST_NAME")
    private String userName;
    
    @HBaseColumn(qualifier="GENDER")
    private String gender;

    public String getAge() {
        return age;
    }

    public void setAge(String age) {
        this.age = age;
    }
    
    
    

}
