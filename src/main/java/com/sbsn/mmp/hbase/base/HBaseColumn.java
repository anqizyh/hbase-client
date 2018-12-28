package com.sbsn.mmp.hbase.base;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface HBaseColumn {

    /**
     * Family name
     * 
     * @return family.
     */
    public String family() default "";

    /**
     * Qualifier name.
     * 
     * @return qualifier.
     */
    public String qualifier();
}
