package com.test.oneid;

import org.apache.spark.sql.api.java.UDF1;

public class my_class implements UDF1<Long, Long> {
    private static final long serialVersionUID = 1L;
    @Override
    public Long call(Long num) throws Exception {
        return (num + 5);
    }
}