package com.chris.web.bean;

import java.util.List;
import java.util.Map;

public class PageHelper {
    // 注意：这两个属性名称不能改变，是定死的
    // 实体类集合
    private List<com.chris.web.bean.WZ> rows ;
    // 数据总条数
    private Long total;


    public PageHelper() {
    }

    @Override
    public String toString() {
        return "PageHelper{" +
                "rows=" + rows +
                ", total=" + total +
                '}';
    }


    public List<com.chris.web.bean.WZ> getRows() {
        return rows;
    }

    public void setRows(List<WZ> rows) {
        this.rows = rows;
    }

    public Long getTotal() {
        return total;
    }

    public void setTotal(Long total) {
        this.total = total;
    }

    public PageHelper(List<WZ> rows, Long total) {
        this.rows = rows;
        this.total = total;
    }
}
