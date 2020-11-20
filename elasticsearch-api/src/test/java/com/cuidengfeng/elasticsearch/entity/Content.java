package com.cuidengfeng.elasticsearch.entity;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.Date;

@Data
@AllArgsConstructor
public class Content {

    private String title;

    private Date publishDt;
}
