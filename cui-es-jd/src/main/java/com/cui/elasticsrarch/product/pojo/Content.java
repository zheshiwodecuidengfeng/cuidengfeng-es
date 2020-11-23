package com.cui.elasticsrarch.product.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author dfcui
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Content {

    /**
     * 商品图片
     */
    private String img;

    /**
     * 商品标题
     */
    private String title;

    /**
     * 商品价格
     */
    private String price;
}
