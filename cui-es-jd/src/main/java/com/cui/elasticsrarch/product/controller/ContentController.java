package com.cui.elasticsrarch.product.controller;

import com.cui.elasticsrarch.product.service.ContentService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.repository.query.Param;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;

@RestController
public class ContentController {

    @Autowired
    private ContentService contentService;

    @GetMapping("/parse/{keyWords}")
    public boolean parse(@PathVariable("keyWords") String keyWords) throws IOException {
        boolean flag = contentService.parseContent(keyWords);
        return flag;
    }
}
