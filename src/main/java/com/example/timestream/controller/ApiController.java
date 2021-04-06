package com.example.timestream.controller;

import com.example.timestream.service.MySqlMigration;
import com.example.timestream.service.QueryExample;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author sa
 * @date 1.04.2021
 * @time 17:50
 */
@RestController
@RequestMapping("api")
@RequiredArgsConstructor
public class ApiController
{
    private final QueryExample queryExample;
    private final MySqlMigration mySqlMigration;

    @GetMapping("items")
    public ResponseEntity<?> getAllItems()
    {
        queryExample.findAllItems();
        return ResponseEntity.ok(true);
    }

    @PostMapping("dummy")
    public ResponseEntity<?> writeDummyRecords()
    {
        queryExample.writeDummyRecords();
        return ResponseEntity.ok(true);
    }

    @GetMapping("categories")
    public ResponseEntity<?> getAllCategories()
    {
        mySqlMigration.migrateCategoriesFromMysqlToTimestream();
        return ResponseEntity.ok(true);
    }
}
