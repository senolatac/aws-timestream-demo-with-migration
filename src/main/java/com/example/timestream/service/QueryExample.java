package com.example.timestream.service;

import com.example.timestream.helper.QueryHelper;
import com.example.timestream.model.CategoryRanking;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.timestreamwrite.TimestreamWriteClient;
import software.amazon.awssdk.services.timestreamwrite.model.*;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author sa
 * @date 1.04.2021
 * @time 17:42
 */
@Service
@RequiredArgsConstructor
public class QueryExample
{
    private final QueryHelper queryHelper;
    private final TimestreamWriteClient timestreamWriteClient;

    @Value("${aws.timestream.database.name}")
    private String DATABASE_NAME;

    @Value("${aws.timestream.table.name}")
    private String TABLE_NAME;

    @Value("${aws.timestream.dummy.table.name}")
    private String DUMMY_TABLE_NAME;

    private String SELECT_ALL_QUERY = "SELECT * FROM " + DATABASE_NAME + "." + TABLE_NAME +  " limit 100";

    public void findAllItems()
    {
        queryHelper.runQuery(SELECT_ALL_QUERY);
    }

    public void writeDummyRecords()
    {
        writeRecordsToTimeStream();
    }

    private void writeRecordsToTimeStream()
    {
        List<Record> records = Arrays.asList(dummyRecord());

        WriteRecordsRequest writeRecordsRequest = WriteRecordsRequest.builder()
                .databaseName(DATABASE_NAME).tableName(DUMMY_TABLE_NAME).records(records).build();

        try {
            WriteRecordsResponse writeRecordsResponse = timestreamWriteClient.writeRecords(writeRecordsRequest);
            System.out.println("WriteRecords Status: " + writeRecordsResponse.sdkHttpResponse().statusCode());
        } catch (RejectedRecordsException e) {
            printRejectedRecordsException(e);
        } catch (Exception e) {
            System.out.println("Error: " + e);
        }
    }

    private void printRejectedRecordsException(RejectedRecordsException e) {
        System.out.println("RejectedRecords: " + e);
        e.rejectedRecords().forEach(System.out::println);
    }

    private Record dummyRecord()
    {
        List<Dimension> dimensions = Arrays.asList(
                Dimension.builder().name("example_type_1").value("dummy_1").build(),
                Dimension.builder().name("example_type_2").value("dummy_2").build()
        );

        LocalDateTime time = LocalDateTime.now(ZoneOffset.UTC);
        System.out.println(time);

        return Record.builder()
                .dimensions(dimensions)
                .measureValueType(MeasureValueType.BIGINT)
                .measureName("dummy_measure")
                .measureValue(String.valueOf(1L))
                .time(String.valueOf(time.atZone(ZoneOffset.UTC).toInstant().toEpochMilli())).build();
    }
}
