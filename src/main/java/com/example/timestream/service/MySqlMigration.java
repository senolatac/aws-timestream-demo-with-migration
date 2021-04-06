package com.example.timestream.service;

import com.example.timestream.model.CategoryRanking;
import com.example.timestream.model.RankingType;
import com.example.timestream.model.TargetDevice;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.timestreamwrite.TimestreamWriteClient;
import software.amazon.awssdk.services.timestreamwrite.model.*;

import java.sql.*;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author sa
 * @date 2.04.2021
 * @time 14:27
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class MySqlMigration
{
    private final TimestreamWriteClient timestreamWriteClient;

    @Value("${migration.from.jdbc-driver}")
    private String JDBCDriver;

    @Value("${migration.from.database-url}")
    private String DATABASE_URL;

    @Value("${migration.from.database.user}")
    private String DATABASE_USER;

    @Value("${migration.from.database.pass}")
    private String DATABASE_PASS;

    @Value("${aws.timestream.database.name}")
    private String DATABASE_NAME;

    @Value("${aws.timestream.table.name}")
    private String TABLE_NAME;

    public void migrateCategoriesFromMysqlToTimestream()
    {
        List<Long> trackIds = findUniqueTrackIds();
        for (Long trackId : trackIds)
        {
            try
            {
                List<CategoryRanking> rankings = findCategories(trackId);
                log.info("trackId:{} size:{}",trackId, rankings.size());
                if (!rankings.isEmpty())
                {
                    for (int i = 0; i< rankings.size(); i = i + 100)
                    {
                        writeRecordsToTimeStream(rankings.subList(i, Math.min(rankings.size(), i + 100)));
                        //log.info("completed slice start:{} total:{}", i, rankings.size());
                    }
                }
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
        }
    }

    private Connection con()
    {
        try
        {
            Class.forName(JDBCDriver);
            Connection con = DriverManager.getConnection(DATABASE_URL, DATABASE_USER, DATABASE_PASS);
            return con;
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        return null;
    }

    private List<Long> findUniqueTrackIds()
    {
        Connection con = con();
        List<Long> trackIds = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        sb.append("Select distinct track_id from category_ranking limit 10000 offset 1000");
        try
        {
            Statement stmt = con.createStatement();
            ResultSet rs = stmt.executeQuery(sb.toString());
            while (rs.next())
            {
                trackIds.add(rs.getLong("track_id"));
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        try
        {
            con.close();
        }
        catch (SQLException e)
        {
            e.printStackTrace();
        }
        return trackIds;
    }

    private List<CategoryRanking> findCategories(Long trackId)
    {
        Connection con = con();
        List<CategoryRanking> categoryRankings = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        sb.append("Select ")
                .append("id, track_id, country_code, category_id, ranking_type, target_device, date, rank ")
                .append("from category_ranking where ")
                .append("track_id =").append(trackId)
                .append(" and date > '2020-04-06'")
                .append(" limit 200000");
        try
        {
            Statement stmt = con.createStatement();
            ResultSet rs = stmt.executeQuery(sb.toString());
            while (rs.next())
            {
                categoryRankings.add(CategoryRanking.builder()
                        .trackId(rs.getLong("track_id"))
                        .categoryId(rs.getInt("category_id"))
                        .countryCode(rs.getString("country_code"))
                        .date(rs.getObject("date", LocalDateTime.class))
                        .rank(rs.getInt("rank"))
                        .rankingType(RankingType._values[rs.getInt("ranking_type")])
                        .targetDevice(TargetDevice._values[rs.getInt("target_device")])
                        .build());
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        try
        {
            con.close();
        }
        catch (SQLException e)
        {
            e.printStackTrace();
        }
        return categoryRankings;
    }

    private void writeRecordsToTimeStream(List<CategoryRanking> rankingList)
    {
        List<Record> records = rankingList.stream().map(this::convertToRecord).collect(Collectors.toList());

        WriteRecordsRequest writeRecordsRequest = WriteRecordsRequest.builder()
                .databaseName(DATABASE_NAME).tableName(TABLE_NAME).records(records).build();

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

    private Record convertToRecord(CategoryRanking ranking)
    {
        List<Dimension> dimensions = Arrays.asList(
            Dimension.builder().name("track_id").value(String.valueOf(ranking.getTrackId())).build(),
            Dimension.builder().name("category_id").value(String.valueOf(ranking.getCategoryId())).build(),
            Dimension.builder().name("country_code").value(ranking.getCountryCode()).build(),
            Dimension.builder().name("ranking_type").value(ranking.getRankingType().name()).build(),
            Dimension.builder().name("target_device").value(ranking.getTargetDevice().name()).build()
        );

        //LocalDateTime time = Collections.max(Arrays.asList(LocalDateTime.of(2021, 1, 1, 0 ,0), ranking.getDate()));
        LocalDateTime time = ranking.getDate();

        //System.out.println(time);

        return Record.builder()
                .dimensions(dimensions)
                .measureValueType(MeasureValueType.BIGINT)
                .measureName("rank")
                .measureValue(String.valueOf(ranking.getRank()))
                .time(String.valueOf(time.atZone(ZoneOffset.UTC).toInstant().toEpochMilli())).build();
    }
}
