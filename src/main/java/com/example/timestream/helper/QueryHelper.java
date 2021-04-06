package com.example.timestream.helper;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.timestreamquery.TimestreamQueryClient;
import software.amazon.awssdk.services.timestreamquery.model.*;
import software.amazon.awssdk.services.timestreamquery.paginators.QueryIterable;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author sa
 * @date 1.04.2021
 * @time 17:43
 */
@Component
@RequiredArgsConstructor
public class QueryHelper
{
    private static final long ONE_GB_IN_BYTES = 1073741824L;

    private final TimestreamQueryClient timestreamQueryClient;

    public void runQuery(String queryString) {
        try {
            QueryRequest queryRequest = QueryRequest.builder().queryString(queryString).build();
            final QueryIterable queryResponseIterator = timestreamQueryClient.queryPaginator(queryRequest);
            for (QueryResponse queryResponse : queryResponseIterator) {
                parseQueryResult(queryResponse);
            }
        } catch (Exception e) {
            // Some queries might fail with 500 if the result of a sequence function has more than 10000 entries
            e.printStackTrace();
        }
    }

    private void parseQueryResult(QueryResponse response) {
        final QueryStatus queryStatus = response.queryStatus();

        System.out.println("Query progress so far: " + queryStatus.progressPercentage() + "%");

        double bytesScannedSoFar = (double) queryStatus.cumulativeBytesScanned() / ONE_GB_IN_BYTES;
        System.out.println("Data scanned so far: " + bytesScannedSoFar + " GB");

        double bytesMeteredSoFar = (double) queryStatus.cumulativeBytesMetered() / ONE_GB_IN_BYTES;
        System.out.println("Data metered so far: " + bytesMeteredSoFar + " GB");

        List<ColumnInfo> columnInfo = response.columnInfo();
        List<Row> rows = response.rows();

        System.out.println("Metadata: " + columnInfo);
        System.out.println("Data: ");

        // iterate every row
        for (Row row : rows) {
            System.out.println(parseRow(columnInfo, row));
        }
    }

    private String parseRow(List<ColumnInfo> columnInfo, Row row) {
        List<Datum> data = row.data();
        List<String> rowOutput = new ArrayList<>();
        // iterate every column per row
        for (int j = 0; j < data.size(); j++) {
            ColumnInfo info = columnInfo.get(j);
            Datum datum = data.get(j);
            rowOutput.add(parseDatum(info, datum));
        }
        return String.format("{%s}", rowOutput.stream().map(Object::toString).collect(Collectors.joining(",")));
    }

    private String parseDatum(ColumnInfo info, Datum datum) {
        if (datum.nullValue() != null && datum.nullValue()) {
            return info.name() + "=" + "NULL";
        }
        Type columnType = info.type();
        // If the column is of TimeSeries Type
        if (columnType.timeSeriesMeasureValueColumnInfo() != null) {
            return parseTimeSeries(info, datum);
        }
        // If the column is of Array Type
        else if (columnType.arrayColumnInfo() != null) {
            List<Datum> arrayValues = datum.arrayValue();
            return info.name() + "=" + parseArray(info.type().arrayColumnInfo(), arrayValues);
        }
        // If the column is of Row Type
        else if (columnType.rowColumnInfo() != null && columnType.rowColumnInfo().size() > 0) {
            List<ColumnInfo> rowColumnInfo = info.type().rowColumnInfo();
            Row rowValues = datum.rowValue();
            return parseRow(rowColumnInfo, rowValues);
        }
        // If the column is of Scalar Type
        else {
            return parseScalarType(info, datum);
        }
    }

    private String parseTimeSeries(ColumnInfo info, Datum datum) {
        List<String> timeSeriesOutput = new ArrayList<>();
        for (TimeSeriesDataPoint dataPoint : datum.timeSeriesValue()) {
            timeSeriesOutput.add("{time=" + dataPoint.time() + ", value=" +
                    parseDatum(info.type().timeSeriesMeasureValueColumnInfo(), dataPoint.value()) + "}");
        }
        return String.format("[%s]", timeSeriesOutput.stream().map(Object::toString).collect(Collectors.joining(",")));
    }

    private String parseScalarType(ColumnInfo info, Datum datum) {
        return parseColumnName(info) + datum.scalarValue();
    }

    private String parseColumnName(ColumnInfo info) {
        return info.name() == null ? "" : info.name() + "=";
    }

    private String parseArray(ColumnInfo arrayColumnInfo, List<Datum> arrayValues) {
        List<String> arrayOutput = new ArrayList<>();
        for (Datum datum : arrayValues) {
            arrayOutput.add(parseDatum(arrayColumnInfo, datum));
        }
        return String.format("[%s]", arrayOutput.stream().map(Object::toString).collect(Collectors.joining(",")));
    }
}
