package com.example.timestream.model;

import lombok.*;

import java.time.LocalDateTime;

/**
 * @author sa
 * @date 2.04.2021
 * @time 15:10
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class CategoryRanking
{
    private Long trackId;

    private String countryCode;

    private Integer categoryId;

    private TargetDevice targetDevice;

    private RankingType rankingType;

    private LocalDateTime date;

    private Integer rank;
}
