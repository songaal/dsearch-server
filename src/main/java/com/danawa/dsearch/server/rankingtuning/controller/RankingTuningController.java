package com.danawa.dsearch.server.rankingtuning.controller;

import com.danawa.dsearch.server.excpetions.ElasticQueryException;
import com.danawa.dsearch.server.rankingtuning.service.RankingTuningService;
import com.danawa.dsearch.server.rankingtuning.entity.RankingTuningRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.*;


@RestController
@RequestMapping("/rankingtuning")
public class RankingTuningController {
    private static Logger logger = LoggerFactory.getLogger(RankingTuningController.class);
    private final RankingTuningService rankingTuningService;
    public RankingTuningController(RankingTuningService rankingTuningService) {
        this.rankingTuningService = rankingTuningService;
    }

    @PostMapping("/")
    public ResponseEntity<?> getRankingTuning(@RequestHeader(value = "cluster-id") UUID clusterId,
                                              @RequestBody RankingTuningRequest rankingTuningRequest) throws ElasticQueryException, IOException {
        return new ResponseEntity<>(rankingTuningService.getAnalyze(clusterId, rankingTuningRequest), HttpStatus.OK);
    }
}
