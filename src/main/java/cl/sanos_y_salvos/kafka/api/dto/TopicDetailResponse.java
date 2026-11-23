package cl.sanos_y_salvos.kafka.api.dto;

import java.util.Map;

public record TopicDetailResponse(
        String name,
        int partitions,
        int replicationFactor,
        Map<String, String> configs
) {}