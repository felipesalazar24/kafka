package cl.sanos_y_salvos.kafka.api.dto;

import java.util.Set;

public record ConsumerGroupResponse(
        String groupId,
        String state,
        int membersCount,
        Set<String> subscribedTopics
) {}
