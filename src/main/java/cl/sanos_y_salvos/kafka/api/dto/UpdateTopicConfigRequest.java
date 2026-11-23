package cl.sanos_y_salvos.kafka.api.dto;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.Pattern;

public record UpdateTopicConfigRequest(
        @Min(value = 1, message = "El número de particiones debe ser al menos 1")
        Integer newPartitions,

        @Min(value = 1000, message = "La retención mínima es 1000 ms")
        Long retentionMs,

        @Pattern(regexp = "^(delete|compact|delete,compact)$", message = "cleanupPolicy debe ser 'delete', 'compact' o 'delete,compact'")
        String cleanupPolicy
) {}