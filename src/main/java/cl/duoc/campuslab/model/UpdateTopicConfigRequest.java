
package cl.duoc.campuslab.model;

public record UpdateTopicConfigRequest(
        Long retentionMs,
        String cleanupPolicy
) {
}
