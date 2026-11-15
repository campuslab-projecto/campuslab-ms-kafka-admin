
package cl.duoc.campuslab.model;

public record CreateTopicRequest(
        String name,
        Integer partitions,
        Short replicationFactor
) {
}
