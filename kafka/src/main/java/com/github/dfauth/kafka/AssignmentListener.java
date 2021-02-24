package com.github.dfauth.kafka;

import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.function.Consumer;

public interface AssignmentListener extends Consumer<AssignmentListener.PartitionAssignmentEvent> {

    @Override
    default void accept(PartitionAssignmentEvent event) {
        onAssignment(event);
    }

    void onAssignment(PartitionAssignmentEvent event);

    abstract class PartitionAssignmentEvent {

        private final Collection<TopicPartition> partitions;

        protected PartitionAssignmentEvent(Collection<TopicPartition> partitions) {
            this.partitions = partitions;
        }

        public Collection<TopicPartition> partitions() {
            return partitions;
        }

        public PartitionAssignmentEvent onAssigment(Consumer<Collection<TopicPartition>> consumer) {
            return this;
        }

        public PartitionAssignmentEvent onRevocation(Consumer<Collection<TopicPartition>> consumer) {
            return this;
        }
    }

    class PartitionsAssignedEvent extends PartitionAssignmentEvent {

        protected PartitionsAssignedEvent(Collection<TopicPartition> partitions) {
            super(partitions);
        }

        @Override
        public PartitionsAssignedEvent onAssigment(Consumer<Collection<TopicPartition>> consumer) {
            consumer.accept(partitions());
            return this;
        }
    }

    class PartitionsRevokedEvent extends PartitionAssignmentEvent {

        protected PartitionsRevokedEvent(Collection<TopicPartition> partitions) {
            super(partitions);
        }

        @Override
        public PartitionAssignmentEvent onRevocation(Consumer<Collection<TopicPartition>> consumer) {
            consumer.accept(partitions());
            return this;
        }
    }
}

