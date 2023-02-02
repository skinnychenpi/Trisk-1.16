package org.apache.flink.runtime.checkpoint;

import org.apache.flink.core.execution.SavepointFormatType;

import java.util.Objects;

/** This class is very similar to {@class SavepointType}, representing rescale point. */
public class RescalepointType implements SnapshotType {

    private final String name;
    private final SavepointType.PostCheckpointAction postCheckpointAction;
    private final SavepointFormatType formatType;

    private RescalepointType(
            final String name,
            final SavepointType.PostCheckpointAction postCheckpointAction,
            final SavepointFormatType formatType) {
        this.postCheckpointAction = postCheckpointAction;
        this.name = name;
        this.formatType = formatType;
    }

    // Trace this method for the overall flow of trigger rescale.
    public static RescalepointType rescalepoint(SavepointFormatType formatType) {
        return new RescalepointType(
                "RescalePoint", SavepointType.PostCheckpointAction.NONE, formatType);
    }

    public static RescalepointType terminate(SavepointFormatType formatType) {
        return new RescalepointType(
                "Terminate Savepoint", SavepointType.PostCheckpointAction.TERMINATE, formatType);
    }

    public static RescalepointType suspend(SavepointFormatType formatType) {
        return new RescalepointType(
                "Suspend Savepoint", SavepointType.PostCheckpointAction.SUSPEND, formatType);
    }

    public boolean isSynchronous() {
        return postCheckpointAction != SavepointType.PostCheckpointAction.NONE;
    }

    public String getName() {
        return name;
    }

    public SavepointFormatType getFormatType() {
        return formatType;
    }

    public SharingFilesStrategy getSharingFilesStrategy() {
        return SharingFilesStrategy.NO_SHARING;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RescalepointType that = (RescalepointType) o;
        return name.equals(that.name)
                && postCheckpointAction == that.postCheckpointAction
                && formatType == that.formatType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, postCheckpointAction, formatType);
    }

    @Override
    public String toString() {
        return "RescalepointType{"
                + "name='"
                + name
                + '\''
                + ", postCheckpointAction="
                + postCheckpointAction
                + ", formatType="
                + formatType
                + '}';
    }

    /** What's the intended action after the checkpoint (relevant for stopping with savepoint). */
    public enum PostCheckpointAction {
        NONE,
        SUSPEND,
        TERMINATE
    }

    public SavepointType.PostCheckpointAction getPostCheckpointAction() {
        return postCheckpointAction;
    }

    @Override
    public boolean isSavepoint() {
        return false;
    }

    @Override
    public boolean isRescalepoint() {
        return true;
    }
}
