package it.agilelab.witboost.cdp.priv.hdfs.provisioning.common;

import java.util.List;
import java.util.Objects;

public record FailedOperation(List<Problem> problems) {
    public FailedOperation {
        Objects.requireNonNull(problems);
    }
}
