package it.agilelab.witboost.cdp.priv.hdfs.provisioning.common;

public class SpecificProvisionerValidationException extends RuntimeException {
    private final FailedOperation failedOperation;

    public SpecificProvisionerValidationException(FailedOperation failedOperation) {
        super();
        this.failedOperation = failedOperation;
    }

    public FailedOperation getFailedOperation() {
        return failedOperation;
    }
}
