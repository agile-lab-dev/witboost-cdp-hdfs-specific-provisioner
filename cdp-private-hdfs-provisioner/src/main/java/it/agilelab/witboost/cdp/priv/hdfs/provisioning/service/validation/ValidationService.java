package it.agilelab.witboost.cdp.priv.hdfs.provisioning.service.validation;

import io.vavr.control.Either;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.common.FailedOperation;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.model.ProvisionRequest;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.model.Specific;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.openapi.model.ProvisioningRequest;

/** Validation services */
public interface ValidationService {
    /**
     * Validate the provision request
     *
     * @param provisioningRequest request to be validated
     * @return a ProvisionRequest object or the error encountered
     */
    Either<FailedOperation, ProvisionRequest<? extends Specific>> validate(ProvisioningRequest provisioningRequest);
}
