package it.agilelab.witboost.cdp.priv.hdfs.provisioning.service.provision;

import it.agilelab.witboost.cdp.priv.hdfs.provisioning.openapi.model.ProvisioningRequest;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.openapi.model.ValidationResult;

/***
 * Provision services
 */
public interface ProvisionService {

    /**
     * Validate the provisioning request
     *
     * @param provisioningRequest request to validate
     * @return the outcome of the validation
     */
    ValidationResult validate(ProvisioningRequest provisioningRequest);
}
