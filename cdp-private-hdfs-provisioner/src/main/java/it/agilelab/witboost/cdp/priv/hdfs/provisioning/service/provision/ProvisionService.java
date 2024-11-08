package it.agilelab.witboost.cdp.priv.hdfs.provisioning.service.provision;

import it.agilelab.witboost.cdp.priv.hdfs.provisioning.openapi.model.ProvisioningRequest;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.openapi.model.ProvisioningStatus;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.openapi.model.UpdateAclRequest;
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

    /**
     * Provision the component present in the request
     *
     * @param provisioningRequest the request
     * @return the outcome of the provision
     */
    ProvisioningStatus provision(ProvisioningRequest provisioningRequest);

    /**
     * Unprovision the component present in the request
     *
     * @param provisioningRequest the request
     * @return the outcome of the unprovision
     */
    ProvisioningStatus unprovision(ProvisioningRequest provisioningRequest);

    /**
     * Updates the Access Control List for the component present in the request
     *
     * @param updateAclRequest the request
     * @return the status of updating Access Control Lists
     */
    ProvisioningStatus updateAcl(UpdateAclRequest updateAclRequest);
}
