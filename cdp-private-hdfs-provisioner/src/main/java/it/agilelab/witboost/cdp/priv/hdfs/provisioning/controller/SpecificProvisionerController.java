package it.agilelab.witboost.cdp.priv.hdfs.provisioning.controller;

import it.agilelab.witboost.cdp.priv.hdfs.provisioning.openapi.controller.V1ApiDelegate;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.openapi.model.ProvisioningRequest;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.openapi.model.ProvisioningStatus;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.openapi.model.ValidationResult;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.service.provision.ProvisionService;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

/**
 * API Controller for the Java Specific Provisioner which implements the autogenerated {@link
 * V1ApiDelegate} interface. The interface defaults the endpoints to throw 501 Not Implemented
 * unless overridden in this class.
 *
 * <p>Exceptions thrown will be handled by {@link SpecificProvisionerExceptionHandler}
 */
@Service
public class SpecificProvisionerController implements V1ApiDelegate {

    private final ProvisionService provisionService;

    public SpecificProvisionerController(ProvisionService provisionService) {
        this.provisionService = provisionService;
    }

    @Override
    public ResponseEntity<ValidationResult> validate(ProvisioningRequest provisioningRequest)
            throws Exception {
        return ResponseEntity.ok(provisionService.validate(provisioningRequest));
    }

    @Override
    public ResponseEntity<ProvisioningStatus> provision(ProvisioningRequest provisioningRequest)
            throws Exception {
        return ResponseEntity.ok(provisionService.provision(provisioningRequest));
    }

    @Override
    public ResponseEntity<ProvisioningStatus> unprovision(ProvisioningRequest provisioningRequest)
            throws Exception {
        return ResponseEntity.ok(provisionService.unprovision(provisioningRequest));
    }
}
