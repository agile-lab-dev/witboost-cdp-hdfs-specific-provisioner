package it.agilelab.witboost.cdp.priv.hdfs.provisioning.service.provision;

import it.agilelab.witboost.cdp.priv.hdfs.provisioning.common.Problem;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.openapi.model.ProvisioningRequest;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.openapi.model.ValidationError;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.openapi.model.ValidationResult;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.service.validation.ValidationService;
import java.util.stream.Collectors;
import org.springframework.stereotype.Service;

@Service
public class ProvisionServiceImpl implements ProvisionService {

    private final ValidationService validationService;

    public ProvisionServiceImpl(ValidationService validationService) {
        this.validationService = validationService;
    }

    @Override
    public ValidationResult validate(ProvisioningRequest provisioningRequest) {
        return validationService
                .validate(provisioningRequest)
                .fold(
                        l ->
                                new ValidationResult(false)
                                        .error(
                                                new ValidationError(
                                                        l.problems().stream()
                                                                .map(Problem::description)
                                                                .collect(Collectors.toList()))),
                        r -> new ValidationResult(true));
    }
}
