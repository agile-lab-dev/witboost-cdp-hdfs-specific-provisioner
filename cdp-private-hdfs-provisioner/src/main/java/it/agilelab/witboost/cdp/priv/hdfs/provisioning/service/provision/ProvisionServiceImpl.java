package it.agilelab.witboost.cdp.priv.hdfs.provisioning.service.provision;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.common.FailedOperation;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.common.Problem;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.common.SpecificProvisionerValidationException;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.openapi.model.*;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.service.validation.ValidationService;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;
import org.springframework.stereotype.Service;

@Service
public class ProvisionServiceImpl implements ProvisionService {

    private final ValidationService validationService;
    private final StorageAreaHandler storageAreaHandler;
    private final OutputPortHandler outputPortHandler;

    private final String OUTPUTPORT_KIND = "outputport";
    private final String STORAGE_KIND = "storage";

    public ProvisionServiceImpl(
            ValidationService validationService,
            StorageAreaHandler storageAreaHandler,
            OutputPortHandler outputPortHandler) {
        this.validationService = validationService;
        this.storageAreaHandler = storageAreaHandler;
        this.outputPortHandler = outputPortHandler;
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

    @Override
    public ProvisioningStatus provision(ProvisioningRequest provisioningRequest) {
        var eitherValidation = validationService.validate(provisioningRequest);
        if (eitherValidation.isLeft())
            throw new SpecificProvisionerValidationException(eitherValidation.getLeft());

        var provisionRequest = eitherValidation.get();
        switch (provisionRequest.component().getKind()) {
            case STORAGE_KIND:
                {
                    var eitherCreatedFolderPath = storageAreaHandler.create(provisionRequest);
                    if (eitherCreatedFolderPath.isLeft())
                        throw new SpecificProvisionerValidationException(eitherCreatedFolderPath.getLeft());
                    String createdFolderPath = eitherCreatedFolderPath.get();
                    var privateInfo = Map.of("path", createdFolderPath);
                    return new ProvisioningStatus(ProvisioningStatus.StatusEnum.COMPLETED, "")
                            .info(new Info(JsonNodeFactory.instance.objectNode(), privateInfo));
                }
            case OUTPUTPORT_KIND:
                {
                    var eitherFolderPath = outputPortHandler.create(provisionRequest);
                    if (eitherFolderPath.isLeft())
                        throw new SpecificProvisionerValidationException(eitherFolderPath.getLeft());
                    String folderPath = eitherFolderPath.get();
                    var publicInfo =
                            Map.of("path", Map.of("type", "string", "label", "HDFS Path", "value", folderPath));
                    return new ProvisioningStatus(ProvisioningStatus.StatusEnum.COMPLETED, "")
                            .info(new Info(publicInfo, JsonNodeFactory.instance.objectNode()));
                }
            default:
                throw new SpecificProvisionerValidationException(
                        unsupportedKind(provisionRequest.component().getKind()));
        }
    }

    @Override
    public ProvisioningStatus unprovision(ProvisioningRequest provisioningRequest) {
        var eitherValidation = validationService.validate(provisioningRequest);
        if (eitherValidation.isLeft())
            throw new SpecificProvisionerValidationException(eitherValidation.getLeft());

        var provisionRequest = eitherValidation.get();
        switch (provisionRequest.component().getKind()) {
            case STORAGE_KIND:
                {
                    var eitherDestroy = storageAreaHandler.destroy(provisionRequest);
                    if (eitherDestroy.isLeft())
                        throw new SpecificProvisionerValidationException(eitherDestroy.getLeft());
                    return new ProvisioningStatus(ProvisioningStatus.StatusEnum.COMPLETED, "");
                }
            case OUTPUTPORT_KIND:
                {
                    var eitherDestroy = outputPortHandler.destroy(provisionRequest);
                    if (eitherDestroy.isLeft())
                        throw new SpecificProvisionerValidationException(eitherDestroy.getLeft());
                    return new ProvisioningStatus(ProvisioningStatus.StatusEnum.COMPLETED, "");
                }
            default:
                throw new SpecificProvisionerValidationException(
                        unsupportedKind(provisionRequest.component().getKind()));
        }
    }

    private FailedOperation unsupportedKind(String kind) {
        return new FailedOperation(
                Collections.singletonList(
                        new Problem(
                                String.format(
                                        "The kind '%s' of the component is not supported by this Specific Provisioner",
                                        kind))));
    }
}
