package it.agilelab.witboost.cdp.priv.hdfs.provisioning.service.validation;

import static io.vavr.control.Either.left;
import static io.vavr.control.Either.right;

import io.vavr.control.Either;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.common.FailedOperation;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.common.Problem;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.model.Component;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.model.ProvisionRequest;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.model.Specific;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.model.StorageSpecific;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.openapi.model.DescriptorKind;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.openapi.model.ProvisioningRequest;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.parser.Parser;
import java.util.Collections;
import java.util.Map;
import org.springframework.stereotype.Service;

@Service
public class ValidationServiceImpl implements ValidationService {

    private final String OUTPUTPORT_KIND = "outputport";
    private final String STORAGE_KIND = "storage";

    private final Map<String, Class<? extends Specific>> kindToSpecificClass =
            Map.of(STORAGE_KIND, StorageSpecific.class, OUTPUTPORT_KIND, Specific.class);

    @Override
    public Either<FailedOperation, ProvisionRequest<? extends Specific>> validate(
            ProvisioningRequest provisioningRequest) {

        if (!DescriptorKind.COMPONENT_DESCRIPTOR.equals(provisioningRequest.getDescriptorKind()))
            return left(
                    new FailedOperation(
                            Collections.singletonList(
                                    new Problem(
                                            String.format(
                                                    "The descriptorKind field is not valid. Expected: '%s', Actual: '%s'",
                                                    DescriptorKind.COMPONENT_DESCRIPTOR,
                                                    provisioningRequest.getDescriptorKind())))));

        var eitherDescriptor = Parser.parseDescriptor(provisioningRequest.getDescriptor());
        if (eitherDescriptor.isLeft()) return left(eitherDescriptor.getLeft());
        var descriptor = eitherDescriptor.get();

        var componentId = descriptor.getComponentIdToProvision();

        var optionalComponentToProvision =
                descriptor.getDataProduct().getComponentToProvision(componentId);
        if (optionalComponentToProvision.isEmpty())
            return left(
                    new FailedOperation(
                            Collections.singletonList(
                                    new Problem(
                                            String.format(
                                                    "Component with ID %s not found in the Descriptor", componentId)))));
        var componentToProvisionAsJson = optionalComponentToProvision.get();

        var optionalComponentKindToProvision =
                descriptor.getDataProduct().getComponentKindToProvision(componentId);
        if (optionalComponentKindToProvision.isEmpty())
            return left(
                    new FailedOperation(
                            Collections.singletonList(
                                    new Problem(
                                            String.format(
                                                    "Component Kind not found for the component with ID %s", componentId)))));
        var componentKindToProvision = optionalComponentKindToProvision.get();

        Component<? extends Specific> componentToProvision;
        switch (componentKindToProvision) {
            case STORAGE_KIND:
                var storageClass = kindToSpecificClass.get(STORAGE_KIND);
                var eitherStorageToProvision =
                        Parser.parseComponent(componentToProvisionAsJson, storageClass);
                if (eitherStorageToProvision.isLeft()) return left(eitherStorageToProvision.getLeft());
                componentToProvision = eitherStorageToProvision.get();
                var storageAreaValidation = StorageAreaValidation.validate(componentToProvision);
                if (storageAreaValidation.isLeft()) return left(storageAreaValidation.getLeft());
                break;
            case OUTPUTPORT_KIND:
                var outputPortClass = kindToSpecificClass.get(OUTPUTPORT_KIND);
                var eitherOutputPortToProvision =
                        Parser.parseComponent(componentToProvisionAsJson, outputPortClass);
                if (eitherOutputPortToProvision.isLeft())
                    return left(eitherOutputPortToProvision.getLeft());
                componentToProvision = eitherOutputPortToProvision.get();
                var outputPortValidation =
                        OutputPortValidation.validate(descriptor.getDataProduct(), componentToProvision);
                if (outputPortValidation.isLeft()) return left(outputPortValidation.getLeft());
                break;
            default:
                return left(
                        new FailedOperation(
                                Collections.singletonList(
                                        new Problem(
                                                String.format(
                                                        "The kind '%s' of the component to provision is not supported by this Specific Provisioner",
                                                        componentKindToProvision)))));
        }
        return right(
                new ProvisionRequest<>(
                        descriptor.getDataProduct(),
                        componentToProvision,
                        provisioningRequest.getRemoveData()));
    }
}
