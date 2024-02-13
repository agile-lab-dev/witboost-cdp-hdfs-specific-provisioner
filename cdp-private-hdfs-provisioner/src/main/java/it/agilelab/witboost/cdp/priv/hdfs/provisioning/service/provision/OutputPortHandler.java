package it.agilelab.witboost.cdp.priv.hdfs.provisioning.service.provision;

import static io.vavr.control.Either.left;
import static io.vavr.control.Either.right;
import static it.agilelab.witboost.cdp.priv.hdfs.provisioning.service.utils.RangerRoleUtils.generateUserRoleName;
import static it.agilelab.witboost.cdp.priv.hdfs.provisioning.service.utils.RangerRoleUtils.rangerRole;

import com.fasterxml.jackson.databind.JsonNode;
import io.vavr.control.Either;
import io.vavr.control.Option;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.common.FailedOperation;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.common.Problem;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.model.OutputPort;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.model.ProvisionRequest;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.model.Specific;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.service.RangerService;
import java.util.Collections;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class OutputPortHandler extends BaseHandler {

    private final RangerService rangerService;
    private final Logger logger = LoggerFactory.getLogger(OutputPortHandler.class);

    public OutputPortHandler(RangerService rangerService) {
        this.rangerService = rangerService;
    }

    public <T extends Specific> Either<FailedOperation, String> create(
            ProvisionRequest<T> provisionRequest) {
        if (provisionRequest.component() instanceof OutputPort<T> op) {
            if (op.getDependsOn() != null && !op.getDependsOn().isEmpty()) {
                String storageComponentId = op.getDependsOn().get(0);
                return provisionRequest
                        .dataProduct()
                        .getComponentToProvision(storageComponentId)
                        .flatMap(s -> Option.of(s.get("specific")))
                        .flatMap(s -> Option.of(s.get("prefixPath")))
                        .map(JsonNode::textValue)
                        .fold(
                                () -> left(unknownPrefixPath(storageComponentId)),
                                prefixPath -> buildHdfsFolderPath(storageComponentId, prefixPath));
            } else {
                return left(missingDependentStorageArea());
            }
        } else {
            return left(wrongComponentType());
        }
    }

    public <T extends Specific> Either<FailedOperation, Void> destroy(
            ProvisionRequest<T> provisionRequest) {
        if (provisionRequest.component() instanceof OutputPort<T> op) {
            if (op.getDependsOn() != null && !op.getDependsOn().isEmpty()) {
                String storageComponentId = op.getDependsOn().get(0);
                return buildUserRolePrefix(storageComponentId)
                        .flatMap(
                                uRP -> {
                                    String userRoleName = generateUserRoleName(uRP);
                                    return rangerService
                                            .findRoleByName(userRoleName)
                                            .flatMap(
                                                    optR ->
                                                            Option.ofOptional(optR)
                                                                    .fold(
                                                                            () -> right(null),
                                                                            userRole ->
                                                                                    rangerService.updateRole(
                                                                                            rangerRole(
                                                                                                    userRole,
                                                                                                    Collections.emptyList(),
                                                                                                    Collections.emptyList()))))
                                            .map(v -> null);
                                });
            } else {
                return left(missingDependentStorageArea());
            }
        } else {
            return left(wrongComponentType());
        }
    }

    private FailedOperation unknownPrefixPath(String storageComponentId) {
        String errorMessage =
                String.format("prefixPath not found for the component %s", storageComponentId);
        logger.error(errorMessage);
        return new FailedOperation(List.of(new Problem(errorMessage)));
    }

    private FailedOperation missingDependentStorageArea() {
        String errorMessage = "The output port has not a corresponding dependent storage area";
        logger.error(errorMessage);
        return new FailedOperation(Collections.singletonList(new Problem(errorMessage)));
    }

    private FailedOperation wrongComponentType() {
        String errorMessage = "The component type is not of expected type OutputPort";
        logger.error(errorMessage);
        return new FailedOperation(Collections.singletonList(new Problem(errorMessage)));
    }
}
