package it.agilelab.witboost.cdp.priv.hdfs.provisioning.service.provision;

import static io.vavr.control.Either.left;
import static io.vavr.control.Either.right;
import static it.agilelab.witboost.cdp.priv.hdfs.provisioning.service.utils.RangerRoleUtils.generateUserRoleName;
import static it.agilelab.witboost.cdp.priv.hdfs.provisioning.service.utils.RangerRoleUtils.rangerRole;

import io.vavr.control.Either;
import io.vavr.control.Option;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.common.FailedOperation;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.common.Problem;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.config.RangerConfig;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.model.*;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.openapi.model.*;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.parser.Parser;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.service.PrincipalMappingService;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.service.RangerService;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.service.utils.RangerRoleUtils;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.ranger.plugin.model.RangerRole;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class OutputPortHandler extends BaseHandler {
    private final Logger logger = LoggerFactory.getLogger(OutputPortHandler.class);

    public OutputPortHandler(
            RangerService rangerService, RangerConfig rangerConfig, PrincipalMappingService principalMappingService) {
        super(rangerService, rangerConfig, principalMappingService);
    }

    public <T extends Specific> Either<FailedOperation, String> create(ProvisionRequest<T> provisionRequest) {
        if (provisionRequest.component() instanceof OutputPort<T> op) {
            if (op.getDependsOn() != null && !op.getDependsOn().isEmpty()) {
                String storageComponentId = op.getDependsOn().get(0);
                return provisionRequest
                        .dataProduct()
                        .getComponentToProvision(storageComponentId)
                        .toEither(unknownPath(storageComponentId))
                        .flatMap(s -> Parser.parseComponent(s, StorageSpecific.class)
                                .flatMap(ss -> {
                                    if (ss.getSpecific() != null)
                                        return ss.getSpecific().getPath();
                                    else return left(unknownPath((storageComponentId)));
                                }));
            } else {
                return left(missingDependentStorageArea());
            }
        } else {
            return left(wrongComponentType());
        }
    }

    public <T extends Specific> Either<FailedOperation, Void> destroy(ProvisionRequest<T> provisionRequest) {
        if (provisionRequest.component() instanceof OutputPort<T> op) {
            if (op.getDependsOn() != null && !op.getDependsOn().isEmpty()) {
                String storageComponentId = op.getDependsOn().get(0);
                return buildUserRolePrefix(storageComponentId).flatMap(uRP -> {
                    String userRoleName = generateUserRoleName(uRP);
                    return rangerService
                            .findRoleByName(userRoleName)
                            .flatMap(optR -> Option.ofOptional(optR)
                                    .fold(
                                            () -> right(null),
                                            userRole -> rangerService.updateRole(rangerRole(
                                                    userRole, Collections.emptyList(), Collections.emptyList()))))
                            .map(v -> null);
                });
            } else {
                return left(missingDependentStorageArea());
            }
        } else {
            return left(wrongComponentType());
        }
    }

    public <T extends Specific> Either<FailedOperation, ProvisioningStatus> updateAcl(
            Collection<String> refs, ProvisionRequest<T> provisionRequest) {

        Map<String, Either<FailedOperation, CDPIdentity>> eitherPrincipals =
                principalMappingService.map(Set.copyOf(refs));

        List<CDPIdentity> principals = eitherPrincipals.values().stream()
                .filter(Either::isRight)
                .map(Either::get)
                .toList();

        List<String> users = principals.stream()
                .filter(p -> p instanceof CDPUser)
                .map(p -> (CDPUser) p)
                .map(CDPUser::userId)
                .toList();

        List<String> groups = principals.stream()
                .filter(p -> p instanceof CDPGroup)
                .map(p -> (CDPGroup) p)
                .map(CDPGroup::name)
                .toList();

        ArrayList<Problem> problems = eitherPrincipals.values().stream()
                .filter(Either::isLeft)
                .map(Either::getLeft)
                .flatMap(x -> x.problems().stream())
                .collect(Collectors.toCollection(ArrayList::new));

        logger.info(
                "Updating Access Control Lists for the following users => {} - groups => {}",
                String.join(", ", users),
                String.join(", ", groups));

        // Validation of the descriptor has been run before executing updateAcl, so we are sure that the
        // component is of type 'OutputPort', the dependency exists and is of the right type (i.e. it's a Storage Area
        // Component)
        OutputPort<T> outputPort = (OutputPort<T>) provisionRequest.component();
        String storageComponentId = outputPort.getDependsOn().get(0);

        Either<FailedOperation, RangerRole> userRangerRoleRes = buildUserRolePrefix(storageComponentId)
                .map(RangerRoleUtils::generateUserRoleName)
                .flatMap(userRoleName -> upsertRole(userRoleName, users, groups, rangerConfig.ownerTechnicalUser()));

        if (userRangerRoleRes.isLeft()) {
            problems.addAll(userRangerRoleRes.getLeft().problems());
        }

        if (problems.isEmpty()) {
            logger.info(
                    "Access Control Lists updated successfully: no problems encountered while updating Access Control Lists");
            return right(new ProvisioningStatus(ProvisioningStatus.StatusEnum.COMPLETED, ""));
        } else {
            logger.warn(
                    "Access Control Lists updated: some issues were encountered while updating Access Control Lists");
            problems.forEach(problem -> logger.warn(problem.description()));
            return left(new FailedOperation(problems));
        }
    }

    private FailedOperation unknownPath(String storageComponentId) {
        String errorMessage = String.format("path not found for the component %s", storageComponentId);
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
