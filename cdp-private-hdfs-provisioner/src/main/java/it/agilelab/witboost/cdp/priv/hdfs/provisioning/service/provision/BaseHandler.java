package it.agilelab.witboost.cdp.priv.hdfs.provisioning.service.provision;

import static io.vavr.control.Either.left;
import static io.vavr.control.Either.right;
import static it.agilelab.witboost.cdp.priv.hdfs.provisioning.service.utils.RangerRoleUtils.rangerRole;

import io.vavr.control.Either;
import io.vavr.control.Option;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.common.FailedOperation;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.common.Problem;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.config.RangerConfig;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.model.ComponentInfo;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.service.PrincipalMappingService;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.service.RangerService;
import java.util.List;
import org.apache.ranger.plugin.model.RangerRole;

public abstract class BaseHandler {

    protected final RangerService rangerService;
    protected final PrincipalMappingService principalMappingService;
    protected final RangerConfig rangerConfig;

    public BaseHandler(
            RangerService rangerService, RangerConfig rangerConfig, PrincipalMappingService principalMappingService) {
        this.rangerService = rangerService;
        this.rangerConfig = rangerConfig;
        this.principalMappingService = principalMappingService;
    }

    protected String prefixPathWithTrailingSlash(String prefixPath) {
        return prefixPath.endsWith("/") ? prefixPath : prefixPath.concat("/");
    }

    protected Either<FailedOperation, ComponentInfo> extractIdentifiers(String componentId) {
        var components = componentId.split(":");
        if (components.length != 7) {
            String errorMessage = String.format(
                    "Component id '%s' is not in the expected shape, cannot extract attributes", componentId);
            return left(new FailedOperation(List.of(new Problem(errorMessage))));
        }
        return right(new ComponentInfo(components[3], components[4], components[5], components[6]));
    }

    protected Either<FailedOperation, String> buildHdfsFolderPath(String componentId, String prefixPath) {
        var eitherIdentifiers = extractIdentifiers(componentId);
        return eitherIdentifiers.map(identifiers -> prefixPathWithTrailingSlash(prefixPath)
                .concat(String.format(
                        "%s/data-products/%s/%s/%s",
                        identifiers.domain(),
                        identifiers.dataProductId(),
                        identifiers.dataProductMajorVersion(),
                        identifiers.componentId())));
    }

    protected Either<FailedOperation, String> buildUserRolePrefix(String componentId) {
        var eitherIdentifiers = extractIdentifiers(componentId);
        return eitherIdentifiers.map(identifiers -> String.format(
                "%s_%s_%s_%s",
                identifiers.domain(),
                identifiers.dataProductId(),
                identifiers.dataProductMajorVersion(),
                identifiers.componentId()));
    }

    protected Either<FailedOperation, RangerRole> upsertRole(
            String roleName, List<String> users, List<String> groups, String deployUser) {
        return rangerService.findRoleByName(roleName).flatMap(r -> Option.ofOptional(r)
                .fold(
                        () -> rangerService.createRole(rangerRole(roleName, users, groups, deployUser)),
                        rr -> rangerService.updateRole(rangerRole(rr, users, groups))));
    }
}
