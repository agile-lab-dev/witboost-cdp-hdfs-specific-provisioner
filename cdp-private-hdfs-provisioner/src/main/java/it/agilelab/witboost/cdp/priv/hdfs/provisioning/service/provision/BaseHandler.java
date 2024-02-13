package it.agilelab.witboost.cdp.priv.hdfs.provisioning.service.provision;

import static io.vavr.control.Either.left;
import static io.vavr.control.Either.right;

import io.vavr.control.Either;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.common.FailedOperation;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.common.Problem;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.model.ComponentInfo;
import java.util.List;

public abstract class BaseHandler {

    protected String prefixPathWithTrailingSlash(String prefixPath) {
        return prefixPath.endsWith("/") ? prefixPath : prefixPath.concat("/");
    }

    protected Either<FailedOperation, ComponentInfo> extractIdentifiers(String componentId) {
        var components = componentId.split(":");
        if (components.length != 7) {
            String errorMessage =
                    String.format(
                            "Component id '%s' is not in the expected shape, cannot extract attributes",
                            componentId);
            return left(new FailedOperation(List.of(new Problem(errorMessage))));
        }
        return right(new ComponentInfo(components[3], components[4], components[5], components[6]));
    }

    protected Either<FailedOperation, String> buildHdfsFolderPath(
            String componentId, String prefixPath) {
        var eitherIdentifiers = extractIdentifiers(componentId);
        return eitherIdentifiers.map(
                identifiers ->
                        prefixPathWithTrailingSlash(prefixPath)
                                .concat(
                                        String.format(
                                                "%s/data-products/%s/%s/%s",
                                                identifiers.domain(),
                                                identifiers.dataProductId(),
                                                identifiers.dataProductMajorVersion(),
                                                identifiers.componentId())));
    }

    protected Either<FailedOperation, String> buildUserRolePrefix(String componentId) {
        var eitherIdentifiers = extractIdentifiers(componentId);
        return eitherIdentifiers.map(
                identifiers ->
                        String.format(
                                "%s_%s_%s_%s",
                                identifiers.domain(),
                                identifiers.dataProductId(),
                                identifiers.dataProductMajorVersion(),
                                identifiers.componentId()));
    }
}
