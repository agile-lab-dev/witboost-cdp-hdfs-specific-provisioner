package it.agilelab.witboost.cdp.priv.hdfs.provisioning.service.validation;

import static io.vavr.control.Either.left;
import static io.vavr.control.Either.right;

import io.vavr.control.Either;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.common.FailedOperation;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.common.Problem;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.model.Component;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.model.DataProduct;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.model.OutputPort;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.model.Specific;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.parser.Parser;
import java.util.Collections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OutputPortValidation {

    private static final Logger logger = LoggerFactory.getLogger(OutputPortValidation.class);
    private static final String STORAGE_KIND = "storage";

    public static Either<FailedOperation, Void> validate(
            DataProduct dp, Component<? extends Specific> component) {
        if (component instanceof OutputPort<? extends Specific> op) {
            if (op.getDependsOn() == null || op.getDependsOn().size() != 1) {
                String errorMessage =
                        String.format(
                                "Expected exactly a dependency for the component %s, found: %d",
                                component.getId(), op.getDependsOn() == null ? 0 : op.getDependsOn().size());
                logger.error(errorMessage);
                return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage))));
            }
            var dependentComponentId = op.getDependsOn().get(0);
            var optionalDependentComponentAsJson = dp.getComponentToProvision(dependentComponentId);
            if (optionalDependentComponentAsJson.isEmpty())
                return left(
                        new FailedOperation(
                                Collections.singletonList(
                                        new Problem(
                                                String.format(
                                                        "Component with ID %s not found in the Descriptor",
                                                        dependentComponentId)))));
            var dependentComponentAsJson = optionalDependentComponentAsJson.get();
            var eitherDependentComponent =
                    Parser.parseComponent(dependentComponentAsJson, Specific.class);
            if (eitherDependentComponent.isLeft()) return left(eitherDependentComponent.getLeft());
            var dependentComponent = eitherDependentComponent.get();

            if (!STORAGE_KIND.equalsIgnoreCase(dependentComponent.getKind())) {
                String errorMessage =
                        String.format(
                                "Kind of dependent component %s is not right. Expected: %s, found: %s",
                                dependentComponent.getId(), STORAGE_KIND, dependentComponent.getKind());
                logger.error(errorMessage);
                return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage))));
            }
        } else {
            String errorMessage =
                    String.format("The component %s is not of type OutputPort", component.getId());
            logger.error(errorMessage);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage))));
        }
        return right(null);
    }
}
