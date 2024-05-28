package it.agilelab.witboost.cdp.priv.hdfs.provisioning.service.validation;

import static io.vavr.control.Either.left;
import static io.vavr.control.Either.right;

import io.vavr.control.Either;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.common.FailedOperation;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.common.Problem;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.model.Component;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.model.Specific;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.model.StorageArea;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.model.StorageSpecific;
import jakarta.validation.Valid;
import java.util.Collections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.validation.annotation.Validated;

@Validated
@org.springframework.stereotype.Component
public class StorageAreaValidation {

    private static final Logger logger = LoggerFactory.getLogger(StorageAreaValidation.class);

    public Either<FailedOperation, Void> validate(@Valid Component<? extends Specific> component) {
        logger.info("Checking component with ID {} is of type StorageArea", component.getId());
        if (!(component instanceof StorageArea<? extends Specific>)) {
            String errorMessage = String.format("The component %s is not of type StorageArea", component.getId());
            logger.error(errorMessage);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage))));
        }
        logger.info("Checking specific section of component {} is of type StorageSpecific", component.getId());
        if (component.getSpecific() instanceof StorageSpecific ss) {
            var eitherPath = ss.getPath();
            if (eitherPath.isLeft()) return left(eitherPath.getLeft());
        } else {
            String errorMessage = String.format(
                    "The specific section of the component %s is not of type StorageSpecific", component.getId());
            logger.error(errorMessage);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage))));
        }
        logger.info("Validation of StorageArea {} completed successfully", component.getId());
        return right(null);
    }
}
