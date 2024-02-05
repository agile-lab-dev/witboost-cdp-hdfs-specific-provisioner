package it.agilelab.witboost.cdp.priv.hdfs.provisioning.parser;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import io.vavr.control.Either;
import io.vavr.control.Try;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.common.FailedOperation;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.common.Problem;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.model.Component;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.model.Descriptor;
import java.util.Collections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Parser {

    private static final Logger logger = LoggerFactory.getLogger(Parser.class);

    private static final ObjectMapper om = new ObjectMapper(new YAMLFactory());

    static {
        om.registerModule(new Jdk8Module());
    }

    public static Either<FailedOperation, Descriptor> parseDescriptor(String yamlDescriptor) {
        return Try.of(() -> om.readValue(yamlDescriptor, Descriptor.class))
                .toEither()
                .mapLeft(
                        t -> {
                            String errorMessage =
                                    "Failed to deserialize the Yaml Descriptor. Details: " + t.getMessage();
                            logger.error(errorMessage, t);
                            return new FailedOperation(Collections.singletonList(new Problem(errorMessage, t)));
                        });
    }

    public static <U> Either<FailedOperation, Component<U>> parseComponent(
            JsonNode node, Class<U> specificClass) {
        return Try.of(
                        () -> {
                            JavaType javaType =
                                    om.getTypeFactory().constructParametricType(Component.class, specificClass);
                            return om.<Component<U>>readValue(node.toString(), javaType);
                        })
                .toEither()
                .mapLeft(
                        t -> {
                            String errorMessage =
                                    "Failed to deserialize the component. Details: " + t.getMessage();
                            logger.error(errorMessage, t);
                            return new FailedOperation(Collections.singletonList(new Problem(errorMessage, t)));
                        });
    }
}
