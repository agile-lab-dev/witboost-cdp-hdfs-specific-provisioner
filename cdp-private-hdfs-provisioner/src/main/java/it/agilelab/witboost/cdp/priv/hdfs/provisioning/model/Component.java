package it.agilelab.witboost.cdp.priv.hdfs.provisioning.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import java.util.Optional;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.EXISTING_PROPERTY,
        property = "kind",
        visible = true)
@JsonSubTypes({
    @JsonSubTypes.Type(value = OutputPort.class, name = "outputport"),
    @JsonSubTypes.Type(value = StorageArea.class, name = "storage")
})
@Valid
public abstract class Component<T> {

    @NotNull
    private String id;

    @NotNull
    private String name;

    private Optional<String> fullyQualifiedName;

    @NotNull
    private String description;

    @NotNull
    private String kind;

    @NotNull
    private @Valid T specific;
}
