package it.agilelab.witboost.cdp.priv.hdfs.provisioning.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
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
public abstract class Component<T> {

    private String id;
    private String name;
    private Optional<String> fullyQualifiedName;
    private String description;
    private String kind;
    private T specific;
}
