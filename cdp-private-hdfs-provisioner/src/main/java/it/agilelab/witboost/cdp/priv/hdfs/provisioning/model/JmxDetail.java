package it.agilelab.witboost.cdp.priv.hdfs.provisioning.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@JsonIgnoreProperties(ignoreUnknown = true)
public class JmxDetail {
    @JsonProperty("State")
    private String state;
}
