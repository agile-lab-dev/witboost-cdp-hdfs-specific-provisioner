package it.agilelab.witboost.cdp.priv.hdfs.provisioning.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@JsonIgnoreProperties(ignoreUnknown = true)
public class JmxResponse {
    private List<JmxDetail> beans;
}
