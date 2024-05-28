package it.agilelab.witboost.cdp.priv.hdfs.provisioning.model;

import static io.vavr.control.Either.left;
import static io.vavr.control.Either.right;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.vavr.control.Either;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.common.FailedOperation;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.common.Problem;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.util.List;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@JsonIgnoreProperties(ignoreUnknown = true)
@Valid
public class StorageSpecific extends Specific {

    @NotBlank
    @Pattern(regexp = "^/.*", message = "Root folder must start with '/'")
    private String rootFolder;

    @NotBlank
    private String folder;

    /**
     * Creates the storage path based on the {@code rootFolder} and the {@code folder} class attributes concatenating as {@code rootFolder/folder }
     * @return {@code Either.right} if built path is valid, or {@code Either.left} with an error
     */
    @JsonIgnore
    public Either<FailedOperation, String> getPath() {
        try {
            return right(Path.of(rootFolder, folder).toString().replace('\\', '/'));
        } catch (InvalidPathException | NullPointerException e) {
            String errorMessage = String.format(
                    "Failed to build path from specific storage. Root folder '%s' or folder '%s' are invalid path strings",
                    rootFolder, folder);
            return left(new FailedOperation(List.of(new Problem(errorMessage))));
        }
    }
}
