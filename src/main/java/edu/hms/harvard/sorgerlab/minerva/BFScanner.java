package edu.hms.harvard.sorgerlab.minerva;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.logging.LogManager;
import java.util.Set;
import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;


import com.amazonaws.services.stepfunctions.model.StartExecutionRequest;
import com.amazonaws.services.stepfunctions.AWSStepFunctionsClientBuilder;
import com.amazonaws.services.stepfunctions.AWSStepFunctions;
import loci.formats.ClassList;
import loci.formats.FormatException;
import loci.formats.IFormatReader;
import loci.formats.ImageReader;
import loci.formats.in.ZipReader;
import org.apache.commons.io.DirectoryWalker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BFScanner extends DirectoryWalker {

    static final Logger logger = LoggerFactory.getLogger(BFScanner.class);

    static final AWSStepFunctions step =
        AWSStepFunctionsClientBuilder.defaultClient();

    final private Set<String> claimedFiles = new HashSet<String>();

    private Path scanDir;

    public BFScanner(Path scanDir) {
        super();
        this.scanDir = scanDir;
    }

    public static void main( String[] args ) {

        // Ensure there is an argument representing the directory to scan
        if (args.length != 1) {
            logger.error("No directory to scanned was specified");
            System.exit(1);
        }

        // Path to the directory to scan (it is assumed that the working
        // directory is the parent of the directory to scan)
        Path scanDir = Paths.get(args[0]).normalize();

        // Ensure the directory to scan is a directory
        if (!Files.isDirectory(scanDir)) {
            logger.error(String.format("Directory was not present to scan %s",
                                        scanDir.toString()));
            System.exit(1);
        }

        // File startDir = new File("/Users/dpwrussell/Downloads/TestData");
        List<String[]> results = new ArrayList();
        BFScanner bfscanner = new BFScanner(scanDir);

        try {
            bfscanner.walk(scanDir.toFile(), results);
        } catch (IOException e) {
            logger.error(String.format("Input/Out Error processing %s", e));
            System.exit(1);
        }

    }

    protected void handleFile(File file, int depth, Collection results) throws IOException {

        String absPath = file.getAbsolutePath();

        // Skip files that have already been claimed as part of another
        // Bio-Formats Unit
        if (claimedFiles.contains(absPath)) {
            return;
        }

        String readersPath = null;

        ClassList<IFormatReader> readers =
            ImageReader.getDefaultReaderClasses();

        readers.removeClass(ZipReader.class);

        ImageReader reader = new ImageReader();

        try {
            reader.setId(absPath);
        } catch (FormatException e) {
            // No reader can read this file
            logger.info(String.format("File not readable by bioformats: %s",
                                      file.getPath().toString()));
            return;
        }

        String[] usedFiles = reader.getUsedFiles();

        // Mark these files as claimed so that they will be skipped when the
        // walker reaches them
        claimedFiles.addAll(Arrays.asList(usedFiles));

        // Add to results
        results.add(usedFiles);

        List<Path> usedPaths = new ArrayList<Path>();
        for (String usedFile : usedFiles) {
            usedPaths.add(
                Paths.get("/Users/dpwrussell/Downloads/TestData")
                    .relativize(Paths.get(usedFile))
            );
        }

        // Build JSON input
        JsonArrayBuilder pathBuilder = Json.createArrayBuilder();
        for (Path path : usedPaths) {
            pathBuilder.add(path.normalize().toString());
        }
        JsonArray paths = pathBuilder.build();
        JsonObject object = Json.createObjectBuilder()
            .add("paths", paths)
            .build();

        // step.startExecution(
        //     new StartExecutionRequest()
        //         .withStateMachineArn()
        //         .withInput()
        //         .withName()
        // );

        System.out.println(object);

    }
}
