package fr.eurecom.dsg.mapreduce.NaiveBayesIMDB;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class NBMain extends Configured implements Tool {

  private URI excludedWordsURI;
  private Path trainPosReviewsFile;
  private Path trainNegReviewsFile;
  private Path testPosReviewsFile;
  private Path testNegReviewsFile;
  private Path outputPath;
  private Path trainOutputPath;
  private boolean verbose;

  @Override
  public int run(String[] args) throws Exception {
    
    Configuration conf = this.getConf();
    FileSystem fs = FileSystem.get(conf);
    this.parseArgs(args, fs);
    fs.close();
    
    System.out.println("Starting the training phase");
    
    NBTrainJob trainJob = new NBTrainJob(new Configuration(conf)
                                       , this.trainPosReviewsFile
                                       , this.trainNegReviewsFile
                                       , this.excludedWordsURI
                                       , this.trainOutputPath);
    
    boolean result = trainJob.waitForCompletion(this.verbose);
    
    if (!result) {
      System.out.println("Train job failed, aborting");
      System.exit(1);
    }
    
    System.out.println("Train phase completed. Starting the testing phase");
    
    fs = FileSystem.get(conf);
    for (FileStatus status : fs.listStatus(trainOutputPath)) {
      if (status.getPath().getName().startsWith("part-")) {
        System.out.println("Caching file " + status.getPath().toUri());
        DistributedCache.addCacheFile(status.getPath().toUri(),conf);
      }
    }
    fs.close();
    
    NBTestJob testJob = new NBTestJob(conf
                                    , this.testPosReviewsFile
                                    , this.testNegReviewsFile
                                    , this.trainOutputPath
                                    , this.outputPath);
    
    return testJob.waitForCompletion(this.verbose) ? 0 : 1;
  }

  public void parseArgs(String[] args, FileSystem fs) throws ParseException,
      NumberFormatException, IOException, URISyntaxException {
    Options options = new Options();
    options.addOption("h", "help", false, "print this help");
    options.addOption("e", "excluded-words", true, "file containing words not"
        + " considered features");
    options.addOption("f", "force", false, "remove output directory if exists");
    options.addOption("v", "verbose", false, "print hadoop output");
    
    CommandLineParser clp = new PosixParser();
    CommandLine cmd = clp.parse(options, args);

    if (cmd.hasOption("h")) {
      HelpFormatter formatter = new HelpFormatter();
      PrintWriter pw = new PrintWriter(new StringWriter());
      String syntax = "NBMain [options] <train_pos_path> <train_neg_path> "
                    + "<test_pos_path> <test_neg_pag> <train_out_dir> " 
                    + "<test_out_dir>";
      String header = "\noptions:";
      formatter.printHelp(syntax, header, options, "", false);
      pw.flush();
      System.exit(0);
    }
    
    if (cmd.hasOption("e")) {
      this.excludedWordsURI = new URI(cmd.getOptionValue("e"));
      System.out.println("Excluded words URI: " + this.excludedWordsURI.toString());
    } else {
      this.excludedWordsURI = null;
    }
    
    this.verbose = cmd.hasOption("v");

    String[] arguments = cmd.getArgs();
    if (arguments.length != 6) {
      System.err.println("Five paths required: two input files (positive and "
          + "negative) for training, two input files (positive and negative) for"
          + " testing, one output path for training and one output path for "
          + "testing");
      new HelpFormatter().printHelp("NB", options);
      System.exit(0);
    }
    
    this.trainPosReviewsFile = new Path(arguments[0]);
    this.trainNegReviewsFile = new Path(arguments[1]);
    this.testPosReviewsFile = new Path(arguments[2]);
    this.testNegReviewsFile = new Path(arguments[3]);
    
    for (Path path : new Path[] { this.trainPosReviewsFile
                                , this.trainNegReviewsFile
                                , this.testPosReviewsFile
                                , this.testNegReviewsFile}) {
      if (!fs.isFile(path)) {
        System.out.println("Input file " + path 
                         + " doesn't exist or is not a file");
        System.exit(0);
      }
    }
    
    this.trainOutputPath = new Path(arguments[4]);
    this.outputPath = new Path(arguments[5]);

    if (cmd.hasOption("f")) {
      for (Path path : new Path[]{ this.trainOutputPath, this.outputPath}) {
        if (fs.exists(path)) {
          System.out.println("Removing existing dir " + path);
          fs.delete(path, true);
        }
      }
    }
  }

  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new NBMain(), args));
  }

}
