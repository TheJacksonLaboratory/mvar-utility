package org.jax.mvar.utility;

import org.jax.mvar.utility.converter.VCFConverter;
import org.jax.mvar.utility.insert.VariantInsertion;
import org.jax.mvar.utility.insert.VariantStrainInsertion;
import org.jax.mvar.utility.insert.VariantTranscriptInsertion;
import org.jax.mvar.utility.model.Variant;
import org.jax.mvar.utility.parser.MGIChecker;

import java.io.*;
import java.util.*;


public class App {

    private static Map<String, Object> cmdArgsParser(String[] args) {
        Map<String, Object> arguments = new LinkedHashMap();

        // check command type (1rst parameter)
        switch (args[0]) {
            case "MGI":
                arguments.put("type", "MGI");
                break;
            case "CONVERT":
                arguments.put("type", "CONVERT");
                break;
            case "GENO":
                arguments.put("type", "GENO");
                break;
            case "REL":
                arguments.put("type", "REL");
                break;
            case "INSERT":
                arguments.put("type", "INSERT");
                break;
            default:
                throw new IllegalStateException("Unexpected command type: " + args[0] + ". " +
                        "Please use INSERT, REL, GENO, MGI or CONVERT as the first parameter.");
        }
        // check and load parameters for given command
        arguments.put("batch_size", 10000);
        arguments.put("start_id", 1);
        arguments.put("stop_id", -1);
        arguments.put("source_name", "Sanger V7");
        arguments.put("check_canon", false);
        arguments.put("data_path", "");
        arguments.put("imputed", (byte)0);
        arguments.put("header_path", "");

        for (int i=0; i < args.length; i++) {
            if (args[i].startsWith("-")) {
                switch (args[i]) {
                    case "-data_path":
                        arguments.put("data_path", args[i+1]);
                        break;
                    case "-batch_size":
                        arguments.put("batch_size", Integer.valueOf(args[i+1]));
                        break;
                    case "-source_name":
                        arguments.put("source_name", args[i+1]);
                        break;
                    case "-strain_path":
                        arguments.put("strain_path", args[i+1]);
                        break;
                    case "-start_id":
                        arguments.put("start_id", Integer.valueOf(args[i+1]));
                        break;
                    case "-stop_id":
                        arguments.put("stop_id", Integer.valueOf(args[i+1]));
                    case "-check_canon":
                        arguments.put("check_canon", true);
                        break;
                    case "-imputed":
                        arguments.put("imputed", Byte.valueOf(args[i+1]));
                        break;
                    case "-header_path":
                        arguments.put("header_path", args[i+1]);
                        break;
                    default:
                        throw new IllegalStateException("Unexpected parameter: " + args[0]);
                }
            }
        }
        return arguments;
    }

    public static void main(String[] args) {
        VariantInsertion insertService = new VariantInsertion();
        Map<String, Object> arguments = cmdArgsParser(args);
        try {
            String type = (String) arguments.get("type");
            int batchSize = (int) arguments.get("batch_size");
            int startId = (int) arguments.get("start_id");
            int stopId = (int) arguments.get("stop_id");
            String path = (String) arguments.get("data_path");
            String headerFilePath = (String) arguments.get("header_path");
            if (type.equals("MGI")) {         // Check MGI vcf data against the MVAR database for duplicates
                // check MGI variants in DB
                MGIChecker checker = new MGIChecker();
                checker.loadVCF(new File(path));
            } else if (type.equals("CONVERT")) {   // Convert CSV to VCF format
                try {
                    // Read variant csv file
                    Map<String, Variant> variants = VCFConverter.parseCSV(path, ",");

                    //write loaded variants into vcf file
                    VCFConverter.writeVCF(variants, path + ".vcf");
                } catch (Exception e) {
                    System.out.println(e.getMessage());
                }
            } else if (type.equals("INSERT")){
                boolean checkForCanon = (boolean) arguments.get("check_canon");
                File headerFile = new File(headerFilePath);
                File f = new File(path);
                assert f != null;
                if (f.isDirectory()) {
                    File[] files = new File(f.getPath()).listFiles();
                    assert files != null;
                    Arrays.sort(files);
                    for (File file : files) {
                        if (file.isFile() && (file.getName().endsWith(".gz") || (file.getName().endsWith(".vcf"))))
                            insertService.loadVCF(file, headerFile, batchSize, false);
                    }

                } else if (f.isFile() && (f.getName().endsWith(".gz") || (f.getName().endsWith(".vcf")))) {
                    insertService.loadVCF(f, f, batchSize, checkForCanon);
                } else {
                    throw new Exception("Could not find file or directory : " + f.getPath());
                }
            } else if (type.equals("REL")){
                String sourceName = (String) arguments.get("source_name");
                VariantTranscriptInsertion.insertVariantTranscriptSourceRel(batchSize, startId, sourceName);
            } else if (type.equals("GENO")){
                String strainFilePath = (String) arguments.get("strain_path");
                byte imputed = (byte) arguments.get("imputed");
                VariantStrainInsertion.insertVariantStrainRelationships(batchSize, startId, stopId, strainFilePath, imputed);
            }
        } catch (Exception exc) {
            System.out.println(exc.getMessage());
        }

    }

}
