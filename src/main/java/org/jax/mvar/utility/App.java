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

    private static Map<String, Object> setArguments(Map<String, Object> arguments, String[] args, boolean isRel, boolean isGeno) {
        arguments.put("REL", isRel);
        arguments.put("GENO", isGeno);
        arguments.put("batch_size", 100000);
        arguments.put("start_id", 1);
        arguments.put("source_name", "Sanger V7");
        if (args.length > 1) {
            for (int i = 1; i <= args.length - 1 ; i ++){
                String[] attribute = args[i].split("=");
                if (attribute[0].equals("batch_size"))
                    arguments.put("batch_size", Integer.valueOf(attribute[1]));
                if (attribute[0].equals("start_id"))
                    arguments.put("start_id", Integer.valueOf(attribute[1]));
                if (attribute[0].equals("source_name"))
                    arguments.put("source_name", attribute[1]);
            }
        }
        return arguments;
    }

    private static Map<String, Object> cmdArgsParser(String[] args) {
        Map<String, Object> arguments = new LinkedHashMap();

        if (args != null) {
            if (args[0].equals("MGI") || args[0].equals("CONVERT")) {
                if (args.length < 2) {
                    System.out.println("Missing data_path argument");
                } else {
                    if (args[0].equals("MGI"))
                        arguments.put("MGI", true);
                    else if (args[0].equals("CONVERT"))
                        arguments.put("CONVERT", true);
                    arguments.put("data_path", args[1]);
                }
            } else {
                arguments.put("chr", "");
                if (args[0].equals("GENO")) {
                    arguments.put("strain_path", args[1].split("=")[1]);
                    setArguments(arguments, args, false, true);
                } else if (args[0].equals("REL")) {
                    setArguments(arguments, args, true, false);
                } else {
                    arguments.put("REL", false);
                    arguments.put("GENO", false);
                    arguments.put("batch_size", 1000);
                    for (int i = 0; i < args.length; i++){
                        String[] attribute = args[i].split("=");
                        if (attribute[0].equals("batch_size")) {
                            arguments.put("batch_size", Integer.valueOf(attribute[1]));
                        } else if (attribute[0].equals("data_path")) {
                            arguments.put("data_path", attribute[1]);
                        } else if (attribute[0].equals("chr")) {
                            arguments.put("chr", attribute[1]);
                        }
                    }
                }
            }
        }
        return arguments;
    }

    public static void main(String[] args) {
        VariantInsertion insertService = new VariantInsertion();
        Map<String, Object> arguments = cmdArgsParser(args);
        try {
            Set<String> keys = arguments.keySet();
            if (keys.contains("MGI")) {         // Check MGI vcf data against the MVAR database for duplicates
                // check MGI variants in DB
                MGIChecker checker = new MGIChecker();
                checker.loadVCF(new File((String) arguments.get("data_path")));
            } else if (keys.contains("CONVERT")) {   // Convert CSV to VCF format
                String filePath = (String) arguments.get("data_path");
                try {
                    // Read variant csv file
                    LinkedHashMap<String, Variant> variants = VCFConverter.parseCSV(filePath, ",");

                    //write loaded variants into vcf file
                    VCFConverter.writeVCF(variants, filePath + ".vcf");
                } catch (Exception e) {
                    System.out.println(e.getMessage());
                }

            } else if (keys.contains("MOUSEMINE")){       // inserts/updates the strains, transcripts, genes, alleles from mousemine into the MVAR DB

            }else {      // insert new variants into DB (relationships, genotype data or variants
                int batchSize = (int)arguments.get("batch_size");
                // if REL (=relationship) then we insert all the variant_transcript relationships from the temp table created
                if ((boolean)arguments.get("REL") && !(boolean)arguments.get("GENO")) {
                    int startId = (int)arguments.get("start_id");
                    String sourceName = (String) arguments.get("source_name");
                    VariantTranscriptInsertion.insertVariantTranscriptSourceRel(batchSize, startId, sourceName);
                } else if ((boolean)arguments.get("GENO") && !(boolean)arguments.get("REL")) {
                    int startId = (int)arguments.get("start_id");
                    String strainFilePath = (String)arguments.get("strain_path");
                    VariantStrainInsertion.insertVariantStrainRelationships(batchSize, startId, strainFilePath);
                } else {
                    String path = (String) arguments.get("data_path");
                    // String strainPath = (String) arguments.get("strains");
                    File f = new File(path);
                    assert f != null;
                    if (f.isDirectory()) {
                        File[] files = new File(f.getPath()).listFiles();
                        assert files != null;
                        Arrays.sort(files);
                        for (File file : files) {
                            if (file.isFile() && (file.getName().endsWith(".gz") || (file.getName().endsWith(".vcf"))))
                                insertService.loadVCF(file, batchSize, false);
                        }

                    } else if (f.isFile() && (f.getName().endsWith(".gz") || (f.getName().endsWith(".vcf")))) {
                        insertService.loadVCF(f, batchSize, false);
                    } else {
                        throw new Exception("Could not find file or directory : " + f.getPath());
                    }
                }
            }

        } catch (Exception exc) {
            System.out.println(exc.getMessage());
        }


    }

}
