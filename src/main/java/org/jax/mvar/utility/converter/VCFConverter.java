package org.jax.mvar.utility.converter;

import org.jax.mvar.utility.model.Variant;

import java.io.*;
import java.util.*;

/**
 * See the VCF technical documentation for more information
 * https://samtools.github.io/hts-specs/VCFv4.2.pdf
 */
public class VCFConverter {

    /**
     * Parse a CSV file and return a LinkedHashMap of variations
     * @param csvFilePath CSV file path
     * @param separator separator used in file
     * @throws Exception
     */
    public static LinkedHashMap<String, Variant> parseCSV(String csvFilePath, String separator) throws Exception {
        LinkedHashMap<String, Variant> variations = new LinkedHashMap<>();

        // read file line by line
        try(InputStream is = new FileInputStream(csvFilePath);
            // create new input stream reader
            InputStreamReader instrm = new InputStreamReader(is);
            // Create the object of BufferedReader object
            BufferedReader br = new BufferedReader(instrm);
        ) {
            String next, strLine = br.readLine();
            int idx = 0, notObservedIdx = 0, observedIdx = 0;
            String[] strainList = null;
            // load data in to map
            for (boolean last = (strLine == null); !last; strLine = next) {

                last = ((next = br.readLine()) == null);

                String[] columns = strLine.split(separator);

                // Columns header
                if (idx == 0) {
                    strainList = Arrays.copyOfRange(columns, 6, columns.length - 1);
                } else {
                    String[] alleles = columns[3].split("/");
                    String[] genotypeData = Arrays.copyOfRange(columns, 6, columns.length - 1);
                    if (alleles.length >= 2) {
                        String genotypes = getGTFields(genotypeData, alleles);
                        Variant var;
                        // TODO retrieve the hgvs notation
                        if (alleles.length == 2)
                            var = new Variant(columns[0], columns[1], columns[2], alleles[0], alleles[1], "", "", "", "GT", genotypes, "", strainList);
                        else
                            var = new Variant(columns[0], columns[1], columns[2], alleles[0], alleles[1].concat(alleles[2]), "", "", "", "GT", genotypes, "", strainList);

                        if (variations.containsKey(var.getVariantRefTxt()))
                            System.out.println(var.getVariantRefTxt() + " already exists and will be overridden.");
                        variations.put(var.getVariantRefTxt(), var);
                        observedIdx++;
                    } else {
                        notObservedIdx++;
                        // System.out.println("variation at line " + idx + " with id " + columns[2] + " does not have observed alleles.");
                    }
                }
                System.out.print(".");
                idx++;
             }
            System.out.println(idx + " total variants parsed");
            System.out.println(notObservedIdx + " variants with non observed alleles");
            System.out.println(observedIdx + " variants with observed alleles");
        }
        return variations;
    }

    private static String getGTFields(String[] genotypeData, String[] alleles) {
        String genotypes = "";
        List<String> alleleList = Arrays.asList(alleles);
        // iterate through each genotype of each sample
        for (int i = 0; i < genotypeData.length; i ++) {
            // iterate through each possible allele, the first one being the ref allele
            if (alleleList.contains(genotypeData[i])) {
                int idx = alleleList.indexOf(genotypeData[i]);
                genotypes = genotypes.concat("0/".concat(String.valueOf(idx)));
            } else {  // if NA of empty
                genotypes = genotypes.concat("./.");
            }
            if (i < genotypeData.length - 1) {
                genotypes = genotypes.concat(","); // we put a comma as a separator
            }
        }
        return genotypes;
    }

    /**
     * Writes a LinkedHashMap of variants to a VCF file.
     * The result file will be written at the same path location as the csv file given
     * and its file extension should be ".vcf"
     * @param variants
     * @param vcfFilePath
     * @throws Exception
     */
    public static void writeVCF(LinkedHashMap<String, Variant> variants, String vcfFilePath) throws Exception {

        if (!vcfFilePath.endsWith(".vcf"))
            throw new Exception("The file name should have the '.vcf' extension.");

        File file = new File(vcfFilePath);
        FileWriter fr = null;
        BufferedWriter br = null;
        String linefeed = System.getProperty("line.separator");
        String sep = "\t";
        try {
            fr = new FileWriter(file);
            br = new BufferedWriter(fr);
            br.write("##fileformat=VCFv4.2" + linefeed);
            // We concatenate the strain names to build the columns headers
            String firstItemKey = variants.keySet().iterator().next();
            Variant firstVar = variants.get(firstItemKey);
            String[] strainList = firstVar.getStrainList();
            String header = "#CHROM" + sep + "POS" + sep + "ID" + sep + "REF" + sep + "ALT" + sep + "QUAL"
                    + sep + "FILTER" + sep + "INFO" + sep + "FORMAT" + sep;
            for (int i = 0; i < strainList.length; i ++) {
                if (i < strainList.length - 1)
                    header = header.concat(strainList[i] + sep);
                else
                    header = header.concat(strainList[i] + linefeed);
            }
            // write the header
            br.write(header);
            // write data
            for(Map.Entry<String, Variant> variantEntry : variants.entrySet()) {
                Variant variant = variantEntry.getValue();
                String line = variant.getChr() + sep + variant.getPos() + sep + variant.getId() + sep
                        + variant.getRef() + sep + variant.getAlt() + sep + variant.getQual() + sep
                        + variant.getFilter() + sep + variant.getInfo() + sep + variant.getFormat() + sep;
                String genotypeStr = variant.getGenotypeData();
                String[] genotypeData = genotypeStr.split(",");
                for (int i = 0; i < genotypeData.length; i++) {
                    if (i < strainList.length - 1)
                        line = line.concat(genotypeData[i] + sep);
                    else
                        line = line.concat(genotypeData[i] + linefeed);
                }
                // write line
                br.write(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.err.println("Error writing variants to file: " + e.getMessage());
        } finally {
            try {
                if (br != null)
                    br.close();
                if (fr != null)
                    fr.close();
                System.out.println("New vcf file created:" + file.getName());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
