package org.jax.mvar.utility.parser;

import org.jax.mvar.utility.Config;
import org.jax.mvar.utility.insert.InsertUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class ParserUtilsTest {

    public static final String HEADER = "##fileformat=VCFv4.2\n" +
            "##ALT=<ID=*,Description=\"Represents allele(s) other than observed.\">\n" +
            "##FILTER=<ID=LowConfidence,Description=\"Set if not true: FORMAT/FI[*]=1\">\n" +
            "##FILTER=<ID=OffExome,Description=\"Variant off-exome in all effect predictions\">\n" +
            "##FILTER=<ID=PASS,Description=\"All filters passed\">\n" +
            "##FORMAT=<ID=AD,Number=R,Type=Integer,Description=\"Allelic depths\">\n" +
            "##FORMAT=<ID=ADF,Number=R,Type=Integer,Description=\"Allelic depths on the forward strand\">\n" +
            "##FORMAT=<ID=ADR,Number=R,Type=Integer,Description=\"Allelic depths on the reverse strand\">\n" +
            "##FORMAT=<ID=DP,Number=1,Type=Integer,Description=\"Number of high-quality bases\">\n" +
            "##FORMAT=<ID=FI,Number=1,Type=Integer,Description=\"Whether a sample was a Pass(1) or fail (0) based on FILTER values\">\n" +
            "##FORMAT=<ID=GQ,Number=1,Type=Integer,Description=\"Phred-scaled Genotype Quality\">\n" +
            "##FORMAT=<ID=GT,Number=1,Type=String,Description=\"Genotype\">\n" +
            "##INFO=<ID=ANN,Number=.,Type=String,Description=\"Functional annotations:'Allele|Annotation|Annotation_Impact|Gene_Name|Gene_ID|Feature_Type|Feature_ID|Transcript_BioType|Rank|HGVS.c|HGVS.p|cDNA.pos / cDNA.length|CDS.pos / CDS.length|AA.pos / AA.length|Distance|ERRORS / WARNINGS / INFO'\">\n" +
            "##INFO=<ID=CSQ,Number=.,Type=String,Description=\"Consequence annotations from Ensembl VEP. Format: Allele|Consequence|IMPACT|SYMBOL|Gene|Feature_type|Feature|BIOTYPE|EXON|INTRON|HGVSc|HGVSp|cDNA_position|CDS_position|Protein_position|Amino_acids|Codons|Existing_variation|DISTANCE|STRAND|FLAGS|VARIANT_CLASS|SYMBOL_SOURCE|HGNC_ID|CANONICAL|MANE|TSL|APPRIS|CCDS|ENSP|SWISSPROT|TREMBL|UNIPARC|UNIPROT_ISOFORM|GENE_PHENO|SIFT|DOMAINS|miRNA|HGVS_OFFSET|HGVSg|AF|AFR_AF|AMR_AF|EAS_AF|EUR_AF|SAS_AF|AA_AF|EA_AF|gnomAD_AF|gnomAD_AFR_AF|gnomAD_AMR_AF|gnomAD_ASJ_AF|gnomAD_EAS_AF|gnomAD_FIN_AF|gnomAD_NFE_AF|gnomAD_OTH_AF|gnomAD_SAS_AF|MAX_AF|MAX_AF_POPS|CLIN_SIG|SOMATIC|PHENO|PUBMED|MOTIF_NAME|MOTIF_POS|HIGH_INF_POS|MOTIF_SCORE_CHANGE|TRANSCRIPTION_FACTORS\">\n" +
            "##INFO=<ID=SVANN,Number=.,Type=String,Description=\"Functional SV Annotation:'Annotation|Annotation_Impact|Gene_Name|Gene_ID|Feature_Type|Feature_ID|Transcript_BioType|ERRORS / WARNINGS / INFO'\">";

    private Config config;

    @Before
    public void init() {
        config = new Config();
    }

    /**
     * Test the getStrainIds method
     */
    @Test
    public void testGetStrainIds() {
        try (Connection connection = DriverManager.getConnection(config.getUrl(), config.getUser(), config.getPassword())) {
            Map<Integer, String> strainMap = ParserUtils.getStrainsFromFile(connection, new File("src/test/resources/snpgrid_samples.txt"));
            Assert.assertEquals(580, strainMap.size());
        } catch (Exception exc) {
            exc.printStackTrace();
        }
    }

    /**
     * Test the getHeader method
     */
    @Test
    public void testGetHeader() {
        File vcfFile = new File("src/test/resources/variant_test.vcf");
        String header = ParserUtils.getHeader(vcfFile);
        String[] headerLines = header.split("\n");
        Assert.assertEquals(104, headerLines.length);
    }

    /**
     * Test getAnnotationKeys method for CSQ id
     */
    @Test
    public void testGetAnnotationKeysCSQ() {
        List<String> expectedAnnotations = Arrays.asList("Allele", "Consequence", "IMPACT", "SYMBOL", "Gene", "Feature_type", "Feature", "BIOTYPE", "EXON", "INTRON", "HGVSc", "HGVSp", "cDNA_position", "CDS_position", "Protein_position", "Amino_acids", "Codons", "Existing_variation", "DISTANCE", "STRAND", "FLAGS", "VARIANT_CLASS", "SYMBOL_SOURCE", "HGNC_ID", "CANONICAL", "MANE", "TSL", "APPRIS", "CCDS", "ENSP", "SWISSPROT", "TREMBL", "UNIPARC", "UNIPROT_ISOFORM", "GENE_PHENO", "SIFT", "DOMAINS", "miRNA", "HGVS_OFFSET", "HGVSg", "AF", "AFR_AF", "AMR_AF", "EAS_AF", "EUR_AF", "SAS_AF", "AA_AF", "EA_AF", "gnomAD_AF", "gnomAD_AFR_AF", "gnomAD_AMR_AF", "gnomAD_ASJ_AF", "gnomAD_EAS_AF", "gnomAD_FIN_AF", "gnomAD_NFE_AF", "gnomAD_OTH_AF", "gnomAD_SAS_AF", "MAX_AF", "MAX_AF_POPS", "CLIN_SIG", "SOMATIC", "PHENO", "PUBMED", "MOTIF_NAME", "MOTIF_POS", "HIGH_INF_POS", "MOTIF_SCORE_CHANGE", "TRANSCRIPTION_FACTORS");
        testGetAnnotation("CSQ", expectedAnnotations);
    }

    /**
     * Test getAnnotationKeys method for ANN id
     */
    @Test
    public void testGetAnnotationKeysANN() {
        List<String> expectedAnnotations = Arrays.asList("Allele", "Annotation", "Annotation_Impact", "Gene_Name", "Gene_ID", "Feature_Type", "Feature_ID", "Transcript_BioType", "Rank", "HGVS.c", "HGVS.p", "cDNA.pos / cDNA.length", "CDS.pos / CDS.length", "AA.pos / AA.length", "Distance", "ERRORS / WARNINGS / INFO");
        testGetAnnotation("ANN", expectedAnnotations);
    }

    private void testGetAnnotation(String id, List<String> expectedAnnotations) {
        List<String> annotations = null;
        try {
            annotations = ParserUtils.getAnnotationKeys(id, HEADER);
        } catch (Exception exc) {
            exc.printStackTrace();
        }
        int idx = 0;
        assert annotations != null;
        if (expectedAnnotations.size() != annotations.size())
            throw new IllegalStateException("Failure to read the header file annotations: " +
                    annotations.size() + " keys were found. " + expectedAnnotations.size() + " are expected.");
        for (String annotation : annotations) {
            Assert.assertEquals(expectedAnnotations.get(idx), annotation);
            idx++;
        }
    }
}
