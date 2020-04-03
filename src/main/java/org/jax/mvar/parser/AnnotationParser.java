package org.jax.mvar.parser;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Class used to parse the INFO column of a VCF variant row entry with the ANN id
 * See http://snpeff.sourceforge.net/VCFannotationformat_v1.0.pdf for more infos on the full ANN standard
 *
 *     ##INFO=<ID=ANN,Number=.,Type=String,
 *     Description="Functional annotations:
 *     1        |2                                  |3                  |4              |4      |5              |6                      |7                  |8      |9              |10
 *     Allele   |Annotation                         |Annotation_Impact  |Gene_Name      |Gene_ID|Feature_Type   |Feature_ID             |Transcript_BioType |Rank   |HGVS.c         |HGVS.p |cDNA.pos / cDNA.length |CDS.pos / CDS.length   |AA.pos / AA.length |Distance   |ERRORS / WARNINGS / INFO'">
 *     G        |intergenic_variant                 |MODIFIER           |4933401J01Rik  |.      |transcript     |ENSMUST00000193812.1   |Noncoding          |       |               |       |                       |                       |                   |72811      |;CSQ=G                     ||||intergenic_variant||||||||;DP=32;DP4=0,0,19,13
 *     C        |intergenic_variant                 |MODIFIER           |Mid1-ps1       |.      |transcript     |ENSMUST00000167967.3   |Noncoding          |       |               |       |                       |                       |                   |5523       |;CSQ=C                     ||||intergenic_variant||||||||;DP=275;DP4=107,44,67,57
 *     C        |downstream_gene_variant            |MODIFIER           |Mid1-ps1       |.      |transcript     |ENSMUST00000167967.3   |Noncoding          |       |               |       |                       |                       |                   |4279       |;CSQ=C                     |ENSMUSG00000095134|ENSMUST00000167967|Transcript|downstream_gene_variant|||||||4280|1;DP=229;DP4=44,89,88,8
 *     C        |coding_transcript_intron_variant   |LOW                |Xkr4           |497097 |transcript     |ENSMUST00000070533.4   |Coding             |2/2    |c.998-17145A>G |p.(%3D)|1148/457017            |998/1944               |333/648            |           |;CSQ=C                     |ENSMUSG00000051951|ENSMUST00000070533|Transcript|intron_variant||||||||-1;DP=66;DP4=18,22,7,19
 *     T        |5_prime_UTR_exon_variant           |LOW                |Rrs1           |59014  |transcript     |ENSMUST00000072079.8   |Coding             |1/1    |c.-8C>T        |p.(%3D)|109/2048               |1/1098                 |1/366              |           |;CSQ=T                     |ENSMUSG00000025911|ENSMUST00000186467|Transcript|upstream_gene_variant|||||||2591|1,T|ENSMUSG00000061024|ENSMUST00000072079|Transcript|5_prime_UTR_variant|109|||||||1,T|ENSMUSG00000025911|ENSMUST00000190654|Transcript|upstream_gene_variant|||||||2603|1,T|ENSMUSG00000025911|ENSMUST00000130927|Transcript|upstream_gene_variant|||||||2576|1,T|ENSMUSG00000025911|ENSMUST00000027044|Transcript|upstream_gene_variant|||||||2539|1,T|ENSMUSG00000025911|ENSMUST00000144177|Transcript|upstream_gene_variant|||||||2432|1;DP=38;DP4=9,17,10,2
 */
public class AnnotationParser extends InfoParser {

    @Override
    public String getInfoId() {
        return "ANN";
    }

    @Override
    public List<Map<String, String>> parse(String infoString) throws Exception {
        // Functional annotations: Allele|Annotation|Annotation_Impact|Gene_Name|Gene_ID|Feature_Type|Feature_ID|Transcript_BioType|Rank|HGVS.c|HGVS.p|cDNA.pos / cDNA.length|CDS.pos / CDS.length|AA.pos / AA.length|Distance|ERRORS / WARNINGS / INFO'
        return super.parse(infoString);
    }

    public List<String> getAnnotationKeys() {
        return Arrays.asList("Allele", "Annotation", "Annotation_Impact", "Gene_Name", "Gene_ID",
                "Feature_Type", "Feature_ID", "Transcript_BioType", "Rank", "HGVS.c",
                "HGVS.p", "cDNA.pos / cDNA.length", "CDS.pos / CDS.length", "AA.pos / AA.length",
                "Distance", "ERRORS / WARNINGS / INFO");
    }
}
