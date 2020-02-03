package parser

import gngs.VCF

/**
 * Class that uses the gngs.VCF parser
 * 
 *
 */
class VcfParser {

	/**
	 * Parse a VCF file given a chrommosome and optionally a type.
	 * @param chromosome can be 1, 2, 3, ...19, X, Y, MT
	 * @param type can be SNP, DEL, INS or ALL
	 * @param vcfFile
	 * @return
	 */
    List<gngs.Variant> parseVcf(String chromosome, String type="ALL", File vcfFile) {
        List<gngs.Variant> varList
        try {
            VCF vcf = VCF.parse(vcfFile.getPath()) { v ->
                ((v.chr == 'chr' + chromosome || v.chr == chromosome) && (type == "ALL" ? true : v.type == type))
            }
            varList = vcf.getVariants()
            println("parsed variants = " + vcf.getVariants().size())
        } catch (Exception e) {
            String error = "Error reading the VCF file " + e.getMessage()
            println(error)
            throw e
        }
        return varList
    }
}
