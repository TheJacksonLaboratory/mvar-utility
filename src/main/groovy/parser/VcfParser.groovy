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
    List<gngs.Variant> parseVcf(String chromosome, def type={"ALL"}, File vcfFile) {
        List<gngs.Variant> varList
        try {
            VCF vcf = VCF.parse(vcfFile.getPath()) { v ->
                ((v.chr == 'chr' + chromosome || v.chr == chromosome) && hasType(type, v))
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

    private boolean hasType(String[] types, def vcf) {
        if (types.contains("ALL"))
            return true
        boolean result = false
        for (String type : types) {
            result = vcf.type == type
            if (result)
                return result
        }
        return result
    }
}
