package parser

import gngs.VCF

import java.util.logging.Logger

class VcfParser {

//    List<gngs.Variant> parseVcf(String chromosome, File vcfFile, String type, Logger logger) {
    List<gngs.Variant> parseVcf(String chromosome, File vcfFile, Logger logger) {
        List<gngs.Variant> varList
        try {
            //vcfFileInputStream.line
            VCF vcf = VCF.parse(vcfFile.getPath()) { v ->
                (v.chr == 'chr' + chromosome || v.chr == chromosome) //&& v.type == type
            }
            varList = vcf.getVariants()
            logger.info("parsed variants = " + vcf.getVariants().size())
            println("parsed variants = " + vcf.getVariants().size())
        } catch (Exception e) {
            logger.severe("Error reading the VCF file " + e.getMessage())
            String error = "Error reading the VCF file " + e.getMessage()
            println(error)
            throw e
        }
        return varList
    }
}
