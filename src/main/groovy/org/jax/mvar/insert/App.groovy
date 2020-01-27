/*
 * This Groovy source file was generated by the Gradle 'init' task.
 */
package org.jax.mvar.insert

class App {

    private static def cmdArgsParser(String[] args) {
        def arguments = [:]
        if (args != null && (args.length == 1 || args.length == 2 || args.length == 3)) {
            arguments["data_path"] = args[0]
            arguments["batch_size"] = 1000
            arguments["use_type"] = false
        }
        if (args != null && (args.length == 2 || args.length == 3)) {
            for (int i = 1; i <= args.length - 1; i++) {
                String[] attribute = args[i].split("=")
                if (attribute[0] == "batch_size") {
                    arguments["batch_size"] = attribute[1].toInteger()
                } else if (attribute[0] == "use_type") {
                    arguments["use_type"] = attribute[1].toBoolean()
                } else {
                    throw new Exception("The second argument need to be \'batch_size=\' or \'use_type=\'")
                }
            }
        }
        if (args == null || args.length > 3) {
            throw new Exception("Command line arguments expected are : data path (mandatory), batch_size=Integer (optional, default=1000), use_type=true/false (optional, default=false)")
        }
        return arguments
    }

    static void main(String[] args) {
        VcfFileInsertionService insertService = new VcfFileInsertionService();
        def arguments = cmdArgsParser(args)
        try {
            String path = arguments["data_path"]
            Integer batchSize = arguments["batch_size"]
            Boolean useType = arguments["use_type"]
            File f = new File(path)
            assert f != null
            if (f.isDirectory()) {
                File[] files = new File(path).listFiles();
                assert files != null
                for (File file : files) {
                    if (file.isFile() && (file.getName().endsWith(".gz") || (file.getName().endsWith(".vcf"))))
                        insertService.loadVCF(file, batchSize, useType)
                }
            } else if (f.isFile()) {
                insertService.loadVCF(f, batchSize, useType);
            }
        } catch (Exception exc) {
            System.out.println(Arrays.toString(exc.getStackTrace()));
        }

    }
}
