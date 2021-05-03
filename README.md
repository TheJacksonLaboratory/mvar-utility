## MVAR-utility

This application is used to :
* insert large amount of variation (VCF files) data to the MVAR MySQL database with the following schema (in ~resources/mvar_core_schema.xml)
* compare some MGI variation data to the existing data already in the MVAR database
* convert CSV variation files (provided by Robyn Ball) to a VCF variation file.

1. Download and build executable

    Once the repository has been checkout on local, you can build the executable JAR file with the following command from within the repo root folder:
    
    <code>
       ./gradlew shadowJar
    </code>

2. Run Insertion
    
    2.1 Insert variation file(s)
    
    The Executable can be run with the following command,
    
    To insert one file (vcf or vcf compressed (gz):
    <code>
        java -jar mvar-utility-all.jar /path/to/data_file.vcf batch_size=integer
    </code>
    
    The "batch_size" is optional and the default value is 1000.

    To insert multiple files, a folder where the files are located can be passed as a parameter:
    <code>
        java -jar mvar-utility-all.jar /path/to/data_folder batch_size=integer
    </code>
   
    The command above requires Java 8 to be installed and possible the following JVM parameters to be set up depending on the size of the data to insert into the database.
    
    <code>
        -XX:+UseG1GC
        -Xmx20g 
        -Xms20g 
        -XX:-UseGCOverheadLimit
    </code>
       
    And in case the java monitoring is needed to use a remote JConsole for instance, one can run the java process with the following parameters:   

    <code>
        -Dcom.sun.management.jmxremote 
        -Dcom.sun.management.jmxremote.authenticate=false 
        -Dcom.sun.management.jmxremote.ssl=false 
        -Dcom.sun.management.jmxremote.port=5000 
        -Djava.rmi.server.hostname=mvr-test01
    </code>

    2.2 Insert variant-transcript and variant-strain relationships
    
    The insertion command does not add the corresponding relationships in order to make the insertion take less time. To insert the relationships the following commands need to be run:
    
    The variant/transcript relationships are added with the "REL" parameter:
    
    <code>
        java -jar mvar-utility-all.jar REL batch_size="integer" start_id="integer"
    </code>
    
    where batch_size is optional (1000000 by default) and start_id is optional (1 by default).

    The variant/strain relationships are added with the "GENO" parameter (we know whether there is a variant for a certain strain by parsing the genotype information in the VCF data). A required parameter is "strain_path" which points to a text file with the list of strains in the DB (separated by carriage returns):
    <code>
       java -jar mvar-utility-all.jar GENO batch_size="integer" start_id="integer" strain_path=/path/to/strain_file.txt
    </code>

    where batch_size is optional (1000000 by default) and start_id is optional (1 by default).
     
3. Run MGI comparison

    In order to compare a particular VCF file (checking the number of existing variation in the DB) the following command can be run:
    
    <code>
        java -jar mvar-utility-all.jar MGI /path/to/vcf/file
    </code>
    
     
4. Run CSV to VCF conversion

    We can also convert a csv file (with the format and column order of the provided example zipped file) into a VCF file:
    
    <code>
        java -jar mvar-utility-all.jar CONVERT /path/to/csv/file
    </code>