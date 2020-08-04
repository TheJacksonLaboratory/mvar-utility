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
    
    The Executable can be run with the following command:
    
    <code>
        java -jar mvar-utility-all.jar /path/to/data batch_size="integer" type=SNP,DEL,INS
    </code>
    
    Both "batch_size" and "type" are optional, and the corresponding default values are 1000 and "ALL". The available types are "SNP", "DEL" and "INS". Multiple types can be added by separating them by commas ",".
    
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

    2.2 Insert variant-transcript relationships
    
    The executable can be run with the following arguments in order to insert the variant/transcript relationships.
    
    <code>
        java -jar mvar-utility-all.jar /path/to/data REL batch_size="integer" start_id="integer"
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