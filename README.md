## MVAR-insert

This application is used to insert large amount of data to a MySQL database with the schema (in ~resources/mvar_core_schema.xml).

1. Download and build executable

    Once the repository has been checkout on local, you can build the executable JAR file with the following command from within the repo root folder:
    
    <code>
       ./gradlew shadowJar
    </code>


2. Run executable

    The Executable can be run with the following command:
    
    <code>
        java -jar mvar-insert-all.jar /path/to/data batch_size="integer" type=SNP,DEL,INS
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