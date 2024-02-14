## MVAR-utility

This application is used to :
* insert large amount of variation (VCF files) data to the MVAR MySQL database with the following schema (in ~resources/mvar_core_schema.xml)
* compare some MGI variation data to the existing data already in the MVAR database
* convert CSV variation files (provided by Robyn Ball) to a VCF variation file.

1. Download and build executable

    Once the repository has been checkout on local, you can build the executable JAR file with the following command from within the repo root folder:

    ```
       ./gradlew shadowJar
    ```

2. Run Insertion
    
    2.1 Insert variation file(s)
    
    The Executable can be run with the following command,
    
    To insert one file (vcf or vcf compressed (gz):
    ```
        java -jar mvar-utility-all.jar INSERT -data_path /path/to/data_file.vcf -header_path /path/to/header_file.txt -batch_size 5000 -check_canon
    ```
   
    The "-header_path" parameter is required if the "data_path" provided is a folder and some files do not have a header.
   
    The "-batch_size" is optional; the default value is 1000. "-check_canon" is also optional and if present as a parameter, then the insertion will include a canonical variant check for uniqueness, so that no duplicates are added to the DB.

    The "-reference" is optional (set to "mm10" by default) and can be set to either "mm10" or "mm39".

    To insert multiple files, a folder where the files are located can be passed as a parameter:
    ```
        java -jar mvar-utility-all.jar INSERT -data_path /path/to/data_folder -header_path /path/to/header_file.txt -batch_size 5000 -check_canon
    ```
   
    The command above requires Java 8 to be installed and possible the following JVM parameters to be set up depending on the size of the data to insert into the database.
    
    ```
        -XX:+UseG1GC
        -Xmx20g 
        -Xms20g 
        -XX:-UseGCOverheadLimit
    ```
       
    And in case the java monitoring is needed to use a remote JConsole for instance, one can run the java process with the following parameters:   

    ```
        -Dcom.sun.management.jmxremote 
        -Dcom.sun.management.jmxremote.authenticate=false 
        -Dcom.sun.management.jmxremote.ssl=false 
        -Dcom.sun.management.jmxremote.port=5000 
        -Djava.rmi.server.hostname=mvr-test01
    ```

    2.2 Insert variant-transcript and variant-strain relationships
    
    The insertion command does not add the corresponding relationships in order to make the insertion take less time. To insert the relationships the following commands need to be run:
    
    The variant/transcript relationships are added with the "REL" parameter:
    
    ```
        java -jar mvar-utility-all.jar REL -batch_size 1000 -start_id 1 -source_name Sanger_V7
    ```
    
    where -batch_size is optional (100000 by default), -start_id is optional (1 by default) and -source_name is by default "Sanger_V7". Available sources are "Sanger_V4, Sanger_V5, Sanger_V6, Sanger_V7 (mm10), Sanger_v8 (mm39), MGI_mm10, MGI_mm39 and SNPGrid_V1" for now.

    Before inserting the strain/variant relationship, make sure that the list of strain names in the strain file that you have 
   (list of strains/individuals pulled from dataset) does exist in the Strain table. The variant/strain relationships are added with the "GENO" parameter (we know whether there is a variant for a certain strain by parsing the genotype information in the VCF data). A required parameter is "strain_path" which points to a text file with the list of strains in the DB (separated by carriage returns):
    ```
       java -jar mvar-utility-all.jar GENO -strain_path /path/to/strain_file.txt -batch_size 1000 -start_id 1 -imputed 1
    ```

    where -batch_size is optional (1000000 by default), -start_id is optional (1 by default) and -imputed is optional (0 by default, where 0=sanger, 1=snpgrid, 2=mgi).

   2.3 Update MVAR id for mm10 lifted to mm39 variants also present in the new mm39 release (link/canonicalize with mm10 MVAR id)

   ```
       java -jar mvar-utility-all.jar CANON  -batch_size 1000 -start_id 1
   ```

3. Run MGI comparison

    In order to compare a particular VCF file (checking the number of existing variation in the DB) the following command can be run:
    
    ```
        java -jar mvar-utility-all.jar MGI -data_path /path/to/vcf/file
    ``` 
     
4. Run CSV to VCF conversion

    We can also convert a csv file (with the format and column order of the provided example zipped file) into a VCF file:
    
    ```
        java -jar mvar-utility-all.jar CONVERT -data_path /path/to/csv/file
    ```