package org.jax.mvar.utility.parser;

import org.jax.mvar.utility.model.Variant;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class VcfParserTest {

    private static final List<String> REF_TXTS = Arrays.asList("1_3421849_C_T", "1_3670629_G_C", "1_3670967_T_C", "4_11521167_C_T", "14_66046191_C_T");
    private static final List<String> PROTEIN_POSITIONS = Arrays.asList("284", "240", "128", "858", "421");
    private static final List<String> AMINO_ACID_CHANGES = Arrays.asList("G/S", "H/Q", "I/V", "A/V", "D/N");

    /**
     * Test parse method
     */
    @Test
    public void testParser() throws Exception {
//        1	3421849	.	C	T	999	PASS	AC=44;AD=1329,1052;AN=104;ANN=T|missense_variant|MODERATE|Xkr4|497097|transcript|ENSMUST00000070533.4|Coding|2/3|c.850G>A|p.(G284S)|1000/457017|850/1944|284/648||;CSQ=T|missense_variant|MODERATE|Xkr4|ENSMUSG00000051951|Transcript|ENSMUST00000070533|protein_coding|2/3||ENSMUST00000070533.4:c.850G>A|ENSMUSP00000070648.4:p.Gly284Ser|1000|850|284|G/S|Ggc/Agc|rs30963380||-1||SNV|MGI||YES||1|P1|CCDS14803.1|ENSMUSP00000070648|Q5GH67||UPI00004C728B||1|tolerated(0.33)|Pfam:PF09815&PANTHER:PTHR16024&PANTHER:PTHR16024:SF16|||1:g.3421849C>T||||||||||||||||||||||||||||;DP=2381;DP4=657,672,482,570;MQ=59	GT:AD:ADF:ADR:DP:FI:GQ:PL	1/1:0,25:0,14:0,11:25:1:70:255,75,0	1/1:1,49:1,17:0,32:50:1:99:255,119,0	1/1:0,20:0,9:0,11:20:1:55:255,60,0	1/1:0,57:0,27:0,30:57:1:99:255,172,0	0/0:60,1:29,0:31,1:61:1:99:0,143,255	0/0:49,0:23,0:26,0:49:1:99:0,148,255	0/0:33,0:19,0:14,0:33:1:97:0,99,255	0/0:51,0:18,0:33,0:51:1:99:0,154,255	1/1:0,72:0,32:0,40:72:1:99:255,217,0	1/1:0,65:0,29:0,36:65:1:99:255,196,0	0/0:14,0:4,0:10,0:14:1:40:0,42,255	0/0:58,0:32,0:26,0:58:1:99:0,175,255	0/0:32,0:13,0:19,0:32:1:94:0,96,255	0/0:47,0:28,0:19,0:47:1:99:0,141,255	0/0:53,0:23,0:30,0:53:1:99:0,160,255	1/1:0,49:0,19:0,30:49:1:99:255,148,0	1/1:0,51:0,26:0,25:51:1:99:255,154,0	0/0:39,0:19,0:20,0:39:1:99:0,117,255	0/0:44,1:21,1:23,0:45:1:98:0,100,255	0/0:56,0:29,0:27,0:56:1:99:0,169,255	0/0:45,0:19,0:26,0:45:1:99:0,135,255	0/0:48,0:25,0:23,0:48:1:99:0,144,255	0/0:43,0:20,0:23,0:43:1:99:0,129,255	0/0:54,0:21,0:33,0:54:1:99:0,163,255	1/1:0,70:0,34:0,36:70:1:99:255,211,0	1/1:0,27:0,11:0,16:27:1:76:255,81,0	0/0:33,0:20,0:13,0:33:1:97:0,99,255	0/0:53,0:26,0:27,0:53:1:99:0,160,255	0/0:15,0:7,0:8,0:15:1:43:0,45,255	0/0:52,0:27,0:25,0:52:1:99:0,157,255	1/1:0,51:0,27:0,24:51:1:99:255,154,0	1/1:0,42:0,18:0,24:42:1:99:255,126,0	0/0:34,0:24,0:10,0:34:1:99:0,102,255	1/1:0,54:0,24:0,30:54:1:99:255,163,0	1/1:0,52:0,29:0,23:52:1:99:255,157,0	0/0:36,0:18,0:18,0:36:1:99:0,108,255	0/0:69,0:37,0:32,0:69:1:99:0,208,255	0/0:49,0:26,0:23,0:49:1:99:0,148,255	1/1:0,24:0,10:0,14:24:1:67:255,72,0	0/0:47,0:24,0:23,0:47:1:99:0,141,255	1/1:0,43:0,15:0,28:43:1:99:255,129,0	1/1:0,40:0,21:0,19:40:1:99:255,120,0	0/0:64,0:32,0:32,0:64:1:99:0,193,255	0/0:33,0:19,0:14,0:33:1:97:0,99,255	0/0:43,0:21,0:22,0:43:1:99:0,129,255	1/1:1,42:0,16:1,26:43:1:99:255,107,0	1/1:2,43:1,17:1,26:45:1:57:255,62,0	0/0:58,0:22,0:36,0:58:1:99:0,175,255	1/1:0,86:0,46:0,40:86:1:99:255,255,0	1/1:0,44:0,19:0,25:44:1:99:255,132,0	1/1:0,44:0,21:0,23:44:1:99:255,132,0	0/0:13,0:9,0:4,0:13:1:37:0,39,255
//        1	3670629	.	G	C	190	PASS	AC=2;AD=2407,62;AN=104;ANN=C|missense_variant|MODERATE|Xkr4|497097|transcript|ENSMUST00000070533.4|Coding|1/3|c.720C>G|p.(H240Q)|870/457017|720/1944|240/648||;CSQ=C|missense_variant|MODERATE|Xkr4|ENSMUSG00000051951|Transcript|ENSMUST00000070533|protein_coding|1/3||ENSMUST00000070533.4:c.720C>G|ENSMUSP00000070648.4:p.His240Gln|870|720|240|H/Q|caC/caG|rs253644334||-1||SNV|MGI||YES||1|P1|CCDS14803.1|ENSMUSP00000070648|Q5GH67||UPI00004C728B||1|tolerated(0.39)|Pfam:PF09815&PANTHER:PTHR16024&PANTHER:PTHR16024:SF16|||1:g.3670629G>C||||||||||||||||||||||||||||,C|regulatory_region_variant|MODIFIER|||RegulatoryFeature|ENSMUSR00000293470|promoter||||||||||rs253644334||||SNV||||||||||||||||||1:g.3670629G>C||||||||||||||||||||||||||||;DP=2483;DP4=1281,1126,33,43;MQ=59	GT:AD:ADF:ADR:DP:FI:GQ:PL	0/0:30,0:14,0:16,0:30:1:99:0,90,255	0/0:63,0:30,0:33,0:64:1:99:0,190,255	0/0:20,0:10,0:10,0:20:1:73:0,60,255	0/0:67,0:38,0:29,0:67:1:99:0,202,255	0/0:65,0:34,0:31,0:65:1:99:0,196,255	0/0:34,0:22,0:12,0:35:1:98:0,102,255	0/0:13,0:8,0:5,0:13:1:52:0,39,255	0/0:33,0:19,0:14,0:34:1:90:0,99,255	0/0:90,0:42,0:48,0:90:1:99:0,255,255	0/0:60,0:27,0:33,0:61:1:99:0,181,255	0/0:15,0:10,0:5,0:15:1:58:0,45,255	0/0:62,0:34,0:28,0:63:1:99:0,187,255	0/0:16,0:7,0:9,0:17:1:60:0,48,255	0/0:35,0:22,0:13,0:35:1:99:0,105,255	0/0:60,0:33,0:27,0:60:1:99:0,181,255	0/0:20,0:10,0:10,0:20:1:73:0,60,255	0/0:43,0:21,0:22,0:43:1:99:0,129,255	0/0:24,0:8,0:16,0:24:1:85:0,72,255	0/0:41,0:26,0:15,0:41:1:99:0,123,255	0/0:71,0:33,0:38,0:71:1:99:0,214,255	0/0:29,0:12,0:17,0:29:1:99:0,87,255	0/0:51,0:27,0:24,0:51:1:99:0,154,255	0/0:21,0:9,0:12,0:21:1:76:0,63,255	0/0:63,0:32,0:31,0:63:1:99:0,190,255	0/0:144,0:92,0:52,0:144:1:99:0,255,255	0/0:37,0:27,0:10,0:37:1:99:0,111,255	0/0:25,0:14,0:11,0:25:1:88:0,75,255	0/0:74,0:41,0:33,0:74:1:99:0,223,255	0/0:11,0:3,0:8,0:11:1:46:0,33,229	0/0:90,0:53,0:37,0:93:1:99:0,255,255	0/0:59,0:32,0:27,0:60:1:99:0,178,255	0/0:22,0:12,0:10,0:22:1:79:0,66,255	0/0:25,0:13,0:12,0:25:1:88:0,75,255	0/0:65,0:32,0:33,0:66:1:99:0,196,255	0/0:39,0:21,0:18,0:39:1:99:0,117,255	0/0:29,0:16,0:13,0:29:1:99:0,87,255	0/0:55,0:30,0:25,0:55:1:99:0,166,255	0/0:60,0:33,0:27,0:60:1:99:0,181,255	0/0:31,0:19,0:12,0:31:1:99:0,93,255	0/0:51,0:30,0:21,0:51:1:99:0,154,255	0/0:30,0:11,0:19,0:30:1:99:0,90,255	0/0:30,0:12,0:18,0:30:1:99:0,90,255	0/0:64,0:34,0:30,0:64:1:99:0,193,255	0/0:46,0:25,0:21,0:46:1:99:0,138,255	0/0:77,0:41,0:36,0:77:1:99:0,232,255	0/0:50,0:28,0:22,0:50:1:99:0,151,255	0/0:74,0:41,0:33,0:74:1:99:0,223,255	0/0:54,0:20,0:34,0:55:1:99:0,163,255	0/0:96,0:51,0:45,0:96:1:99:0,255,255	0/0:36,0:18,0:18,0:36:1:99:0,108,255	1/1:0,62:0,29:0,33:64:1:99:255,187,0	0/0:7,0:4,0:3,0:7:1:34:0,21,210
//        1	3670967	.	T	C	26.44	PASS	AC=1;AD=1942,5;AN=104;ANN=C|missense_variant|MODERATE|Xkr4|497097|transcript|ENSMUST00000070533.4|Coding|1/3|c.382A>G|p.(I128V)|532/457017|382/1944|128/648||;CSQ=C|missense_variant|MODERATE|Xkr4|ENSMUSG00000051951|Transcript|ENSMUST00000070533|protein_coding|1/3||ENSMUST00000070533.4:c.382A>G|ENSMUSP00000070648.4:p.Ile128Val|532|382|128|I/V|Atc/Gtc|||-1||SNV|MGI||YES||1|P1|CCDS14803.1|ENSMUSP00000070648|Q5GH67||UPI00004C728B||1|tolerated(0.4)|Pfam:PF09815&Transmembrane_helices:TMhelix&PANTHER:PTHR16024&PANTHER:PTHR16024:SF16|||1:g.3670967T>C||||||||||||||||||||||||||||,C|regulatory_region_variant|MODIFIER|||RegulatoryFeature|ENSMUSR00000293470|promoter||||||||||||||SNV||||||||||||||||||1:g.3670967T>C||||||||||||||||||||||||||||;DP=1954;DP4=804,1138,3,9;MQ=59	GT:AD:ADF:ADR:DP:FI:GQ:PL	0/0:6,0:1,0:5,0:6:1:37:0,18,120	0/0:43,0:18,0:25,0:43:1:99:0,129,255	0/0:9,0:0,0:9,0:9:1:46:0,27,132	0/0:50,0:27,0:23,0:50:1:99:0,151,255	0/0:45,0:20,0:25,0:45:1:99:0,135,255	0/0:41,0:19,0:22,0:41:1:99:0,123,255	0/0:14,0:7,0:7,0:14:1:61:0,42,255	0/0:39,0:16,0:23,0:39:1:99:0,117,255	0/0:75,0:38,0:37,0:75:1:99:0,226,255	0/0:44,0:7,0:37,0:44:1:99:0,132,255	0/0:5,0:1,0:4,0:5:0:34:0,15,142	0/0:72,0:34,0:38,0:72:1:99:0,217,255	0/0:12,0:3,0:9,0:12:1:55:0,36,246	0/0:38,0:24,0:14,0:38:1:99:0,114,255	0/0:58,1:29,0:29,1:59:1:99:0,160,255	0/0:11,0:1,0:10,0:11:1:52:0,33,202	0/0:30,0:11,0:19,0:30:1:99:0,90,255	0/0:23,0:7,0:16,0:23:1:88:0,69,255	0/0:53,0:26,0:27,0:55:1:99:0,160,255	0/0:62,0:27,0:35,0:62:1:99:0,187,255	0/0:24,0:15,0:9,0:24:1:91:0,72,255	0/0:35,0:21,0:14,0:35:1:99:0,105,255	0/0:11,0:2,0:9,0:11:1:52:0,33,253	0/0:47,0:23,0:24,0:47:1:99:0,141,255	0/0:116,0:39,0:77,0:116:1:99:0,255,255	0/0:29,0:6,0:23,0:29:1:99:0,87,255	0/0:33,0:21,0:12,0:33:1:99:0,99,255	0/0:52,0:9,0:43,0:52:1:99:0,157,255	0/0:4,0:1,0:3,0:4:0:31:0,12,106	0/0:58,0:19,0:39,0:58:1:99:0,175,255	0/0:62,0:27,0:35,0:62:1:99:0,187,255	0/0:33,0:18,0:15,0:33:1:99:0,99,255	0/0:21,0:7,0:14,0:21:1:82:0,63,255	0/0:53,1:24,0:29,1:55:1:99:0,145,255	0/0:21,0:14,0:7,0:21:1:82:0,63,255	0/0:21,0:1,0:20,0:21:1:82:0,63,197	0/0:49,0:26,0:23,0:50:1:99:0,148,255	0/0:33,0:4,0:29,0:33:1:99:0,99,255	0/1:9,3:3,0:6,3:12:0:51:71,0,225	0/0:40,0:22,0:18,0:40:1:99:0,120,255	0/0:25,0:15,0:10,0:25:1:94:0,75,255	0/0:40,0:17,0:23,0:40:1:99:0,120,255	0/0:45,0:8,0:37,0:45:1:99:0,135,255	0/0:24,0:17,0:7,0:24:1:91:0,72,255	0/0:30,0:6,0:24,0:30:1:99:0,90,255	0/0:49,0:24,0:25,0:50:1:99:0,148,255	0/0:42,0:10,0:32,0:42:1:99:0,126,255	0/0:60,0:30,0:30,0:61:1:99:0,181,255	0/0:57,0:12,0:45,0:57:1:99:0,172,255	0/0:29,0:13,0:16,0:29:1:99:0,87,255	0/0:52,0:31,0:21,0:53:1:99:0,157,255	0/0:8,0:3,0:5,0:8:1:43:0,24,209
//        4	11521167	.	C	T	999	PASS	AC=10;AD=2261,232;AN=104;ANN=T|missense_variant|MODERATE|Virma|66185|transcript|ENSMUST00000055372.13|Coding|10/13|c.2573C>T|p.(A858V)|2658/41664|2573/3420|858/1140||,T|missense_variant|MODERATE|Virma|66185|transcript|ENSMUST00000059914.12|Coding|10/24|c.2573C>T|p.(A858V)|2658/64727|2573/5436|858/1812||,T|missense_variant|MODERATE|Virma|66185|transcript|ENSMUST00000108307.2|Coding|11/25|c.2723C>T|p.(A908V)|2723/64642|2723/5586|908/1862||;CSQ=T|missense_variant|MODERATE|Virma|ENSMUSG00000040720|Transcript|ENSMUST00000055372|protein_coding|10/13||ENSMUST00000055372.13:c.2573C>T|ENSMUSP00000063188.7:p.Ala858Val|2658|2573|858|A/V|gCt/gTt|rs218355622||1||SNV|MGI||||1|||ENSMUSP00000063188|A2AIV2||UPI00001C2E89||1|tolerated(1)|PANTHER:PTHR23185|||4:g.11521167C>T||||||||||||||||||||||||||||,T|missense_variant|MODERATE|Virma|ENSMUSG00000040720|Transcript|ENSMUST00000059914|protein_coding|10/24||ENSMUST00000059914.12:c.2573C>T|ENSMUSP00000058078.6:p.Ala858Val|2658|2573|858|A/V|gCt/gTt|rs218355622||1||SNV|MGI||||1|P1|CCDS84705.1|ENSMUSP00000058078|A2AIV2||UPI00001E35E5||1|tolerated(1)|PANTHER:PTHR23185|||4:g.11521167C>T||||||||||||||||||||||||||||,T|missense_variant|MODERATE|Virma|ENSMUSG00000040720|Transcript|ENSMUST00000108307|protein_coding|11/25||ENSMUST00000108307.2:c.2723C>T|ENSMUSP00000103943.2:p.Ala908Val|2723|2723|908|A/V|gCt/gTt|rs218355622||1||SNV|MGI||YES||5||CCDS38693.1|ENSMUSP00000103943||E9PZY8|UPI00001F1A32||1|tolerated(1)|PANTHER:PTHR23185|||4:g.11521167C>T||||||||||||||||||||||||||||;DP=2496;DP4=1243,1018,147,88;MQ=59	GT:AD:ADF:ADR:DP:FI:GQ:PL	0/0:52,0:25,0:27,0:52:1:99:0,157,255	0/0:76,0:43,0:33,0:76:1:99:0,229,255	0/0:26,1:16,1:10,0:28:1:73:0,67,244	0/0:66,0:41,0:25,0:66:1:99:0,199,255	0/0:48,0:27,0:21,0:48:1:99:0,144,255	0/0:50,0:26,0:24,0:50:1:99:0,151,255	0/0:39,0:18,0:21,0:39:1:99:0,117,255	0/0:56,0:28,0:28,0:56:1:99:0,169,255	0/0:85,0:47,0:38,0:85:1:99:0,255,255	0/0:44,0:28,0:16,0:44:1:99:0,132,255	0/0:14,0:8,0:6,0:14:1:48:0,42,255	0/0:53,0:30,0:23,0:53:1:99:0,160,255	0/0:35,0:21,0:14,0:35:1:99:0,105,255	0/0:52,0:34,0:18,0:52:1:99:0,157,255	0/0:65,0:33,0:32,0:65:1:99:0,196,255	0/0:48,0:22,0:26,0:48:1:99:0,144,255	0/0:50,0:26,0:24,0:50:1:99:0,151,255	0/0:42,0:23,0:19,0:42:1:99:0,126,255	1/1:1,34:1,24:0,10:35:1:64:255,77,0	0/0:51,0:39,0:12,0:51:1:99:0,154,255	0/0:44,0:27,0:17,0:44:1:99:0,132,255	1/1:0,70:0,41:0,29:70:1:99:255,211,0	0/0:32,0:15,0:17,0:32:1:99:0,96,255	0/0:56,0:34,0:22,0:56:1:99:0,169,255	0/0:90,0:44,0:46,0:91:1:99:0,255,255	0/0:40,0:16,0:24,0:40:1:99:0,120,255	1/1:0,33:0,16:0,17:33:1:86:255,99,0	0/0:45,0:24,0:21,0:45:1:99:0,135,255	0/0:11,1:8,0:3,1:12:0:11:0,4,234	0/0:47,0:23,0:24,0:47:1:99:0,141,255	0/0:65,1:29,1:36,0:66:1:99:0,164,255	0/0:30,0:16,0:14,0:30:1:96:0,90,255	0/0:18,0:8,0:10,0:18:1:60:0,54,255	0/0:82,0:55,0:27,0:82:1:99:0,247,255	0/0:53,0:26,0:27,0:53:1:99:0,160,255	0/0:32,0:16,0:16,0:32:1:99:0,96,255	0/0:60,0:36,0:24,0:60:1:99:0,181,255	0/0:53,0:29,0:24,0:53:1:99:0,160,255	0/0:40,0:17,0:23,0:40:1:99:0,120,255	1/1:2,43:1,25:1,18:45:1:92:255,105,0	0/0:46,0:28,0:18,0:46:1:99:0,138,255	0/0:30,0:14,0:16,0:30:1:96:0,90,255	0/0:57,0:41,0:16,0:57:1:99:0,172,255	0/0:51,0:29,0:22,0:51:1:99:0,154,255	0/0:57,0:32,0:25,0:57:1:99:0,172,255	0/0:36,0:21,0:15,0:36:1:99:0,108,255	0/0:43,0:24,0:19,0:44:1:99:0,129,255	1/1:0,49:0,36:0,13:49:1:99:255,148,0	0/0:80,0:47,0:33,0:80:1:99:0,241,255	0/0:33,0:16,0:17,0:33:1:99:0,99,255	0/0:52,0:23,0:29,0:52:1:99:0,157,255	0/0:23,0:8,0:15,0:23:1:75:0,69,255
//        14	66046191	.	C	T	317	PASS	AC=4;AD=2063,69;AN=104;ANN=T|missense_variant|MODERATE|Adam2|11495|transcript|ENSMUST00000022618.5|Coding|13/21|c.1261G>A|p.(D421N)|1272/50405|1261/2208|421/736||,T|non_coding_transcript_exon_variant|LOW|Adam2|11495|transcript|ENSMUST00000225667.1|Noncoding|13/13|n.1236G>A||1236/31709||||;CSQ=T|missense_variant|MODERATE|Adam2|ENSMUSG00000022039|Transcript|ENSMUST00000022618|protein_coding|13/21||ENSMUST00000022618.5:c.1261G>A|ENSMUSP00000022618.5:p.Asp421Asn|1272|1261|421|D/N|Gac/Aac|rs1135047373||-1||SNV|MGI||YES||1|P1|CCDS36959.1|ENSMUSP00000022618|Q60718||UPI0000021DC5||1|tolerated(0.47)|PROSITE_profiles:PS50214&PANTHER:PTHR11905&PANTHER:PTHR11905:SF108&Gene3D:4.10.70.10&Pfam:PF00200&SMART:SM00050&Superfamily:SSF57552|||14:g.66046191C>T||||||||||||||||||||||||||||,T|downstream_gene_variant|MODIFIER|Gm10233|ENSMUSG00000068165|Transcript|ENSMUST00000089275|processed_pseudogene||||||||||rs1135047373|484|1||SNV|MGI||YES|||||||||||||||14:g.66046191C>T||||||||||||||||||||||||||||,T|non_coding_transcript_exon_variant|MODIFIER|Adam2|ENSMUSG00000022039|Transcript|ENSMUST00000225667|retained_intron|13/13||ENSMUST00000225667.1:n.1236G>A||1236|||||rs1135047373||-1||SNV|MGI||||||||||||1|||||14:g.66046191C>T||||||||||||||||||||||||||||;DP=2139;DP4=1211,852,52,24;MQ=59	GT:AD:ADF:ADR:DP:FI:GQ:PL	0/0:37,0:23,0:14,0:37:1:99:0,111,255	0/0:48,0:31,0:17,0:50:1:99:0,144,255	0/0:17,0:14,0:3,0:17:1:61:0,51,183	0/0:50,0:33,0:17,0:50:1:99:0,151,255	0/0:39,0:20,0:19,0:39:1:99:0,117,255	0/0:45,0:23,0:22,0:46:1:99:0,135,255	0/0:33,0:20,0:13,0:33:1:99:0,99,255	0/0:47,0:27,0:20,0:48:1:99:0,141,255	0/0:105,0:54,0:51,0:105:1:99:0,255,255	0/0:59,0:35,0:24,0:59:1:99:0,178,255	0/0:13,0:7,0:6,0:13:1:49:0,39,255	0/0:46,0:24,0:22,0:46:1:99:0,138,255	0/0:31,0:16,0:15,0:31:1:99:0,93,255	0/0:50,0:23,0:27,0:50:1:99:0,151,255	0/0:46,0:25,0:21,0:47:1:99:0,138,255	0/0:30,0:12,0:18,0:30:1:99:0,90,255	0/0:44,0:19,0:25,0:44:1:99:0,132,255	0/0:40,0:19,0:21,0:40:1:99:0,120,255	0/0:28,0:28,0:0,0:28:1:94:0,84,229	0/0:52,0:30,0:22,0:52:1:99:0,157,255	0/0:43,0:22,0:21,0:43:1:99:0,129,255	0/0:34,0:30,0:4,0:34:1:99:0,102,255	0/0:37,0:15,0:22,0:37:1:99:0,111,255	0/0:46,0:23,0:23,0:46:1:99:0,138,255	0/0:65,0:42,0:23,0:66:1:99:0,196,255	0/0:40,0:23,0:17,0:40:1:99:0,120,255	1/1:0,9:0,9:0,0:9:0:10:158,27,0	0/0:47,0:29,0:18,0:47:1:99:0,141,255	0/0:10,0:5,0:5,0:10:1:40:0,30,245	0/0:38,0:22,0:16,0:38:1:99:0,114,255	0/0:49,0:33,0:16,0:49:1:99:0,148,255	0/0:33,0:24,0:9,0:33:1:99:0,99,255	0/0:19,0:11,0:8,0:19:1:67:0,57,255	0/0:45,0:26,0:19,0:46:1:99:0,135,255	0/0:46,0:25,0:21,0:46:1:99:0,138,255	0/0:34,1:12,0:22,1:35:1:99:0,90,255	0/0:63,0:41,0:22,0:63:1:99:0,190,255	0/0:63,0:45,0:18,0:63:1:99:0,190,255	0/0:21,0:11,0:10,0:21:1:73:0,63,255	0/0:21,0:19,0:2,0:21:1:73:0,63,255	0/0:40,0:26,0:14,0:40:1:99:0,120,255	0/0:44,0:22,0:22,0:44:1:99:0,132,255	0/0:75,0:45,0:30,0:75:1:99:0,226,255	0/0:37,0:20,0:17,0:37:1:99:0,111,255	0/0:60,0:35,0:25,0:60:1:99:0,181,255	0/0:41,0:23,0:18,0:41:1:99:0,123,255	0/0:28,0:13,0:15,0:28:1:94:0,84,255	0/0:32,0:31,0:1,0:32:1:99:0,96,246	1/1:0,59:0,38:0,21:59:1:99:255,178,0	0/0:41,0:26,0:15,0:41:1:99:0,123,255	0/0:37,0:23,0:14,0:37:1:99:0,111,255	0/0:14,0:6,0:8,0:14:1:52:0,42,255

        Map<String, Variant> variations = VcfParser.parseVcf(new File("src/test/resources/variant_test.vcf"), false);
        // test length
        Assert.assertEquals(5, variations.size());

        int idx = 0;
        for (Map.Entry<String, Variant> entry : variations.entrySet()) {
            String refTxt = entry.getKey();
            Variant variant = entry.getValue();
            // Test variantRefTxt
            Assert.assertEquals(REF_TXTS.get(idx), variant.getVariantRefTxt());
            Assert.assertEquals(REF_TXTS.get(idx), refTxt);
            // test protein position
            Assert.assertEquals(PROTEIN_POSITIONS.get(idx), variant.getProteinPosition());
            // test amino acids change
            Assert.assertEquals(AMINO_ACID_CHANGES.get(idx), variant.getAminoAcidChange());
            idx++;
        }
    }

}
