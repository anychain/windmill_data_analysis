package com.ibm.poc

/*
import parquet.hadoop.{ParquetOutputFormat, ParquetInputFormat}
import spark.SparkContext
import spark.SparkContext._
import org.apache.hadoop.mapreduce.Job
import parquet.avro.{AvroParquetOutputFormat, AvroWriteSupport, AvroReadSupport}
import parquet.filter.{RecordFilter, UnboundRecordFilter}
import java.lang.Iterable
import parquet.column.ColumnReader
import parquet.filter.ColumnRecordFilter._
import parquet.filter.ColumnPredicates._
import com.google.common.io.Files
* 
*/
import java.io.{ByteArrayOutputStream, File}


import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StructType,StructField,StringType};
import org.apache.spark.sql.SaveMode
import java.util.Properties
import java.text.SimpleDateFormat
import java.util.Date
import java.sql.Timestamp
import org.apache.spark.sql.SQLContext



case class OptConfig(
    hdfsUrl: String = "",
    sparkMaster: String = "",
    csvFiles: Seq[File] = Seq(),
    values: Map[String, String] = Map(),
    conditions: Map[String, String] = Map(),
    verbose: Boolean = false,
    debug: Boolean = false,
    mode: String = "")


/*
 * Parameters:
 * <hdfs url to parquet files>: hdfs://<host>:<port>/user/hive/poc
 * <spark master>
 * <action>: delete/update/insert
 * <>
 */
object ParquetDataUpdater {


  def parseArgs(args: Array[String]) : OptConfig = {
    val parser = new scopt.OptionParser[OptConfig]("ParquetDataUpdater"){
      head("ParquetDataUpdater", "0.1")
      note("Commands:\n")
      cmd("update") action { (_, c) => 
        c.copy(mode = "update") } text("update the data.") children(
          opt[Map[String, String]]("conditions") valueName("k1=v1,k2=v2...")
            action { (x, c) => c.copy(conditions = x) } text("conditions, like where options in SQL."),
          opt[Map[String, String]]("values") valueName("col1=v1,col2=v2...")
            action { (x, c) => c.copy(values = x) } text("values to set.")
        )
      cmd("delete") action { (_, c) => 
        c.copy(mode = "delete") } text("delete the data.") children(
          opt[Map[String, String]]("conditions") valueName("k1=v1,k2=v2...")
            action { (x, c) => c.copy(conditions = x) } text("conditions, like where options in SQL.")
        )
      cmd("insert") action { (_, c) => 
        c.copy(mode = "insert") } text("insert data from csv files.") children(
          opt[Seq[File]]('f', "csvFiles") valueName("<csv file1>,<csv file2>...")
            action { (x,c) => c.copy(csvFiles = x) } text("csv data files, separated by ','.")
        )
      note("Options:\n")
      opt[String]('h', "hdfsUrl") valueName("<hdfs url>") action { (x, c) =>
        c.copy(hdfsUrl = x) } text("hdfs url to parquet files, for example: hdfs://<host>:<port>/user/hive/poc")
      opt[String]('s', "sparkMaster") valueName("<spark master url>") action { (x, c) =>
        c.copy(sparkMaster = x) } text("spark master url, for example: spark://<host>:<port>")
      note("Others:\n")
      help("help") text("prints this usage text")
      opt[Unit]("verbose") action { (_, c) => c.copy(verbose = true) } text("enable verbose output.")
      opt[Unit]("debug") action { (_, c) => c.copy(debug = true) } text("enable debug mode.")
    }

    if (args.length == 0) {
      println(parser.usage)
      return null
    }
    // parser.parse returns Option[C]
    parser.parse(args, OptConfig()) match {
      case Some(config) =>
        if (config.mode == "") {
          println("[ERROR] Must specify a command: update/delete/insert!\n\n")
          println(parser.usage)
          return null
        }
        return config
      case None =>
        // arguments are bad, error message will have been displayed
    }    

    return null
  }

  def loadSchema : Array[String] = {
    val SCHEMA_STR = Array("wt_date","wt_id","c1","c2","c3","c4","c5","c6","c7","c8","c9","c10","c11","c12","c13","c14","c15","c16","c17","c18",
          "c19","c20","c21","c22","c23","c24","c25","c26","c27","c28","c29","c30","c31","c32","c33","c34","c35","c36","c37","c38","c39","c40",
          "c41","c42","c43","c44","c45","c46","c47","c48","c49","c50","c51","c52","c53","c54","c55","c56","c57","c58","c59","c60","c61","c62",
          "c63","c64","c65","c66","c67","c68","c69","c70","c71","c72","c73","c74","c75","c76","c77","c78","c79","c80","c81","c82","c83","c84",
          "c85","c86","c87","c88","c89","c90","c91","c92","c93","c94","c95","c96","c97","c98","c99","c100","c101","c102","c103","c104","c105",
          "c106","c107","c108","c109","c110","c111","c112","c113","c114","c115","c116","c117","c118","c119","c120","c121","c122","c123",
          "c124","c125","c126","c127","c128","c129","c130","c131","c132","c133","c134","c135","c136","c137","c138","c139","c140","c141",
          "c142","c143","c144","c145","c146","c147","c148","c149","c150","c151","c152","c153","c154","c155","c156","c157","c158","c159",
          "c160","c161","c162","c163","c164","c165","c166","c167","c168","c169","c170","c171","c172","c173","c174","c175","c176","c177",
          "c178","c179","c180","c181","c182","c183","c184","c185","c186","c187","c188","c189","c190","c191","c192","c193","c194","c195",
          "c196","c197","c198","c199","c200","c201","c202","c203","c204","c205","c206","c207","c208","c209","c210","c211","c212","c213",
          "c214","c215","c216","c217","c218","c219","c220","c221","c222","c223","c224","c225","c226","c227","c228","c229","c230","c231",
          "c232","c233","c234","c235","c236","c237","c238","c239","c240","c241","c242","c243","c244","c245","c246","c247","c248","c249",
          "c250","c251","c252","c253","c254","c255","c256","c257","c258","c259","c260","c261","c262","c263","c264","c265","c266","c267",
          "c268","c269","c270","c271","c272","c273","c274","c275","c276","c277","c278","c279","c280","c281","c282","c283","c284","c285",
          "c286","c287","c288","c289","c290","c291","c292","c293","c294","c295","c296","c297","c298","c299","c300","c301","c302","c303",
          "c304","c305","c306","c307","c308","c309","c310","c311","c312","c313","c314","c315","c316","c317","c318","c319","c320","c321",
          "c322","c323","c324","c325","c326","c327","c328","c329","c330","c331","c332","c333","c334","c335","c336","c337","c338","c339",
          "c340","c341","c342","c343","c344","c345","c346","c347","c348","c349","c350","c351","c352","c353","c354","c355","c356","c357",
          "c358","c359","c360","c361","c362","c363","c364","c365","c366","c367","c368","c369","c370","c371","c372","c373","c374","c375",
          "c376","c377","c378","c379","c380","c381","c382","c383","c384","c385","c386","c387","c388","c389","c390","c391","c392","c393",
          "c394","c395","c396","c397","c398","c399","c400","c401","c402","c403","c404","c405","c406","c407","c408","c409","c410","c411",
          "c412","c413","c414","c415","c416","c417","c418","c419","c420","c421","c422","c423","c424","c425","c426","c427","c428","c429",
          "c430","c431","c432","c433","c434","c435","c436","c437","c438","c439","c440","c441","c442","c443","c444","c445","c446","c447",
          "c448","c449","c450","c451","c452","c453","c454","c455","c456","c457","c458","c459","c460","c461","c462","c463","c464","c465",
          "c466","c467","c468","c469","c470","c471","c472","c473","c474","c475","c476","c477","c478","c479","c480","c481","c482","c483",
          "c484","c485","c486","c487","c488","c489","c490","c491","c492","c493","c494","c495","c496","c497","c498","c499","c500")
    return SCHEMA_STR
  }
  

  def main(args: Array[String]) {
    val optConfig = parseArgs(args)
    val schema = StructType(loadSchema.map(fieldName => StructField(fieldName, StringType, true)))

    val conf = new SparkConf().setAppName("ParquetDataUpdater")
    conf.setMaster(optConfig.sparkMaster)
    val sc = new SparkContext(conf)
    val hiveContext = new SQLContext(sc)

    val opts = Map("header" -> "false", "delimiter" -> ",")
    // Load poc csv data
    var df = hiveContext.read.format("parquet").options(opts).load(optConfig.hdfsUrl + "/*.parquet")
    var row_rdd = df.map(p => Row(p(0),p(1),p(3),p(4),p(6),p(7),p(8),p(9),p(10),p(11),
          p(12),p(13),p(14),p(15),p(16),p(17),p(18),p(19),p(20),p(21),p(22),p(23),p(24),
          p(25),p(26),p(27),p(28),p(29),p(30),p(31),p(32),p(33),p(34),p(35),p(36),p(37),
          p(38),p(39),p(40),p(41),p(42),p(43),p(44),p(45),p(46),p(47),p(48),p(49),p(50),
          p(51),p(52),p(53),p(54),p(55),p(56),p(57),p(58),p(59),p(60),p(61),p(62),p(63),
          p(64),p(65),p(66),p(67),p(68),p(69),p(70),p(71),p(72),p(73),p(74),p(75),p(76),
          p(77),p(78),p(79),p(80),p(81),p(82),p(83),p(84),p(85),p(86),p(87),p(88),p(89),
          p(90),p(91),p(92),p(93),p(94),p(95),p(96),p(97),p(98),p(99),p(100),p(101),p(102),
          p(103),p(104),p(105),p(106),p(107),p(108),p(109),p(110),p(111),p(112),p(113),p(114),
          p(115),p(116),p(117),p(118),p(119),p(120),p(121),p(122),p(123),p(124),p(125),p(126),
          p(127),p(128),p(129),p(130),p(131),p(132),p(133),p(134),p(135),p(136),p(137),p(138),
          p(139),p(140),p(141),p(142),p(143),p(144),p(145),p(146),p(147),p(148),p(149),p(150),
          p(151),p(152),p(153),p(154),p(155),p(156),p(157),p(158),p(159),p(160),p(161),p(162),
          p(163),p(164),p(165),p(166),p(167),p(168),p(169),p(170),p(171),p(172),p(173),p(174),
          p(175),p(176),p(177),p(178),p(179),p(180),p(181),p(182),p(183),p(184),p(185),p(186),
          p(187),p(188),p(189),p(190),p(191),p(192),p(193),p(194),p(195),p(196),p(197),p(198),
          p(199),p(200),p(201),p(202),p(203),p(204),p(205),p(206),p(207),p(208),p(209),p(210),
          p(211),p(212),p(213),p(214),p(215),p(216),p(217),p(218),p(219),p(220),p(221),p(222),
          p(223),p(224),p(225),p(226),p(227),p(228),p(229),p(230),p(231),p(232),p(233),p(234),
          p(235),p(236),p(237),p(238),p(239),p(240),p(241),p(242),p(243),p(244),p(245),p(246),
          p(247),p(248),p(249),p(250),p(251),p(252),p(253),p(254),p(255),p(256),p(257),p(258),
          p(259),p(260),p(261),p(262),p(263),p(264),p(265),p(266),p(267),p(268),p(269),p(270),
          p(271),p(272),p(273),p(274),p(275),p(276),p(277),p(278),p(279),p(280),p(281),p(282),
          p(283),p(284),p(285),p(286),p(287),p(288),p(289),p(290),p(291),p(292),p(293),p(294),
          p(295),p(296),p(297),p(298),p(299),p(300),p(301),p(302),p(303),p(304),p(305),p(306),
          p(307),p(308),p(309),p(310),p(311),p(312),p(313),p(314),p(315),p(316),p(317),p(318),
          p(319),p(320),p(321),p(322),p(323),p(324),p(325),p(326),p(327),p(328),p(329),p(330),
          p(331),p(332),p(333),p(334),p(335),p(336),p(337),p(338),p(339),p(340),p(341),p(342),
          p(343),p(344),p(345),p(346),p(347),p(348),p(349),p(350),p(351),p(352),p(353),p(354),
          p(355),p(356),p(357),p(358),p(359),p(360),p(361),p(362),p(363),p(364),p(365),p(366),
          p(367),p(368),p(369),p(370),p(371),p(372),p(373),p(374),p(375),p(376),p(377),p(378),
          p(379),p(380),p(381),p(382),p(383),p(384),p(385),p(386),p(387),p(388),p(389),p(390),
          p(391),p(392),p(393),p(394),p(395),p(396),p(397),p(398),p(399),p(400),p(401),p(402),
          p(403),p(404),p(405),p(406),p(407),p(408),p(409),p(410),p(411),p(412),p(413),p(414),
          p(415),p(416),p(417),p(418),p(419),p(420),p(421),p(422),p(423),p(424),p(425),p(426),
          p(427),p(428),p(429),p(430),p(431),p(432),p(433),p(434),p(435),p(436),p(437),p(438),
          p(439),p(440),p(441),p(442),p(443),p(444),p(445),p(446),p(447),p(448),p(449),p(450),
          p(451),p(452),p(453),p(454),p(455),p(456),p(457),p(458),p(459),p(460),p(461),p(462),
          p(463),p(464),p(465),p(466),p(467),p(468),p(469),p(470),p(471),p(472),p(473),p(474),
          p(475),p(476),p(477),p(478),p(479),p(480),p(481),p(482),p(483),p(484),p(485),p(486),
          p(487),p(488),p(489),p(490),p(491),p(492),p(493),p(494),p(495),p(496),p(497),p(498),
          p(499),p(500),p(501)))
    val pocDataFrame = hiveContext.createDataFrame(row_rdd, schema)

    pocDataFrame.registerTempTable("poc_update_table")
    val result = hiveContext.sql("select wt_id from poc_update_table where wt_date='2015-10-29 14:54:39'")
    result.map(t => "Name: " + t(0)).collect().foreach(println)
  }

}