package com.ibm.poc

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StructType,StructField,StringType,FloatType}
import org.apache.spark.sql.SaveMode
import java.util.Properties
import java.text.SimpleDateFormat
import java.util.Date
import java.util.Calendar
import java.sql.Timestamp
import java.sql.SQLException
import java.lang.Long
import org.apache.spark.rdd.RDD


object PocData {
  def createRDDFromMap(sc: SparkContext, list: List[Map[String, Nothing]]): RDD[Row] = {
    val rdd = sc.makeRDD(list)
    val row_rdd = rdd.map(p => Row(p("time"),p("ID"),p("K1"),p("K2"),p("K3"),p("K4"),p("K5"),p("K6"),p("K7"),p("K8"),p("K9"),p("K10"),p("K11"),p("K12"),
      p("K13"),p("K14"),p("K15"),p("K16"),p("K17"),p("K18"),p("K19"),p("K20"),p("K21"),p("K22"),p("K23"),p("K24"),p("K25"),p("K26"),p("K27"),
      p("K28"),p("K29"),p("K30"),p("K31"),p("K32"),p("K33"),p("K34"),p("K35"),p("K36"),p("K37"),p("K38"),p("K39"),p("K40"),p("K41"),p("K42"),
      p("K43"),p("K44"),p("K45"),p("K46"),p("K47"),p("K48"),p("K49"),p("K50"),p("K51"),p("K52"),p("K53"),p("K54"),p("K55"),p("K56"),p("K57"),
      p("K58"),p("K59"),p("K60"),p("K61"),p("K62"),p("K63"),p("K64"),p("K65"),p("K66"),p("K67"),p("K68"),p("K69"),p("K70"),p("K71"),p("K72"),
      p("K73"),p("K74"),p("K75"),p("K76"),p("K77"),p("K78"),p("K79"),p("K80"),p("K81"),p("K82"),p("K83"),p("K84"),p("K85"),p("K86"),p("K87"),
      p("K88"),p("K89"),p("K90"),p("K91"),p("K92"),p("K93"),p("K94"),p("K95"),p("K96"),p("K97"),p("K98"),p("K99"),p("K100"),p("K101"),p("K102"),
      p("K103"),p("K104"),p("K105"),p("K106"),p("K107"),p("K108"),p("K109"),p("K110"),p("K111"),p("K112"),p("K113"),p("K114"),p("K115"),p("K116"),
      p("K117"),p("K118"),p("K119"),p("K120"),p("K121"),p("K122"),p("K123"),p("K124"),p("K125"),p("K126"),p("K127"),p("K128"),p("K129"),p("K130"),
      p("K131"),p("K132"),p("K133"),p("K134"),p("K135"),p("K136"),p("K137"),p("K138"),p("K139"),p("K140"),p("K141"),p("K142"),p("K143"),p("K144"),
      p("K145"),p("K146"),p("K147"),p("K148"),p("K149"),p("K150"),p("K151"),p("K152"),p("K153"),p("K154"),p("K155"),p("K156"),p("K157"),p("K158"),
      p("K159"),p("K160"),p("K161"),p("K162"),p("K163"),p("K164"),p("K165"),p("K166"),p("K167"),p("K168"),p("K169"),p("K170"),p("K171"),p("K172"),
      p("K173"),p("K174"),p("K175"),p("K176"),p("K177"),p("K178"),p("K179"),p("K180"),p("K181"),p("K182"),p("K183"),p("K184"),p("K185"),p("K186"),
      p("K187"),p("K188"),p("K189"),p("K190"),p("K191"),p("K192"),p("K193"),p("K194"),p("K195"),p("K196"),p("K197"),p("K198"),p("K199"),p("K200"),
      p("K201"),p("K202"),p("K203"),p("K204"),p("K205"),p("K206"),p("K207"),p("K208"),p("K209"),p("K210"),p("K211"),p("K212"),p("K213"),p("K214"),
      p("K215"),p("K216"),p("K217"),p("K218"),p("K219"),p("K220"),p("K221"),p("K222"),p("K223"),p("K224"),p("K225"),p("K226"),p("K227"),p("K228"),
      p("K229"),p("K230"),p("K231"),p("K232"),p("K233"),p("K234"),p("K235"),p("K236"),p("K237"),p("K238"),p("K239"),p("K240"),p("K241"),p("K242"),
      p("K243"),p("K244"),p("K245"),p("K246"),p("K247"),p("K248"),p("K249"),p("K250"),p("K251"),p("K252"),p("K253"),p("K254"),p("K255"),p("K256"),
      p("K257"),p("K258"),p("K259"),p("K260"),p("K261"),p("K262"),p("K263"),p("K264"),p("K265"),p("K266"),p("K267"),p("K268"),p("K269"),p("K270"),
      p("K271"),p("K272"),p("K273"),p("K274"),p("K275"),p("K276"),p("K277"),p("K278"),p("K279"),p("K280"),p("K281"),p("K282"),p("K283"),p("K284"),
      p("K285"),p("K286"),p("K287"),p("K288"),p("K289"),p("K290"),p("K291"),p("K292"),p("K293"),p("K294"),p("K295"),p("K296"),p("K297"),p("K298"),
      p("K299"),p("K300"),p("K301"),p("K302"),p("K303"),p("K304"),p("K305"),p("K306"),p("K307"),p("K308"),p("K309"),p("K310"),p("K311"),p("K312"),
      p("K313"),p("K314"),p("K315"),p("K316"),p("K317"),p("K318"),p("K319"),p("K320"),p("K321"),p("K322"),p("K323"),p("K324"),p("K325"),p("K326"),
      p("K327"),p("K328"),p("K329"),p("K330"),p("K331"),p("K332"),p("K333"),p("K334"),p("K335"),p("K336"),p("K337"),p("K338"),p("K339"),p("K340"),
      p("K341"),p("K342"),p("K343"),p("K344"),p("K345"),p("K346"),p("K347"),p("K348"),p("K349"),p("K350"),p("TAG1"),p("TAG2"),p("TAG3"),p("TAG4"),
      p("TAG5"),p("TAG6"),p("TAG7"),p("TAG8"),p("TAG9"),p("TAG10"),p("TAG11"),p("TAG12"),p("TAG13"),p("TAG14"),p("TAG15"),p("TAG16"),p("TAG17"),
      p("TAG18"),p("TAG19"),p("TAG20"),p("TAG21"),p("TAG22"),p("TAG23"),p("TAG24"),p("TAG25"),p("TAG26"),p("TAG27"),p("TAG28"),p("TAG29"),p("TAG30"),
      p("TAG31"),p("TAG32"),p("TAG33"),p("TAG34"),p("TAG35"),p("TAG36"),p("TAG37"),p("TAG38"),p("TAG39"),p("TAG40"),p("TAG41"),p("TAG42"),p("TAG43"),
      p("TAG44"),p("TAG45"),p("TAG46"),p("TAG47"),p("TAG48"),p("TAG49"),p("TAG50"),p("TAG51"),p("TAG52"),p("TAG53"),p("TAG54"),p("TAG55"),p("TAG56"),
      p("TAG57"),p("TAG58"),p("TAG59"),p("TAG60"),p("TAG61"),p("TAG62"),p("TAG63"),p("TAG64"),p("TAG65"),p("TAG66"),p("TAG67"),p("TAG68"),p("TAG69"),
      p("TAG70"),p("TAG71"),p("TAG72"),p("TAG73"),p("TAG74"),p("TAG75"),p("TAG76"),p("TAG77"),p("TAG78"),p("TAG79"),p("TAG80"),p("TAG81"),p("TAG82"),
      p("TAG83"),p("TAG84"),p("TAG85"),p("TAG86"),p("TAG87"),p("TAG88"),p("TAG89"),p("TAG90"),p("TAG91"),p("TAG92"),p("TAG93"),p("TAG94"),p("TAG95"),
      p("TAG96"),p("TAG97"),p("TAG98"),p("TAG99"),p("TAG100"),p("TAG101"),p("TAG102"),p("TAG103"),p("TAG104"),p("TAG105"),p("TAG106"),p("TAG107"),
      p("TAG108"),p("TAG109"),p("TAG110"),p("TAG111"),p("TAG112"),p("TAG113"),p("TAG114"),p("TAG115"),p("TAG116"),p("TAG117"),p("TAG118"),p("TAG119"),
      p("TAG120"),p("TAG121"),p("TAG122"),p("TAG123"),p("TAG124"),p("TAG125"),p("TAG126"),p("TAG127"),p("TAG128"),p("TAG129"),p("TAG130"),p("TAG131"),
      p("TAG132"),p("TAG133"),p("TAG134"),p("TAG135"),p("TAG136"),p("TAG137"),p("TAG138"),p("TAG139"),p("TAG140"),p("TAG141"),p("TAG142"),p("TAG143"),
      p("TAG144"),p("TAG145"),p("TAG146"),p("TAG147"),p("TAG148"),p("TAG149"),p("TAG150"),p("CAL")))
    return row_rdd
  }

  def createRDD(df: DataFrame): RDD[Row] = {
      val row_rdd = df.map(p => Row(p(0).toString(), p(1).toString(), p(2).toString().toFloat, p(3).toString().toFloat,
           p(4).toString().toFloat, p(5).toString().toFloat, p(6).toString().toFloat, p(7).toString().toFloat, p(8).toString().toFloat,
           p(9).toString().toFloat, p(10).toString().toFloat, p(11).toString().toFloat, p(12).toString().toFloat, p(13).toString().toFloat,
           p(14).toString().toFloat, p(15).toString().toFloat, p(16).toString().toFloat, p(17).toString().toFloat, p(18).toString().toFloat,
           p(19).toString().toFloat, p(20).toString().toFloat, p(21).toString().toFloat, p(22).toString().toFloat, p(23).toString().toFloat,
           p(24).toString().toFloat, p(25).toString().toFloat, p(26).toString().toFloat, p(27).toString().toFloat, p(28).toString().toFloat,
           p(29).toString().toFloat, p(30).toString().toFloat, p(31).toString().toFloat, p(32).toString().toFloat, p(33).toString().toFloat,
           p(34).toString().toFloat, p(35).toString().toFloat, p(36).toString().toFloat, p(37).toString().toFloat, p(38).toString().toFloat,
           p(39).toString().toFloat, p(40).toString().toFloat, p(41).toString().toFloat, p(42).toString().toFloat, p(43).toString().toFloat,
           p(44).toString().toFloat, p(45).toString().toFloat, p(46).toString().toFloat, p(47).toString().toFloat, p(48).toString().toFloat,
           p(49).toString().toFloat, p(50).toString().toFloat, p(51).toString().toFloat, p(52).toString().toFloat, p(53).toString().toFloat,
           p(54).toString().toFloat, p(55).toString().toFloat, p(56).toString().toFloat, p(57).toString().toFloat, p(58).toString().toFloat,
           p(59).toString().toFloat, p(60).toString().toFloat, p(61).toString().toFloat, p(62).toString().toFloat, p(63).toString().toFloat,
           p(64).toString().toFloat, p(65).toString().toFloat, p(66).toString().toFloat, p(67).toString().toFloat, p(68).toString().toFloat,
           p(69).toString().toFloat, p(70).toString().toFloat, p(71).toString().toFloat, p(72).toString().toFloat, p(73).toString().toFloat,
           p(74).toString().toFloat, p(75).toString().toFloat, p(76).toString().toFloat, p(77).toString().toFloat, p(78).toString().toFloat,
           p(79).toString().toFloat, p(80).toString().toFloat, p(81).toString().toFloat, p(82).toString().toFloat, p(83).toString().toFloat,
           p(84).toString().toFloat, p(85).toString().toFloat, p(86).toString().toFloat, p(87).toString().toFloat, p(88).toString().toFloat,
           p(89).toString().toFloat, p(90).toString().toFloat, p(91).toString().toFloat, p(92).toString().toFloat, p(93).toString().toFloat,
           p(94).toString().toFloat, p(95).toString().toFloat, p(96).toString().toFloat, p(97).toString().toFloat, p(98).toString().toFloat,
           p(99).toString().toFloat, p(100).toString().toFloat, p(101).toString().toFloat, p(102).toString().toFloat, p(103).toString().toFloat,
           p(104).toString().toFloat, p(105).toString().toFloat, p(106).toString().toFloat, p(107).toString().toFloat, p(108).toString().toFloat,
           p(109).toString().toFloat, p(110).toString().toFloat, p(111).toString().toFloat, p(112).toString().toFloat, p(113).toString().toFloat,
           p(114).toString().toFloat, p(115).toString().toFloat, p(116).toString().toFloat, p(117).toString().toFloat, p(118).toString().toFloat,
           p(119).toString().toFloat, p(120).toString().toFloat, p(121).toString().toFloat, p(122).toString().toFloat, p(123).toString().toFloat,
           p(124).toString().toFloat, p(125).toString().toFloat, p(126).toString().toFloat, p(127).toString().toFloat, p(128).toString().toFloat,
           p(129).toString().toFloat, p(130).toString().toFloat, p(131).toString().toFloat, p(132).toString().toFloat, p(133).toString().toFloat,
           p(134).toString().toFloat, p(135).toString().toFloat, p(136).toString().toFloat, p(137).toString().toFloat, p(138).toString().toFloat,
           p(139).toString().toFloat, p(140).toString().toFloat, p(141).toString().toFloat, p(142).toString().toFloat, p(143).toString().toFloat,
           p(144).toString().toFloat, p(145).toString().toFloat, p(146).toString().toFloat, p(147).toString().toFloat, p(148).toString().toFloat,
           p(149).toString().toFloat, p(150).toString().toFloat, p(151).toString().toFloat, p(152).toString().toFloat, p(153).toString().toFloat,
           p(154).toString().toFloat, p(155).toString().toFloat, p(156).toString().toFloat, p(157).toString().toFloat, p(158).toString().toFloat,
           p(159).toString().toFloat, p(160).toString().toFloat, p(161).toString().toFloat, p(162).toString().toFloat, p(163).toString().toFloat,
           p(164).toString().toFloat, p(165).toString().toFloat, p(166).toString().toFloat, p(167).toString().toFloat, p(168).toString().toFloat,
           p(169).toString().toFloat, p(170).toString().toFloat, p(171).toString().toFloat, p(172).toString().toFloat, p(173).toString().toFloat,
           p(174).toString().toFloat, p(175).toString().toFloat, p(176).toString().toFloat, p(177).toString().toFloat, p(178).toString().toFloat,
           p(179).toString().toFloat, p(180).toString().toFloat, p(181).toString().toFloat, p(182).toString().toFloat, p(183).toString().toFloat,
           p(184).toString().toFloat, p(185).toString().toFloat, p(186).toString().toFloat, p(187).toString().toFloat, p(188).toString().toFloat,
           p(189).toString().toFloat, p(190).toString().toFloat, p(191).toString().toFloat, p(192).toString().toFloat, p(193).toString().toFloat,
           p(194).toString().toFloat, p(195).toString().toFloat, p(196).toString().toFloat, p(197).toString().toFloat, p(198).toString().toFloat,
           p(199).toString().toFloat, p(200).toString().toFloat, p(201).toString().toFloat, p(202).toString().toFloat, p(203).toString().toFloat,
           p(204).toString().toFloat, p(205).toString().toFloat, p(206).toString().toFloat, p(207).toString().toFloat, p(208).toString().toFloat,
           p(209).toString().toFloat, p(210).toString().toFloat, p(211).toString().toFloat, p(212).toString().toFloat, p(213).toString().toFloat,
           p(214).toString().toFloat, p(215).toString().toFloat, p(216).toString().toFloat, p(217).toString().toFloat, p(218).toString().toFloat,
           p(219).toString().toFloat, p(220).toString().toFloat, p(221).toString().toFloat, p(222).toString().toFloat, p(223).toString().toFloat,
           p(224).toString().toFloat, p(225).toString().toFloat, p(226).toString().toFloat, p(227).toString().toFloat, p(228).toString().toFloat,
           p(229).toString().toFloat, p(230).toString().toFloat, p(231).toString().toFloat, p(232).toString().toFloat, p(233).toString().toFloat,
           p(234).toString().toFloat, p(235).toString().toFloat, p(236).toString().toFloat, p(237).toString().toFloat, p(238).toString().toFloat,
           p(239).toString().toFloat, p(240).toString().toFloat, p(241).toString().toFloat, p(242).toString().toFloat, p(243).toString().toFloat,
           p(244).toString().toFloat, p(245).toString().toFloat, p(246).toString().toFloat, p(247).toString().toFloat, p(248).toString().toFloat,
           p(249).toString().toFloat, p(250).toString().toFloat, p(251).toString().toFloat, p(252).toString().toFloat, p(253).toString().toFloat,
           p(254).toString().toFloat, p(255).toString().toFloat, p(256).toString().toFloat, p(257).toString().toFloat, p(258).toString().toFloat,
           p(259).toString().toFloat, p(260).toString().toFloat, p(261).toString().toFloat, p(262).toString().toFloat, p(263).toString().toFloat,
           p(264).toString().toFloat, p(265).toString().toFloat, p(266).toString().toFloat, p(267).toString().toFloat, p(268).toString().toFloat,
           p(269).toString().toFloat, p(270).toString().toFloat, p(271).toString().toFloat, p(272).toString().toFloat, p(273).toString().toFloat,
           p(274).toString().toFloat, p(275).toString().toFloat, p(276).toString().toFloat, p(277).toString().toFloat, p(278).toString().toFloat,
           p(279).toString().toFloat, p(280).toString().toFloat, p(281).toString().toFloat, p(282).toString().toFloat, p(283).toString().toFloat,
           p(284).toString().toFloat, p(285).toString().toFloat, p(286).toString().toFloat, p(287).toString().toFloat, p(288).toString().toFloat,
           p(289).toString().toFloat, p(290).toString().toFloat, p(291).toString().toFloat, p(292).toString().toFloat, p(293).toString().toFloat,
           p(294).toString().toFloat, p(295).toString().toFloat, p(296).toString().toFloat, p(297).toString().toFloat, p(298).toString().toFloat,
           p(299).toString().toFloat, p(300).toString().toFloat, p(301).toString().toFloat, p(302).toString().toFloat, p(303).toString().toFloat,
           p(304).toString().toFloat, p(305).toString().toFloat, p(306).toString().toFloat, p(307).toString().toFloat, p(308).toString().toFloat,
           p(309).toString().toFloat, p(310).toString().toFloat, p(311).toString().toFloat, p(312).toString().toFloat, p(313).toString().toFloat,
           p(314).toString().toFloat, p(315).toString().toFloat, p(316).toString().toFloat, p(317).toString().toFloat, p(318).toString().toFloat,
           p(319).toString().toFloat, p(320).toString().toFloat, p(321).toString().toFloat, p(322).toString().toFloat, p(323).toString().toFloat,
           p(324).toString().toFloat, p(325).toString().toFloat, p(326).toString().toFloat, p(327).toString().toFloat, p(328).toString().toFloat,
           p(329).toString().toFloat, p(330).toString().toFloat, p(331).toString().toFloat, p(332).toString().toFloat, p(333).toString().toFloat,
           p(334).toString().toFloat, p(335).toString().toFloat, p(336).toString().toFloat, p(337).toString().toFloat, p(338).toString().toFloat,
           p(339).toString().toFloat, p(340).toString().toFloat, p(341).toString().toFloat, p(342).toString().toFloat, p(343).toString().toFloat,
           p(344).toString().toFloat, p(345).toString().toFloat, p(346).toString().toFloat, p(347).toString().toFloat, p(348).toString().toFloat,
           p(349).toString().toFloat, p(350).toString().toFloat, p(351).toString().toFloat, p(352).toString().toFloat, p(353).toString().toFloat,
           p(354).toString().toFloat, p(355).toString().toFloat, p(356).toString().toFloat, p(357).toString().toFloat, p(358).toString().toFloat,
           p(359).toString().toFloat, p(360).toString().toFloat, p(361).toString().toFloat, p(362).toString().toFloat, p(363).toString().toFloat,
           p(364).toString().toFloat, p(365).toString().toFloat, p(366).toString().toFloat, p(367).toString().toFloat, p(368).toString().toFloat,
           p(369).toString().toFloat, p(370).toString().toFloat, p(371).toString().toFloat, p(372).toString().toFloat, p(373).toString().toFloat,
           p(374).toString().toFloat, p(375).toString().toFloat, p(376).toString().toFloat, p(377).toString().toFloat, p(378).toString().toFloat,
           p(379).toString().toFloat, p(380).toString().toFloat, p(381).toString().toFloat, p(382).toString().toFloat, p(383).toString().toFloat,
           p(384).toString().toFloat, p(385).toString().toFloat, p(386).toString().toFloat, p(387).toString().toFloat, p(388).toString().toFloat,
           p(389).toString().toFloat, p(390).toString().toFloat, p(391).toString().toFloat, p(392).toString().toFloat, p(393).toString().toFloat,
           p(394).toString().toFloat, p(395).toString().toFloat, p(396).toString().toFloat, p(397).toString().toFloat, p(398).toString().toFloat,
           p(399).toString().toFloat, p(400).toString().toFloat, p(401).toString().toFloat, p(402).toString().toFloat, p(403).toString().toFloat,
           p(404).toString().toFloat, p(405).toString().toFloat, p(406).toString().toFloat, p(407).toString().toFloat, p(408).toString().toFloat,
           p(409).toString().toFloat, p(410).toString().toFloat, p(411).toString().toFloat, p(412).toString().toFloat, p(413).toString().toFloat,
           p(414).toString().toFloat, p(415).toString().toFloat, p(416).toString().toFloat, p(417).toString().toFloat, p(418).toString().toFloat,
           p(419).toString().toFloat, p(420).toString().toFloat, p(421).toString().toFloat, p(422).toString().toFloat, p(423).toString().toFloat,
           p(424).toString().toFloat, p(425).toString().toFloat, p(426).toString().toFloat, p(427).toString().toFloat, p(428).toString().toFloat, 
           p(429).toString().toFloat, p(430).toString().toFloat, p(431).toString().toFloat, p(432).toString().toFloat, p(433).toString().toFloat,
           p(434).toString().toFloat, p(435).toString().toFloat, p(436).toString().toFloat, p(437).toString().toFloat, p(438).toString().toFloat,
           p(439).toString().toFloat, p(440).toString().toFloat, p(441).toString().toFloat, p(442).toString().toFloat, p(443).toString().toFloat,
           p(444).toString().toFloat, p(445).toString().toFloat, p(446).toString().toFloat, p(447).toString().toFloat, p(448).toString().toFloat,
           p(449).toString().toFloat, p(450).toString().toFloat, p(451).toString().toFloat, p(452).toString().toFloat, p(453).toString().toFloat,
           p(454).toString().toFloat, p(455).toString().toFloat, p(456).toString().toFloat, p(457).toString().toFloat, p(458).toString().toFloat,
           p(459).toString().toFloat, p(460).toString().toFloat, p(461).toString().toFloat, p(462).toString().toFloat, p(463).toString().toFloat,
           p(464).toString().toFloat, p(465).toString().toFloat, p(466).toString().toFloat, p(467).toString().toFloat, p(468).toString().toFloat,
           p(469).toString().toFloat, p(470).toString().toFloat, p(471).toString().toFloat, p(472).toString().toFloat, p(473).toString().toFloat,
           p(474).toString().toFloat, p(475).toString().toFloat, p(476).toString().toFloat, p(477).toString().toFloat, p(478).toString().toFloat,
           p(479).toString().toFloat, p(480).toString().toFloat, p(481).toString().toFloat, p(482).toString().toFloat, p(483).toString().toFloat,
           p(484).toString().toFloat, p(485).toString().toFloat, p(486).toString().toFloat, p(487).toString().toFloat, p(488).toString().toFloat,
           p(489).toString().toFloat, p(490).toString().toFloat, p(491).toString().toFloat, p(492).toString().toFloat, p(493).toString().toFloat,
           p(494).toString().toFloat, p(495).toString().toFloat, p(496).toString().toFloat, p(497).toString().toFloat, p(498).toString().toFloat,
           p(499).toString().toFloat, p(500).toString().toFloat, p(501).toString().toFloat))
    return row_rdd

  }

  def dataSchema: StructType = {
      val st =  StructType(
        StructField("TIME", StringType, false) ::
        StructField("ID", StringType, false) ::
        StructField("K1", IntType, false) ::
        StructField("K2", IntType, false) ::
        StructField("K3", IntType, false) ::
        StructField("K4", IntType, false) ::
        StructField("K5", IntType, false) ::
        StructField("K6", IntType, false) ::
        StructField("K7", IntType, false) ::
        StructField("K8", IntType, false) ::
        StructField("K9", IntType, false) ::
        StructField("K10", IntType, false) ::
        StructField("K11", IntType, false) ::
        StructField("K12", IntType, false) ::
        StructField("K13", IntType, false) ::
        StructField("K14", IntType, false) ::
        StructField("K15", IntType, false) ::
        StructField("K16", IntType, false) ::
        StructField("K17", IntType, false) ::
        StructField("K18", IntType, false) ::
        StructField("K19", IntType, false) ::
        StructField("K20", IntType, false) ::
        StructField("K21", IntType, false) ::
        StructField("K22", IntType, false) ::
        StructField("K23", IntType, false) ::
        StructField("K24", IntType, false) ::
        StructField("K25", IntType, false) ::
        StructField("K26", IntType, false) ::
        StructField("K27", IntType, false) ::
        StructField("K28", IntType, false) ::
        StructField("K29", IntType, false) ::
        StructField("K30", IntType, false) ::
        StructField("K31", IntType, false) ::
        StructField("K32", IntType, false) ::
        StructField("K33", IntType, false) ::
        StructField("K34", IntType, false) ::
        StructField("K35", IntType, false) ::
        StructField("K36", IntType, false) ::
        StructField("K37", IntType, false) ::
        StructField("K38", IntType, false) ::
        StructField("K39", IntType, false) ::
        StructField("K40", IntType, false) ::
        StructField("K41", IntType, false) ::
        StructField("K42", IntType, false) ::
        StructField("K43", IntType, false) ::
        StructField("K44", IntType, false) ::
        StructField("K45", IntType, false) ::
        StructField("K46", IntType, false) ::
        StructField("K47", IntType, false) ::
        StructField("K48", IntType, false) ::
        StructField("K49", IntType, false) ::
        StructField("K50", IntType, false) ::
        StructField("K51", IntType, false) ::
        StructField("K52", IntType, false) ::
        StructField("K53", IntType, false) ::
        StructField("K54", IntType, false) ::
        StructField("K55", IntType, false) ::
        StructField("K56", IntType, false) ::
        StructField("K57", IntType, false) ::
        StructField("K58", IntType, false) ::
        StructField("K59", IntType, false) ::
        StructField("K60", IntType, false) ::
        StructField("K61", IntType, false) ::
        StructField("K62", IntType, false) ::
        StructField("K63", IntType, false) ::
        StructField("K64", IntType, false) ::
        StructField("K65", IntType, false) ::
        StructField("K66", IntType, false) ::
        StructField("K67", IntType, false) ::
        StructField("K68", IntType, false) ::
        StructField("K69", IntType, false) ::
        StructField("K70", IntType, false) ::
        StructField("K71", IntType, false) ::
        StructField("K72", IntType, false) ::
        StructField("K73", IntType, false) ::
        StructField("K74", IntType, false) ::
        StructField("K75", IntType, false) ::
        StructField("K76", IntType, false) ::
        StructField("K77", IntType, false) ::
        StructField("K78", IntType, false) ::
        StructField("K79", IntType, false) ::
        StructField("K80", IntType, false) ::
        StructField("K81", IntType, false) ::
        StructField("K82", IntType, false) ::
        StructField("K83", IntType, false) ::
        StructField("K84", IntType, false) ::
        StructField("K85", IntType, false) ::
        StructField("K86", IntType, false) ::
        StructField("K87", IntType, false) ::
        StructField("K88", IntType, false) ::
        StructField("K89", IntType, false) ::
        StructField("K90", IntType, false) ::
        StructField("K91", IntType, false) ::
        StructField("K92", IntType, false) ::
        StructField("K93", IntType, false) ::
        StructField("K94", IntType, false) ::
        StructField("K95", IntType, false) ::
        StructField("K96", IntType, false) ::
        StructField("K97", IntType, false) ::
        StructField("K98", IntType, false) ::
        StructField("K99", IntType, false) ::
        StructField("K100", IntType, false) ::
        StructField("K101", IntType, false) ::
        StructField("K102", IntType, false) ::
        StructField("K103", IntType, false) ::
        StructField("K104", IntType, false) ::
        StructField("K105", IntType, false) ::
        StructField("K106", IntType, false) ::
        StructField("K107", IntType, false) ::
        StructField("K108", IntType, false) ::
        StructField("K109", IntType, false) ::
        StructField("K110", IntType, false) ::
        StructField("K111", IntType, false) ::
        StructField("K112", IntType, false) ::
        StructField("K113", IntType, false) ::
        StructField("K114", IntType, false) ::
        StructField("K115", IntType, false) ::
        StructField("K116", IntType, false) ::
        StructField("K117", IntType, false) ::
        StructField("K118", IntType, false) ::
        StructField("K119", IntType, false) ::
        StructField("K120", IntType, false) ::
        StructField("K121", IntType, false) ::
        StructField("K122", IntType, false) ::
        StructField("K123", IntType, false) ::
        StructField("K124", IntType, false) ::
        StructField("K125", IntType, false) ::
        StructField("K126", IntType, false) ::
        StructField("K127", IntType, false) ::
        StructField("K128", IntType, false) ::
        StructField("K129", IntType, false) ::
        StructField("K130", IntType, false) ::
        StructField("K131", IntType, false) ::
        StructField("K132", IntType, false) ::
        StructField("K133", IntType, false) ::
        StructField("K134", IntType, false) ::
        StructField("K135", IntType, false) ::
        StructField("K136", IntType, false) ::
        StructField("K137", IntType, false) ::
        StructField("K138", IntType, false) ::
        StructField("K139", IntType, false) ::
        StructField("K140", IntType, false) ::
        StructField("K141", IntType, false) ::
        StructField("K142", IntType, false) ::
        StructField("K143", IntType, false) ::
        StructField("K144", IntType, false) ::
        StructField("K145", IntType, false) ::
        StructField("K146", IntType, false) ::
        StructField("K147", IntType, false) ::
        StructField("K148", IntType, false) ::
        StructField("K149", IntType, false) ::
        StructField("K150", IntType, false) ::
        StructField("K151", IntType, false) ::
        StructField("K152", IntType, false) ::
        StructField("K153", IntType, false) ::
        StructField("K154", IntType, false) ::
        StructField("K155", IntType, false) ::
        StructField("K156", IntType, false) ::
        StructField("K157", IntType, false) ::
        StructField("K158", IntType, false) ::
        StructField("K159", IntType, false) ::
        StructField("K160", IntType, false) ::
        StructField("K161", IntType, false) ::
        StructField("K162", IntType, false) ::
        StructField("K163", IntType, false) ::
        StructField("K164", IntType, false) ::
        StructField("K165", IntType, false) ::
        StructField("K166", IntType, false) ::
        StructField("K167", IntType, false) ::
        StructField("K168", IntType, false) ::
        StructField("K169", IntType, false) ::
        StructField("K170", IntType, false) ::
        StructField("K171", IntType, false) ::
        StructField("K172", IntType, false) ::
        StructField("K173", IntType, false) ::
        StructField("K174", IntType, false) ::
        StructField("K175", IntType, false) ::
        StructField("K176", IntType, false) ::
        StructField("K177", IntType, false) ::
        StructField("K178", IntType, false) ::
        StructField("K179", IntType, false) ::
        StructField("K180", IntType, false) ::
        StructField("K181", IntType, false) ::
        StructField("K182", IntType, false) ::
        StructField("K183", IntType, false) ::
        StructField("K184", IntType, false) ::
        StructField("K185", IntType, false) ::
        StructField("K186", IntType, false) ::
        StructField("K187", IntType, false) ::
        StructField("K188", IntType, false) ::
        StructField("K189", IntType, false) ::
        StructField("K190", IntType, false) ::
        StructField("K191", IntType, false) ::
        StructField("K192", IntType, false) ::
        StructField("K193", IntType, false) ::
        StructField("K194", IntType, false) ::
        StructField("K195", IntType, false) ::
        StructField("K196", IntType, false) ::
        StructField("K197", IntType, false) ::
        StructField("K198", IntType, false) ::
        StructField("K199", IntType, false) ::
        StructField("K200", IntType, false) ::
        StructField("K201", IntType, false) ::
        StructField("K202", IntType, false) ::
        StructField("K203", IntType, false) ::
        StructField("K204", IntType, false) ::
        StructField("K205", IntType, false) ::
        StructField("K206", IntType, false) ::
        StructField("K207", IntType, false) ::
        StructField("K208", IntType, false) ::
        StructField("K209", IntType, false) ::
        StructField("K210", IntType, false) ::
        StructField("K211", IntType, false) ::
        StructField("K212", IntType, false) ::
        StructField("K213", IntType, false) ::
        StructField("K214", IntType, false) ::
        StructField("K215", IntType, false) ::
        StructField("K216", IntType, false) ::
        StructField("K217", IntType, false) ::
        StructField("K218", IntType, false) ::
        StructField("K219", IntType, false) ::
        StructField("K220", IntType, false) ::
        StructField("K221", IntType, false) ::
        StructField("K222", IntType, false) ::
        StructField("K223", IntType, false) ::
        StructField("K224", IntType, false) ::
        StructField("K225", IntType, false) ::
        StructField("K226", IntType, false) ::
        StructField("K227", IntType, false) ::
        StructField("K228", IntType, false) ::
        StructField("K229", IntType, false) ::
        StructField("K230", IntType, false) ::
        StructField("K231", IntType, false) ::
        StructField("K232", IntType, false) ::
        StructField("K233", IntType, false) ::
        StructField("K234", IntType, false) ::
        StructField("K235", IntType, false) ::
        StructField("K236", IntType, false) ::
        StructField("K237", IntType, false) ::
        StructField("K238", IntType, false) ::
        StructField("K239", IntType, false) ::
        StructField("K240", IntType, false) ::
        StructField("K241", IntType, false) ::
        StructField("K242", IntType, false) ::
        StructField("K243", IntType, false) ::
        StructField("K244", IntType, false) ::
        StructField("K245", IntType, false) ::
        StructField("K246", IntType, false) ::
        StructField("K247", IntType, false) ::
        StructField("K248", IntType, false) ::
        StructField("K249", IntType, false) ::
        StructField("K250", IntType, false) ::
        StructField("K251", IntType, false) ::
        StructField("K252", IntType, false) ::
        StructField("K253", IntType, false) ::
        StructField("K254", IntType, false) ::
        StructField("K255", IntType, false) ::
        StructField("K256", IntType, false) ::
        StructField("K257", IntType, false) ::
        StructField("K258", IntType, false) ::
        StructField("K259", IntType, false) ::
        StructField("K260", IntType, false) ::
        StructField("K261", IntType, false) ::
        StructField("K262", IntType, false) ::
        StructField("K263", IntType, false) ::
        StructField("K264", IntType, false) ::
        StructField("K265", IntType, false) ::
        StructField("K266", IntType, false) ::
        StructField("K267", IntType, false) ::
        StructField("K268", IntType, false) ::
        StructField("K269", IntType, false) ::
        StructField("K270", IntType, false) ::
        StructField("K271", IntType, false) ::
        StructField("K272", IntType, false) ::
        StructField("K273", IntType, false) ::
        StructField("K274", IntType, false) ::
        StructField("K275", IntType, false) ::
        StructField("K276", IntType, false) ::
        StructField("K277", IntType, false) ::
        StructField("K278", IntType, false) ::
        StructField("K279", IntType, false) ::
        StructField("K280", IntType, false) ::
        StructField("K281", IntType, false) ::
        StructField("K282", IntType, false) ::
        StructField("K283", IntType, false) ::
        StructField("K284", IntType, false) ::
        StructField("K285", IntType, false) ::
        StructField("K286", IntType, false) ::
        StructField("K287", IntType, false) ::
        StructField("K288", IntType, false) ::
        StructField("K289", IntType, false) ::
        StructField("K290", IntType, false) ::
        StructField("K291", IntType, false) ::
        StructField("K292", IntType, false) ::
        StructField("K293", IntType, false) ::
        StructField("K294", IntType, false) ::
        StructField("K295", IntType, false) ::
        StructField("K296", IntType, false) ::
        StructField("K297", IntType, false) ::
        StructField("K298", IntType, false) ::
        StructField("K299", IntType, false) ::
        StructField("K300", IntType, false) ::
        StructField("K301", IntType, false) ::
        StructField("K302", IntType, false) ::
        StructField("K303", IntType, false) ::
        StructField("K304", IntType, false) ::
        StructField("K305", IntType, false) ::
        StructField("K306", IntType, false) ::
        StructField("K307", IntType, false) ::
        StructField("K308", IntType, false) ::
        StructField("K309", IntType, false) ::
        StructField("K310", IntType, false) ::
        StructField("K311", IntType, false) ::
        StructField("K312", IntType, false) ::
        StructField("K313", IntType, false) ::
        StructField("K314", IntType, false) ::
        StructField("K315", IntType, false) ::
        StructField("K316", IntType, false) ::
        StructField("K317", IntType, false) ::
        StructField("K318", IntType, false) ::
        StructField("K319", IntType, false) ::
        StructField("K320", IntType, false) ::
        StructField("K321", IntType, false) ::
        StructField("K322", IntType, false) ::
        StructField("K323", IntType, false) ::
        StructField("K324", IntType, false) ::
        StructField("K325", IntType, false) ::
        StructField("K326", IntType, false) ::
        StructField("K327", IntType, false) ::
        StructField("K328", IntType, false) ::
        StructField("K329", IntType, false) ::
        StructField("K330", IntType, false) ::
        StructField("K331", IntType, false) ::
        StructField("K332", IntType, false) ::
        StructField("K333", IntType, false) ::
        StructField("K334", IntType, false) ::
        StructField("K335", IntType, false) ::
        StructField("K336", IntType, false) ::
        StructField("K337", IntType, false) ::
        StructField("K338", IntType, false) ::
        StructField("K339", IntType, false) ::
        StructField("K340", IntType, false) ::
        StructField("K341", IntType, false) ::
        StructField("K342", IntType, false) ::
        StructField("K343", IntType, false) ::
        StructField("K344", IntType, false) ::
        StructField("K345", IntType, false) ::
        StructField("K346", IntType, false) ::
        StructField("K347", IntType, false) ::
        StructField("K348", IntType, false) ::
        StructField("K349", IntType, false) ::
        StructField("TAG1", FloatType, false) ::
        StructField("TAG2", FloatType, false) ::
        StructField("TAG3", FloatType, false) ::
        StructField("TAG4", FloatType, false) ::
        StructField("TAG5", FloatType, false) ::
        StructField("TAG6", FloatType, false) ::
        StructField("TAG7", FloatType, false) ::
        StructField("TAG8", FloatType, false) ::
        StructField("TAG9", FloatType, false) ::
        StructField("TAG10", FloatType, false) ::
        StructField("TAG11", FloatType, false) ::
        StructField("TAG12", FloatType, false) ::
        StructField("TAG13", FloatType, false) ::
        StructField("TAG14", FloatType, false) ::
        StructField("TAG15", FloatType, false) ::
        StructField("TAG16", FloatType, false) ::
        StructField("TAG17", FloatType, false) ::
        StructField("TAG18", FloatType, false) ::
        StructField("TAG19", FloatType, false) ::
        StructField("TAG20", FloatType, false) ::
        StructField("TAG21", FloatType, false) ::
        StructField("TAG22", FloatType, false) ::
        StructField("TAG23", FloatType, false) ::
        StructField("TAG24", FloatType, false) ::
        StructField("TAG25", FloatType, false) ::
        StructField("TAG26", FloatType, false) ::
        StructField("TAG27", FloatType, false) ::
        StructField("TAG28", FloatType, false) ::
        StructField("TAG29", FloatType, false) ::
        StructField("TAG30", FloatType, false) ::
        StructField("TAG31", FloatType, false) ::
        StructField("TAG32", FloatType, false) ::
        StructField("TAG33", FloatType, false) ::
        StructField("TAG34", FloatType, false) ::
        StructField("TAG35", FloatType, false) ::
        StructField("TAG36", FloatType, false) ::
        StructField("TAG37", FloatType, false) ::
        StructField("TAG38", FloatType, false) ::
        StructField("TAG39", FloatType, false) ::
        StructField("TAG40", FloatType, false) ::
        StructField("TAG41", FloatType, false) ::
        StructField("TAG42", FloatType, false) ::
        StructField("TAG43", FloatType, false) ::
        StructField("TAG44", FloatType, false) ::
        StructField("TAG45", FloatType, false) ::
        StructField("TAG46", FloatType, false) ::
        StructField("TAG47", FloatType, false) ::
        StructField("TAG48", FloatType, false) ::
        StructField("TAG49", FloatType, false) ::
        StructField("TAG50", FloatType, false) ::
        StructField("TAG51", FloatType, false) ::
        StructField("TAG52", FloatType, false) ::
        StructField("TAG53", FloatType, false) ::
        StructField("TAG54", FloatType, false) ::
        StructField("TAG55", FloatType, false) ::
        StructField("TAG56", FloatType, false) ::
        StructField("TAG57", FloatType, false) ::
        StructField("TAG58", FloatType, false) ::
        StructField("TAG59", FloatType, false) ::
        StructField("TAG60", FloatType, false) ::
        StructField("TAG61", FloatType, false) ::
        StructField("TAG62", FloatType, false) ::
        StructField("TAG63", FloatType, false) ::
        StructField("TAG64", FloatType, false) ::
        StructField("TAG65", FloatType, false) ::
        StructField("TAG66", FloatType, false) ::
        StructField("TAG67", FloatType, false) ::
        StructField("TAG68", FloatType, false) ::
        StructField("TAG69", FloatType, false) ::
        StructField("TAG70", FloatType, false) ::
        StructField("TAG71", FloatType, false) ::
        StructField("TAG72", FloatType, false) ::
        StructField("TAG73", FloatType, false) ::
        StructField("TAG74", FloatType, false) ::
        StructField("TAG75", FloatType, false) ::
        StructField("TAG76", FloatType, false) ::
        StructField("TAG77", FloatType, false) ::
        StructField("TAG78", FloatType, false) ::
        StructField("TAG79", FloatType, false) ::
        StructField("TAG80", FloatType, false) ::
        StructField("TAG81", FloatType, false) ::
        StructField("TAG82", FloatType, false) ::
        StructField("TAG83", FloatType, false) ::
        StructField("TAG84", FloatType, false) ::
        StructField("TAG85", FloatType, false) ::
        StructField("TAG86", FloatType, false) ::
        StructField("TAG87", FloatType, false) ::
        StructField("TAG88", FloatType, false) ::
        StructField("TAG89", FloatType, false) ::
        StructField("TAG90", FloatType, false) ::
        StructField("TAG91", FloatType, false) ::
        StructField("TAG92", FloatType, false) ::
        StructField("TAG93", FloatType, false) ::
        StructField("TAG94", FloatType, false) ::
        StructField("TAG95", FloatType, false) ::
        StructField("TAG96", FloatType, false) ::
        StructField("TAG97", FloatType, false) ::
        StructField("TAG98", FloatType, false) ::
        StructField("TAG99", FloatType, false) ::
        StructField("TAG100", FloatType, false) ::
        StructField("TAG101", FloatType, false) ::
        StructField("TAG102", FloatType, false) ::
        StructField("TAG103", FloatType, false) ::
        StructField("TAG104", FloatType, false) ::
        StructField("TAG105", FloatType, false) ::
        StructField("TAG106", FloatType, false) ::
        StructField("TAG107", FloatType, false) ::
        StructField("TAG108", FloatType, false) ::
        StructField("TAG109", FloatType, false) ::
        StructField("TAG110", FloatType, false) ::
        StructField("TAG111", FloatType, false) ::
        StructField("TAG112", FloatType, false) ::
        StructField("TAG113", FloatType, false) ::
        StructField("TAG114", FloatType, false) ::
        StructField("TAG115", FloatType, false) ::
        StructField("TAG116", FloatType, false) ::
        StructField("TAG117", FloatType, false) ::
        StructField("TAG118", FloatType, false) ::
        StructField("TAG119", FloatType, false) ::
        StructField("TAG120", FloatType, false) ::
        StructField("TAG121", FloatType, false) ::
        StructField("TAG122", FloatType, false) ::
        StructField("TAG123", FloatType, false) ::
        StructField("TAG124", FloatType, false) ::
        StructField("TAG125", FloatType, false) ::
        StructField("TAG126", FloatType, false) ::
        StructField("TAG127", FloatType, false) ::
        StructField("TAG128", FloatType, false) ::
        StructField("TAG129", FloatType, false) ::
        StructField("TAG130", FloatType, false) ::
        StructField("TAG131", FloatType, false) ::
        StructField("TAG132", FloatType, false) ::
        StructField("TAG133", FloatType, false) ::
        StructField("TAG134", FloatType, false) ::
        StructField("TAG135", FloatType, false) ::
        StructField("TAG136", FloatType, false) ::
        StructField("TAG137", FloatType, false) ::
        StructField("TAG138", FloatType, false) ::
        StructField("TAG139", FloatType, false) ::
        StructField("TAG140", FloatType, false) ::
        StructField("TAG141", FloatType, false) ::
        StructField("TAG142", FloatType, false) ::
        StructField("TAG143", FloatType, false) ::
        StructField("TAG144", FloatType, false) ::
        StructField("TAG145", FloatType, false) ::
        StructField("TAG146", FloatType, false) ::
        StructField("TAG147", FloatType, false) ::
        StructField("TAG148", FloatType, false) ::
        StructField("TAG149", FloatType, false) ::
        StructField("TAG150", FloatType, false) ::
        StructField("CAL", FloatType, false) :: Nil
      )
      return st
    }   
}