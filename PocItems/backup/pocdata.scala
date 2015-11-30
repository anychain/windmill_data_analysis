package com.ibm.poc

import org.apache.spark.sql.types.{StructType,StructField,StringType,FloatType}
import org.apache.spark.sql.Row
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.rdd.RDD

object PocData {

    def dataFrame(df: DataFrame, sqlContext: HiveContext): DataFrame = {
        val row_rdd = createMappedRDD(df)
        val pocDataFrame = sqlContext.createDataFrame(row_rdd, dataSchema)
        return pocDataFrame
    }

    def createMappedRDD(df: DataFrame): RDD[Row] = {
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
      return StructType(
          StructField("C0", StringType, false) ::
          StructField("C1", StringType, false) ::
          StructField("C2", FloatType, false) ::
          StructField("C3", FloatType, false) ::
          StructField("C4", FloatType, false) ::
          StructField("C5", FloatType, false) ::
          StructField("C6", FloatType, false) ::
          StructField("C7", FloatType, false) ::
          StructField("C8", FloatType, false) ::
          StructField("C9", FloatType, false) ::
          StructField("C10", FloatType, false) ::
          StructField("C11", FloatType, false) ::
          StructField("C12", FloatType, false) ::
          StructField("C13", FloatType, false) ::
          StructField("C14", FloatType, false) ::
          StructField("C15", FloatType, false) ::
          StructField("C16", FloatType, false) ::
          StructField("C17", FloatType, false) ::
          StructField("C18", FloatType, false) ::
          StructField("C19", FloatType, false) ::
          StructField("C20", FloatType, false) ::
          StructField("C21", FloatType, false) ::
          StructField("C22", FloatType, false) ::
          StructField("C23", FloatType, false) ::
          StructField("C24", FloatType, false) ::
          StructField("C25", FloatType, false) ::
          StructField("C26", FloatType, false) ::
          StructField("C27", FloatType, false) ::
          StructField("C28", FloatType, false) ::
          StructField("C29", FloatType, false) ::
          StructField("C30", FloatType, false) ::
          StructField("C31", FloatType, false) ::
          StructField("C32", FloatType, false) ::
          StructField("C33", FloatType, false) ::
          StructField("C34", FloatType, false) ::
          StructField("C35", FloatType, false) ::
          StructField("C36", FloatType, false) ::
          StructField("C37", FloatType, false) ::
          StructField("C38", FloatType, false) ::
          StructField("C39", FloatType, false) ::
          StructField("C40", FloatType, false) ::
          StructField("C41", FloatType, false) ::
          StructField("C42", FloatType, false) ::
          StructField("C43", FloatType, false) ::
          StructField("C44", FloatType, false) ::
          StructField("C45", FloatType, false) ::
          StructField("C46", FloatType, false) ::
          StructField("C47", FloatType, false) ::
          StructField("C48", FloatType, false) ::
          StructField("C49", FloatType, false) ::
          StructField("C50", FloatType, false) ::
          StructField("C51", FloatType, false) ::
          StructField("C52", FloatType, false) ::
          StructField("C53", FloatType, false) ::
          StructField("C54", FloatType, false) ::
          StructField("C55", FloatType, false) ::
          StructField("C56", FloatType, false) ::
          StructField("C57", FloatType, false) ::
          StructField("C58", FloatType, false) ::
          StructField("C59", FloatType, false) ::
          StructField("C60", FloatType, false) ::
          StructField("C61", FloatType, false) ::
          StructField("C62", FloatType, false) ::
          StructField("C63", FloatType, false) ::
          StructField("C64", FloatType, false) ::
          StructField("C65", FloatType, false) ::
          StructField("C66", FloatType, false) ::
          StructField("C67", FloatType, false) ::
          StructField("C68", FloatType, false) ::
          StructField("C69", FloatType, false) ::
          StructField("C70", FloatType, false) ::
          StructField("C71", FloatType, false) ::
          StructField("C72", FloatType, false) ::
          StructField("C73", FloatType, false) ::
          StructField("C74", FloatType, false) ::
          StructField("C75", FloatType, false) ::
          StructField("C76", FloatType, false) ::
          StructField("C77", FloatType, false) ::
          StructField("C78", FloatType, false) ::
          StructField("C79", FloatType, false) ::
          StructField("C80", FloatType, false) ::
          StructField("C81", FloatType, false) ::
          StructField("C82", FloatType, false) ::
          StructField("C83", FloatType, false) ::
          StructField("C84", FloatType, false) ::
          StructField("C85", FloatType, false) ::
          StructField("C86", FloatType, false) ::
          StructField("C87", FloatType, false) ::
          StructField("C88", FloatType, false) ::
          StructField("C89", FloatType, false) ::
          StructField("C90", FloatType, false) ::
          StructField("C91", FloatType, false) ::
          StructField("C92", FloatType, false) ::
          StructField("C93", FloatType, false) ::
          StructField("C94", FloatType, false) ::
          StructField("C95", FloatType, false) ::
          StructField("C96", FloatType, false) ::
          StructField("C97", FloatType, false) ::
          StructField("C98", FloatType, false) ::
          StructField("C99", FloatType, false) ::
          StructField("C100", FloatType, false) ::
          StructField("C101", FloatType, false) ::
          StructField("C102", FloatType, false) ::
          StructField("C103", FloatType, false) ::
          StructField("C104", FloatType, false) ::
          StructField("C105", FloatType, false) ::
          StructField("C106", FloatType, false) ::
          StructField("C107", FloatType, false) ::
          StructField("C108", FloatType, false) ::
          StructField("C109", FloatType, false) ::
          StructField("C110", FloatType, false) ::
          StructField("C111", FloatType, false) ::
          StructField("C112", FloatType, false) ::
          StructField("C113", FloatType, false) ::
          StructField("C114", FloatType, false) ::
          StructField("C115", FloatType, false) ::
          StructField("C116", FloatType, false) ::
          StructField("C117", FloatType, false) ::
          StructField("C118", FloatType, false) ::
          StructField("C119", FloatType, false) ::
          StructField("C120", FloatType, false) ::
          StructField("C121", FloatType, false) ::
          StructField("C122", FloatType, false) ::
          StructField("C123", FloatType, false) ::
          StructField("C124", FloatType, false) ::
          StructField("C125", FloatType, false) ::
          StructField("C126", FloatType, false) ::
          StructField("C127", FloatType, false) ::
          StructField("C128", FloatType, false) ::
          StructField("C129", FloatType, false) ::
          StructField("C130", FloatType, false) ::
          StructField("C131", FloatType, false) ::
          StructField("C132", FloatType, false) ::
          StructField("C133", FloatType, false) ::
          StructField("C134", FloatType, false) ::
          StructField("C135", FloatType, false) ::
          StructField("C136", FloatType, false) ::
          StructField("C137", FloatType, false) ::
          StructField("C138", FloatType, false) ::
          StructField("C139", FloatType, false) ::
          StructField("C140", FloatType, false) ::
          StructField("C141", FloatType, false) ::
          StructField("C142", FloatType, false) ::
          StructField("C143", FloatType, false) ::
          StructField("C144", FloatType, false) ::
          StructField("C145", FloatType, false) ::
          StructField("C146", FloatType, false) ::
          StructField("C147", FloatType, false) ::
          StructField("C148", FloatType, false) ::
          StructField("C149", FloatType, false) ::
          StructField("C150", FloatType, false) ::
          StructField("C151", FloatType, false) ::
          StructField("C152", FloatType, false) ::
          StructField("C153", FloatType, false) ::
          StructField("C154", FloatType, false) ::
          StructField("C155", FloatType, false) ::
          StructField("C156", FloatType, false) ::
          StructField("C157", FloatType, false) ::
          StructField("C158", FloatType, false) ::
          StructField("C159", FloatType, false) ::
          StructField("C160", FloatType, false) ::
          StructField("C161", FloatType, false) ::
          StructField("C162", FloatType, false) ::
          StructField("C163", FloatType, false) ::
          StructField("C164", FloatType, false) ::
          StructField("C165", FloatType, false) ::
          StructField("C166", FloatType, false) ::
          StructField("C167", FloatType, false) ::
          StructField("C168", FloatType, false) ::
          StructField("C169", FloatType, false) ::
          StructField("C170", FloatType, false) ::
          StructField("C171", FloatType, false) ::
          StructField("C172", FloatType, false) ::
          StructField("C173", FloatType, false) ::
          StructField("C174", FloatType, false) ::
          StructField("C175", FloatType, false) ::
          StructField("C176", FloatType, false) ::
          StructField("C177", FloatType, false) ::
          StructField("C178", FloatType, false) ::
          StructField("C179", FloatType, false) ::
          StructField("C180", FloatType, false) ::
          StructField("C181", FloatType, false) ::
          StructField("C182", FloatType, false) ::
          StructField("C183", FloatType, false) ::
          StructField("C184", FloatType, false) ::
          StructField("C185", FloatType, false) ::
          StructField("C186", FloatType, false) ::
          StructField("C187", FloatType, false) ::
          StructField("C188", FloatType, false) ::
          StructField("C189", FloatType, false) ::
          StructField("C190", FloatType, false) ::
          StructField("C191", FloatType, false) ::
          StructField("C192", FloatType, false) ::
          StructField("C193", FloatType, false) ::
          StructField("C194", FloatType, false) ::
          StructField("C195", FloatType, false) ::
          StructField("C196", FloatType, false) ::
          StructField("C197", FloatType, false) ::
          StructField("C198", FloatType, false) ::
          StructField("C199", FloatType, false) ::
          StructField("C200", FloatType, false) ::
          StructField("C201", FloatType, false) ::
          StructField("C202", FloatType, false) ::
          StructField("C203", FloatType, false) ::
          StructField("C204", FloatType, false) ::
          StructField("C205", FloatType, false) ::
          StructField("C206", FloatType, false) ::
          StructField("C207", FloatType, false) ::
          StructField("C208", FloatType, false) ::
          StructField("C209", FloatType, false) ::
          StructField("C210", FloatType, false) ::
          StructField("C211", FloatType, false) ::
          StructField("C212", FloatType, false) ::
          StructField("C213", FloatType, false) ::
          StructField("C214", FloatType, false) ::
          StructField("C215", FloatType, false) ::
          StructField("C216", FloatType, false) ::
          StructField("C217", FloatType, false) ::
          StructField("C218", FloatType, false) ::
          StructField("C219", FloatType, false) ::
          StructField("C220", FloatType, false) ::
          StructField("C221", FloatType, false) ::
          StructField("C222", FloatType, false) ::
          StructField("C223", FloatType, false) ::
          StructField("C224", FloatType, false) ::
          StructField("C225", FloatType, false) ::
          StructField("C226", FloatType, false) ::
          StructField("C227", FloatType, false) ::
          StructField("C228", FloatType, false) ::
          StructField("C229", FloatType, false) ::
          StructField("C230", FloatType, false) ::
          StructField("C231", FloatType, false) ::
          StructField("C232", FloatType, false) ::
          StructField("C233", FloatType, false) ::
          StructField("C234", FloatType, false) ::
          StructField("C235", FloatType, false) ::
          StructField("C236", FloatType, false) ::
          StructField("C237", FloatType, false) ::
          StructField("C238", FloatType, false) ::
          StructField("C239", FloatType, false) ::
          StructField("C240", FloatType, false) ::
          StructField("C241", FloatType, false) ::
          StructField("C242", FloatType, false) ::
          StructField("C243", FloatType, false) ::
          StructField("C244", FloatType, false) ::
          StructField("C245", FloatType, false) ::
          StructField("C246", FloatType, false) ::
          StructField("C247", FloatType, false) ::
          StructField("C248", FloatType, false) ::
          StructField("C249", FloatType, false) ::
          StructField("C250", FloatType, false) ::
          StructField("C251", FloatType, false) ::
          StructField("C252", FloatType, false) ::
          StructField("C253", FloatType, false) ::
          StructField("C254", FloatType, false) ::
          StructField("C255", FloatType, false) ::
          StructField("C256", FloatType, false) ::
          StructField("C257", FloatType, false) ::
          StructField("C258", FloatType, false) ::
          StructField("C259", FloatType, false) ::
          StructField("C260", FloatType, false) ::
          StructField("C261", FloatType, false) ::
          StructField("C262", FloatType, false) ::
          StructField("C263", FloatType, false) ::
          StructField("C264", FloatType, false) ::
          StructField("C265", FloatType, false) ::
          StructField("C266", FloatType, false) ::
          StructField("C267", FloatType, false) ::
          StructField("C268", FloatType, false) ::
          StructField("C269", FloatType, false) ::
          StructField("C270", FloatType, false) ::
          StructField("C271", FloatType, false) ::
          StructField("C272", FloatType, false) ::
          StructField("C273", FloatType, false) ::
          StructField("C274", FloatType, false) ::
          StructField("C275", FloatType, false) ::
          StructField("C276", FloatType, false) ::
          StructField("C277", FloatType, false) ::
          StructField("C278", FloatType, false) ::
          StructField("C279", FloatType, false) ::
          StructField("C280", FloatType, false) ::
          StructField("C281", FloatType, false) ::
          StructField("C282", FloatType, false) ::
          StructField("C283", FloatType, false) ::
          StructField("C284", FloatType, false) ::
          StructField("C285", FloatType, false) ::
          StructField("C286", FloatType, false) ::
          StructField("C287", FloatType, false) ::
          StructField("C288", FloatType, false) ::
          StructField("C289", FloatType, false) ::
          StructField("C290", FloatType, false) ::
          StructField("C291", FloatType, false) ::
          StructField("C292", FloatType, false) ::
          StructField("C293", FloatType, false) ::
          StructField("C294", FloatType, false) ::
          StructField("C295", FloatType, false) ::
          StructField("C296", FloatType, false) ::
          StructField("C297", FloatType, false) ::
          StructField("C298", FloatType, false) ::
          StructField("C299", FloatType, false) ::
          StructField("C300", FloatType, false) ::
          StructField("C301", FloatType, false) ::
          StructField("C302", FloatType, false) ::
          StructField("C303", FloatType, false) ::
          StructField("C304", FloatType, false) ::
          StructField("C305", FloatType, false) ::
          StructField("C306", FloatType, false) ::
          StructField("C307", FloatType, false) ::
          StructField("C308", FloatType, false) ::
          StructField("C309", FloatType, false) ::
          StructField("C310", FloatType, false) ::
          StructField("C311", FloatType, false) ::
          StructField("C312", FloatType, false) ::
          StructField("C313", FloatType, false) ::
          StructField("C314", FloatType, false) ::
          StructField("C315", FloatType, false) ::
          StructField("C316", FloatType, false) ::
          StructField("C317", FloatType, false) ::
          StructField("C318", FloatType, false) ::
          StructField("C319", FloatType, false) ::
          StructField("C320", FloatType, false) ::
          StructField("C321", FloatType, false) ::
          StructField("C322", FloatType, false) ::
          StructField("C323", FloatType, false) ::
          StructField("C324", FloatType, false) ::
          StructField("C325", FloatType, false) ::
          StructField("C326", FloatType, false) ::
          StructField("C327", FloatType, false) ::
          StructField("C328", FloatType, false) ::
          StructField("C329", FloatType, false) ::
          StructField("C330", FloatType, false) ::
          StructField("C331", FloatType, false) ::
          StructField("C332", FloatType, false) ::
          StructField("C333", FloatType, false) ::
          StructField("C334", FloatType, false) ::
          StructField("C335", FloatType, false) ::
          StructField("C336", FloatType, false) ::
          StructField("C337", FloatType, false) ::
          StructField("C338", FloatType, false) ::
          StructField("C339", FloatType, false) ::
          StructField("C340", FloatType, false) ::
          StructField("C341", FloatType, false) ::
          StructField("C342", FloatType, false) ::
          StructField("C343", FloatType, false) ::
          StructField("C344", FloatType, false) ::
          StructField("C345", FloatType, false) ::
          StructField("C346", FloatType, false) ::
          StructField("C347", FloatType, false) ::
          StructField("C348", FloatType, false) ::
          StructField("C349", FloatType, false) ::
          StructField("C350", FloatType, false) ::
          StructField("C351", FloatType, false) ::
          StructField("C352", FloatType, false) ::
          StructField("C353", FloatType, false) ::
          StructField("C354", FloatType, false) ::
          StructField("C355", FloatType, false) ::
          StructField("C356", FloatType, false) ::
          StructField("C357", FloatType, false) ::
          StructField("C358", FloatType, false) ::
          StructField("C359", FloatType, false) ::
          StructField("C360", FloatType, false) ::
          StructField("C361", FloatType, false) ::
          StructField("C362", FloatType, false) ::
          StructField("C363", FloatType, false) ::
          StructField("C364", FloatType, false) ::
          StructField("C365", FloatType, false) ::
          StructField("C366", FloatType, false) ::
          StructField("C367", FloatType, false) ::
          StructField("C368", FloatType, false) ::
          StructField("C369", FloatType, false) ::
          StructField("C370", FloatType, false) ::
          StructField("C371", FloatType, false) ::
          StructField("C372", FloatType, false) ::
          StructField("C373", FloatType, false) ::
          StructField("C374", FloatType, false) ::
          StructField("C375", FloatType, false) ::
          StructField("C376", FloatType, false) ::
          StructField("C377", FloatType, false) ::
          StructField("C378", FloatType, false) ::
          StructField("C379", FloatType, false) ::
          StructField("C380", FloatType, false) ::
          StructField("C381", FloatType, false) ::
          StructField("C382", FloatType, false) ::
          StructField("C383", FloatType, false) ::
          StructField("C384", FloatType, false) ::
          StructField("C385", FloatType, false) ::
          StructField("C386", FloatType, false) ::
          StructField("C387", FloatType, false) ::
          StructField("C388", FloatType, false) ::
          StructField("C389", FloatType, false) ::
          StructField("C390", FloatType, false) ::
          StructField("C391", FloatType, false) ::
          StructField("C392", FloatType, false) ::
          StructField("C393", FloatType, false) ::
          StructField("C394", FloatType, false) ::
          StructField("C395", FloatType, false) ::
          StructField("C396", FloatType, false) ::
          StructField("C397", FloatType, false) ::
          StructField("C398", FloatType, false) ::
          StructField("C399", FloatType, false) ::
          StructField("C400", FloatType, false) ::
          StructField("C401", FloatType, false) ::
          StructField("C402", FloatType, false) ::
          StructField("C403", FloatType, false) ::
          StructField("C404", FloatType, false) ::
          StructField("C405", FloatType, false) ::
          StructField("C406", FloatType, false) ::
          StructField("C407", FloatType, false) ::
          StructField("C408", FloatType, false) ::
          StructField("C409", FloatType, false) ::
          StructField("C410", FloatType, false) ::
          StructField("C411", FloatType, false) ::
          StructField("C412", FloatType, false) ::
          StructField("C413", FloatType, false) ::
          StructField("C414", FloatType, false) ::
          StructField("C415", FloatType, false) ::
          StructField("C416", FloatType, false) ::
          StructField("C417", FloatType, false) ::
          StructField("C418", FloatType, false) ::
          StructField("C419", FloatType, false) ::
          StructField("C420", FloatType, false) ::
          StructField("C421", FloatType, false) ::
          StructField("C422", FloatType, false) ::
          StructField("C423", FloatType, false) ::
          StructField("C424", FloatType, false) ::
          StructField("C425", FloatType, false) ::
          StructField("C426", FloatType, false) ::
          StructField("C427", FloatType, false) ::
          StructField("C428", FloatType, false) ::
          StructField("C429", FloatType, false) ::
          StructField("C430", FloatType, false) ::
          StructField("C431", FloatType, false) ::
          StructField("C432", FloatType, false) ::
          StructField("C433", FloatType, false) ::
          StructField("C434", FloatType, false) ::
          StructField("C435", FloatType, false) ::
          StructField("C436", FloatType, false) ::
          StructField("C437", FloatType, false) ::
          StructField("C438", FloatType, false) ::
          StructField("C439", FloatType, false) ::
          StructField("C440", FloatType, false) ::
          StructField("C441", FloatType, false) ::
          StructField("C442", FloatType, false) ::
          StructField("C443", FloatType, false) ::
          StructField("C444", FloatType, false) ::
          StructField("C445", FloatType, false) ::
          StructField("C446", FloatType, false) ::
          StructField("C447", FloatType, false) ::
          StructField("C448", FloatType, false) ::
          StructField("C449", FloatType, false) ::
          StructField("C450", FloatType, false) ::
          StructField("C451", FloatType, false) ::
          StructField("C452", FloatType, false) ::
          StructField("C453", FloatType, false) ::
          StructField("C454", FloatType, false) ::
          StructField("C455", FloatType, false) ::
          StructField("C456", FloatType, false) ::
          StructField("C457", FloatType, false) ::
          StructField("C458", FloatType, false) ::
          StructField("C459", FloatType, false) ::
          StructField("C460", FloatType, false) ::
          StructField("C461", FloatType, false) ::
          StructField("C462", FloatType, false) ::
          StructField("C463", FloatType, false) ::
          StructField("C464", FloatType, false) ::
          StructField("C465", FloatType, false) ::
          StructField("C466", FloatType, false) ::
          StructField("C467", FloatType, false) ::
          StructField("C468", FloatType, false) ::
          StructField("C469", FloatType, false) ::
          StructField("C470", FloatType, false) ::
          StructField("C471", FloatType, false) ::
          StructField("C472", FloatType, false) ::
          StructField("C473", FloatType, false) ::
          StructField("C474", FloatType, false) ::
          StructField("C475", FloatType, false) ::
          StructField("C476", FloatType, false) ::
          StructField("C477", FloatType, false) ::
          StructField("C478", FloatType, false) ::
          StructField("C479", FloatType, false) ::
          StructField("C480", FloatType, false) ::
          StructField("C481", FloatType, false) ::
          StructField("C482", FloatType, false) ::
          StructField("C483", FloatType, false) ::
          StructField("C484", FloatType, false) ::
          StructField("C485", FloatType, false) ::
          StructField("C486", FloatType, false) ::
          StructField("C487", FloatType, false) ::
          StructField("C488", FloatType, false) ::
          StructField("C489", FloatType, false) ::
          StructField("C490", FloatType, false) ::
          StructField("C491", FloatType, false) ::
          StructField("C492", FloatType, false) ::
          StructField("C493", FloatType, false) ::
          StructField("C494", FloatType, false) ::
          StructField("C495", FloatType, false) ::
          StructField("C496", FloatType, false) ::
          StructField("C497", FloatType, false) ::
          StructField("C498", FloatType, false) ::
          StructField("C499", FloatType, false) ::
          StructField("C500", FloatType, false) ::
          StructField("C501", FloatType, false) ::  Nil
      )
    }
}