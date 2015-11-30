package io.esse.poc

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StructType,StructField,StringType,FloatType};
import org.apache.spark.sql.SaveMode
import java.util.Properties
import java.text.SimpleDateFormat
import java.util.Date
import java.util.Calendar
import java.sql.Timestamp
import java.sql.SQLException
import java.lang.Long
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

object POC_Load_CSV {
  def main(args: Array[String]) {
    if (args.length != 4) {
      System.err.println("Syntax: POC_Load_CSV <Spark Master URL> <HDFS URL> <Source location> <Target location>")
      System.exit(1)
    }
    val hdfs = args(1) // hdfs://bd001:8020

    val conf = new SparkConf().setAppName("POC_Load_CSV Application")
    conf.setMaster(args(0))
    val sc = new SparkContext(conf)
    load_csv(hdfs, args(2), args(3), sc)
  }

  def getSchema(): StructType = {
    StructType(
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

  def load_csv(hdfs: String, dir: String, dist: String, sc: SparkContext) = {
    // val sqlContext = new SQLContext(sc)
    val size = 128 * 1024 * 1024

    val hiveContext = new HiveContext(sc)
    import hiveContext.implicits._
    hiveContext.sql("SET hive.metastore.warehouse.dir=" + hdfs + "/user/hive/warehouse")
    hiveContext.sql("SET hive.exec.dynamic.partition.mode=nostrick")
    hiveContext.sql("SET hive.exec.dynamic.partition=true")
    hiveContext.sql("SET hive.exec.max.dynamic.partitions=100000")
    hiveContext.setConf("spark.sql.planner.externalSort", "true")
    hiveContext.setConf("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    hiveContext.sparkContext.hadoopConfiguration.setInt("dfs.blocksize", size)
    hiveContext.sparkContext.hadoopConfiguration.setInt("parquet.block.size", size)
    val opts = Map("header" -> "false", "delimiter" -> ",")

    val source = hiveContext.read.format("com.databricks.spark.csv").options(opts).load(dir)

    val row_rdd = source.map(p => Row(p(0).toString(), p(1).toString(), p(2).toString().toFloat, p(3).toString().toFloat,
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
    
    row_rdd.persist(StorageLevel.MEMORY_ONLY_SER)
    val df = hiveContext.createDataFrame(row_rdd, getSchema())


    df.na.drop().write.mode(SaveMode.Append).save(hdfs + dist)
    // val tmp = hiveContext.read.load(hdfs + dist)
    // tmp.registerTempTable("temp_table")

    // hiveContext.sql("CREATE TABLE IF NOT EXISTS poc_merge_part(C0 string, C2 float, C3 float, C4 float, C5 float, C6 float, "
    //   + "C7 float, C8 float, C9 float, C10 float, C11 float, C12 float, C13 float, C14 float, C15 float, C16 float, "
    //   + "C17 float, C18 float, C19 float, C20 float, C21 float, C22 float, C23 float, C24 float, C25 float, C26 float, "
    //   + "C27 float, C28 float, C29 float, C30 float, C31 float, C32 float, C33 float, C34 float, C35 float, C36 float, "
    //   + "C37 float, C38 float, C39 float, C40 float, C41 float, C42 float, C43 float, C44 float, C45 float, C46 float, "
    //   + "C47 float, C48 float, C49 float, C50 float, C51 float, C52 float, C53 float, C54 float, C55 float, C56 float, "
    //   + "C57 float, C58 float, C59 float, C60 float, C61 float, C62 float, C63 float, C64 float, C65 float, C66 float, "
    //   + "C67 float, C68 float, C69 float, C70 float, C71 float, C72 float, C73 float, C74 float, C75 float, C76 float, "
    //   + "C77 float, C78 float, C79 float, C80 float, C81 float, C82 float, C83 float, C84 float, C85 float, C86 float, "
    //   + "C87 float, C88 float, C89 float, C90 float, C91 float, C92 float, C93 float, C94 float, C95 float, C96 float, "
    //   + "C97 float, C98 float, C99 float, C100 float, C101 float, C102 float, C103 float, C104 float, C105 float, "
    //   + "C106 float, C107 float, C108 float, C109 float, C110 float, C111 float, C112 float, C113 float, C114 float, "
    //   + "C115 float, C116 float, C117 float, C118 float, C119 float, C120 float, C121 float, C122 float, C123 float, "
    //   + "C124 float, C125 float, C126 float, C127 float, C128 float, C129 float, C130 float, C131 float, C132 float, "
    //   + "C133 float, C134 float, C135 float, C136 float, C137 float, C138 float, C139 float, C140 float, C141 float, "
    //   + "C142 float, C143 float, C144 float, C145 float, C146 float, C147 float, C148 float, C149 float, C150 float, "
    //   + "C151 float, C152 float, C153 float, C154 float, C155 float, C156 float, C157 float, C158 float, C159 float, "
    //   + "C160 float, C161 float, C162 float, C163 float, C164 float, C165 float, C166 float, C167 float, C168 float, "
    //   + "C169 float, C170 float, C171 float, C172 float, C173 float, C174 float, C175 float, C176 float, C177 float, "
    //   + "C178 float, C179 float, C180 float, C181 float, C182 float, C183 float, C184 float, C185 float, C186 float, "
    //   + "C187 float, C188 float, C189 float, C190 float, C191 float, C192 float, C193 float, C194 float, C195 float, "
    //   + "C196 float, C197 float, C198 float, C199 float, C200 float, C201 float, C202 float, C203 float, C204 float, "
    //   + "C205 float, C206 float, C207 float, C208 float, C209 float, C210 float, C211 float, C212 float, C213 float, "
    //   + "C214 float, C215 float, C216 float, C217 float, C218 float, C219 float, C220 float, C221 float, C222 float, "
    //   + "C223 float, C224 float, C225 float, C226 float, C227 float, C228 float, C229 float, C230 float, C231 float, "
    //   + "C232 float, C233 float, C234 float, C235 float, C236 float, C237 float, C238 float, C239 float, C240 float, "
    //   + "C241 float, C242 float, C243 float, C244 float, C245 float, C246 float, C247 float, C248 float, C249 float, "
    //   + "C250 float, C251 float, C252 float, C253 float, C254 float, C255 float, C256 float, C257 float, C258 float, "
    //   + "C259 float, C260 float, C261 float, C262 float, C263 float, C264 float, C265 float, C266 float, C267 float, "
    //   + "C268 float, C269 float, C270 float, C271 float, C272 float, C273 float, C274 float, C275 float, C276 float, "
    //   + "C277 float, C278 float, C279 float, C280 float, C281 float, C282 float, C283 float, C284 float, C285 float, "
    //   + "C286 float, C287 float, C288 float, C289 float, C290 float, C291 float, C292 float, C293 float, C294 float, "
    //   + "C295 float, C296 float, C297 float, C298 float, C299 float, C300 float, C301 float, C302 float, C303 float, "
    //   + "C304 float, C305 float, C306 float, C307 float, C308 float, C309 float, C310 float, C311 float, C312 float, "
    //   + "C313 float, C314 float, C315 float, C316 float, C317 float, C318 float, C319 float, C320 float, C321 float, "
    //   + "C322 float, C323 float, C324 float, C325 float, C326 float, C327 float, C328 float, C329 float, C330 float, "
    //   + "C331 float, C332 float, C333 float, C334 float, C335 float, C336 float, C337 float, C338 float, C339 float, "
    //   + "C340 float, C341 float, C342 float, C343 float, C344 float, C345 float, C346 float, C347 float, C348 float, "
    //   + "C349 float, C350 float, C351 float, C352 float, C353 float, C354 float, C355 float, C356 float, C357 float, "
    //   + "C358 float, C359 float, C360 float, C361 float, C362 float, C363 float, C364 float, C365 float, C366 float, "
    //   + "C367 float, C368 float, C369 float, C370 float, C371 float, C372 float, C373 float, C374 float, C375 float, "
    //   + "C376 float, C377 float, C378 float, C379 float, C380 float, C381 float, C382 float, C383 float, C384 float, "
    //   + "C385 float, C386 float, C387 float, C388 float, C389 float, C390 float, C391 float, C392 float, C393 float, "
    //   + "C394 float, C395 float, C396 float, C397 float, C398 float, C399 float, C400 float, C401 float, C402 float, "
    //   + "C403 float, C404 float, C405 float, C406 float, C407 float, C408 float, C409 float, C410 float, C411 float, "
    //   + "C412 float, C413 float, C414 float, C415 float, C416 float, C417 float, C418 float, C419 float, C420 float, "
    //   + "C421 float, C422 float, C423 float, C424 float, C425 float, C426 float, C427 float, C428 float, C429 float, "
    //   + "C430 float, C431 float, C432 float, C433 float, C434 float, C435 float, C436 float, C437 float, C438 float, "
    //   + "C439 float, C440 float, C441 float, C442 float, C443 float, C444 float, C445 float, C446 float, C447 float, "
    //   + "C448 float, C449 float, C450 float, C451 float, C452 float, C453 float, C454 float, C455 float, C456 float, "
    //   + "C457 float, C458 float, C459 float, C460 float, C461 float, C462 float, C463 float, C464 float, C465 float, "
    //   + "C466 float, C467 float, C468 float, C469 float, C470 float, C471 float, C472 float, C473 float, C474 float, "
    //   + "C475 float, C476 float, C477 float, C478 float, C479 float, C480 float, C481 float, C482 float, C483 float, "
    //   + "C484 float, C485 float, C486 float, C487 float, C488 float, C489 float, C490 float, C491 float, C492 float, "
    //   + "C493 float, C494 float, C495 float, C496 float, C497 float, C498 float, C499 float, C500 float, C501 float) "
    //   + "partitioned by (C1 string) ")
    // hiveContext.sql("insert into table poc_merge_part "
    //   + "partition (C1) "
    //   + "select C0, C2, C3, C4, C5, C6, C7, C8, C9, C10, C11, C12, C13, C14, C15, C16, "
    //   + "C17, C18, C19, C20, C21, C22, C23, C24, C25, C26, C27, C28, C29, C30, C31, "
    //   + "C32, C33, C34, C35, C36, C37, C38, C39, C40, C41, C42, C43, C44, C45, C46, "
    //   + "C47, C48, C49, C50, C51, C52, C53, C54, C55, C56, C57, C58, C59, C60, C61, "
    //   + "C62, C63, C64, C65, C66, C67, C68, C69, C70, C71, C72, C73, C74, C75, C76, "
    //   + "C77, C78, C79, C80, C81, C82, C83, C84, C85, C86, C87, C88, C89, C90, C91, "
    //   + "C92, C93, C94, C95, C96, C97, C98, C99, C100, C101, C102, C103, C104, C105, "
    //   + "C106, C107, C108, C109, C110, C111, C112, C113, C114, C115, C116, C117, C118, "
    //   + "C119, C120, C121, C122, C123, C124, C125, C126, C127, C128, C129, C130, C131, "
    //   + "C132, C133, C134, C135, C136, C137, C138, C139, C140, C141, C142, C143, C144, "
    //   + "C145, C146, C147, C148, C149, C150, C151, C152, C153, C154, C155, C156, C157, "
    //   + "C158, C159, C160, C161, C162, C163, C164, C165, C166, C167, C168, C169, C170, "
    //   + "C171, C172, C173, C174, C175, C176, C177, C178, C179, C180, C181, C182, C183, "
    //   + "C184, C185, C186, C187, C188, C189, C190, C191, C192, C193, C194, C195, C196, "
    //   + "C197, C198, C199, C200, C201, C202, C203, C204, C205, C206, C207, C208, C209, "
    //   + "C210, C211, C212, C213, C214, C215, C216, C217, C218, C219, C220, C221, C222, "
    //   + "C223, C224, C225, C226, C227, C228, C229, C230, C231, C232, C233, C234, C235, "
    //   + "C236, C237, C238, C239, C240, C241, C242, C243, C244, C245, C246, C247, C248, "
    //   + "C249, C250, C251, C252, C253, C254, C255, C256, C257, C258, C259, C260, C261, "
    //   + "C262, C263, C264, C265, C266, C267, C268, C269, C270, C271, C272, C273, C274, "
    //   + "C275, C276, C277, C278, C279, C280, C281, C282, C283, C284, C285, C286, C287, "
    //   + "C288, C289, C290, C291, C292, C293, C294, C295, C296, C297, C298, C299, C300, "
    //   + "C301, C302, C303, C304, C305, C306, C307, C308, C309, C310, C311, C312, C313, "
    //   + "C314, C315, C316, C317, C318, C319, C320, C321, C322, C323, C324, C325, C326, "
    //   + "C327, C328, C329, C330, C331, C332, C333, C334, C335, C336, C337, C338, C339, "
    //   + "C340, C341, C342, C343, C344, C345, C346, C347, C348, C349, C350, C351, C352, "
    //   + "C353, C354, C355, C356, C357, C358, C359, C360, C361, C362, C363, C364, C365, "
    //   + "C366, C367, C368, C369, C370, C371, C372, C373, C374, C375, C376, C377, C378, "
    //   + "C379, C380, C381, C382, C383, C384, C385, C386, C387, C388, C389, C390, C391, "
    //   + "C392, C393, C394, C395, C396, C397, C398, C399, C400, C401, C402, C403, C404, "
    //   + "C405, C406, C407, C408, C409, C410, C411, C412, C413, C414, C415, C416, C417, "
    //   + "C418, C419, C420, C421, C422, C423, C424, C425, C426, C427, C428, C429, C430, "
    //   + "C431, C432, C433, C434, C435, C436, C437, C438, C439, C440, C441, C442, C443, "
    //   + "C444, C445, C446, C447, C448, C449, C450, C451, C452, C453, C454, C455, C456, "
    //   + "C457, C458, C459, C460, C461, C462, C463, C464, C465, C466, C467, C468, C469, "
    //   + "C470, C471, C472, C473, C474, C475, C476, C477, C478, C479, C480, C481, C482, "
    //   + "C483, C484, C485, C486, C487, C488, C489, C490, C491, C492, C493, C494, C495, "
    //   + "C496, C497, C498, C499, C500, C501, C1 "
    //   + "from temp_table ")

  }
}