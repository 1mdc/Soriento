package com.emotioncity.soriento

import com.orientechnologies.orient.core.command.OCommandRequest
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal
import com.orientechnologies.orient.core.db.document.{ODatabaseDocument, ODatabaseDocumentTx}
import com.orientechnologies.orient.core.record.impl.ODocument
import com.orientechnologies.orient.core.sql.OCommandSQL
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery

import scala.collection.JavaConversions._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, blocking}

/**
  * Created by stream on 31.03.15.
  */
object RichODatabaseDocumentImpl {

  implicit class RichODatabaseDocumentTx(db: ODatabaseDocument) {

    def queryDocumentsBySql(sql: String, args:AnyRef*): List[ODocument] = blockingCall { db =>
      val results: java.util.List[ODocument] = db.query(if(args.size > 0) new OSQLSynchQuery[ODocument](sql).execute(args) else new OSQLSynchQuery[ODocument](sql))
      results.toList
    }

    def queryBySql[T](query: String, args:AnyRef*)(implicit reader: ODocumentReader[T]): List[T] = blockingCall { db =>
      val results: java.util.List[ODocument] = db.query(if(args.size > 0) new OSQLSynchQuery[ODocument](query).execute(args) else new OSQLSynchQuery[ODocument](query))
      results.toList.map(document => reader.read(document))
    }

    def command(query: String, args:AnyRef*): OCommandRequest = blockingCall { db =>
      db.command[OCommandRequest](if(args.size > 0) new OCommandSQL(query).execute(args) else new OCommandSQL(query)).execute() //type annotation of return?
    }

    def saveAs[T](oDocument: ODocument)(implicit reader: ODocumentReader[T]): Option[T] = blockingCall { db =>
      val savedDocument = db.save[ODocument](oDocument)
      reader.readOpt(savedDocument)
    }

    /**
      * TODO in OrientDb 2.2 use isPooled method of db instance
      * thanks orientdb team
      * @return
      */
    def isPooled = db.getClass.getName.equalsIgnoreCase("com.orientechnologies.orient.core.db.OPartitionedDatabasePool$DatabaseDocumentTxPolled")

    protected def blockingCall[T](payload: ODatabaseDocumentTx => T): T = {
      /*println("Blocking call")
      println(if (isPooled) "Database is pooled" else "Database is unpooled")*/
      val instance = ODatabaseRecordThreadLocal.INSTANCE.get
      //println("ThreadLocal is: " + instance.getClass.getName)
      val internalDb = if (isPooled) instance.asInstanceOf[ODatabaseDocumentTx] else instance.asInstanceOf[ODatabaseDocumentTx].copy()
      payload(internalDb)
    }

    protected def asyncCall[T](payload: ODatabaseDocumentTx => T): Future[T] = {
      /*println("Async call")
      println(if (isPooled) "Database is pooled" else "Database is unpooled")*/
      db.activateOnCurrentThread()
      val instance = ODatabaseRecordThreadLocal.INSTANCE.get
      //println("ThreadLocal is: " + instance.getClass.getName)
      Future {
        val internalDb = if (isPooled) {
          val tempDb = db.asInstanceOf[ODatabaseDocumentTx]
          tempDb.activateOnCurrentThread()
        } else {
          instance.asInstanceOf[ODatabaseDocumentTx].copy()
        }
        blocking {
          payload(internalDb)
        }
      }
    }

    def asyncQueryBySql(sql: String, args:AnyRef*): Future[List[ODocument]] = asyncCall { internalDb =>
      val results: java.util.List[ODocument] = internalDb.query(if(args.size > 0) new OSQLSynchQuery[ODocument](sql).execute(args) else new OSQLSynchQuery[ODocument](sql))
      results.toList
    }

    def asyncQueryBySql[T](query: String, args:AnyRef*)(implicit reader: ODocumentReader[T]): Future[List[T]] = asyncCall { internalDb =>
      val results: java.util.List[ODocument] = internalDb.query(
        if(args.size > 0)
          new OSQLSynchQuery[ODocument](query).execute(args)
        else
          new OSQLSynchQuery[ODocument](query)
        )
      results.toList.map(document => reader.read(document))
    }


  }

}
