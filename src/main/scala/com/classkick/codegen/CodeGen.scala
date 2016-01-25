package com.classkick.codegen

import java.net.URI

import slick.backend.DatabaseConfig
import slick.codegen.SourceCodeGenerator
import slick.driver.JdbcProfile
import slick.model.QualifiedName
import slick.util.ConfigExtensionMethods.configExtensionMethods
import slick.{model => m}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext}

/** A runnable class to execute the code generator without further setup */
object CodeGen {

    def fixName(name: QualifiedName): QualifiedName = {
        name.copy(schema = name.catalog, catalog = name.schema)
    }

    def fixFk(fk: m.ForeignKey): m.ForeignKey = {
        fk.copy(
            referencedTable = fixName(fk.referencedTable),
            referencedColumns = fk.referencedColumns.map(fixColumn),
            referencingTable = fixName(fk.referencingTable),
            referencingColumns = fk.referencingColumns.map(fixColumn)
        )
    }

    def fixColumn(col: m.Column): m.Column = {
        col.copy(table = fixName(col.table))
    }

    def fixTable(table: m.Table): m.Table = {
        val newName = fixName(table.name)
        table.copy(
            name = newName,
            columns = table.columns.map(_.copy(table = newName)),
            primaryKey = table.primaryKey.map(pk => pk.copy(table = newName, columns = pk.columns.map(fixColumn))),
            foreignKeys = table.foreignKeys.map(fixFk),
            indices = table.indices.map(idx => idx.copy(table = newName, columns = idx.columns.map(fixColumn)))
        )
    }

    def fixSchema(model: m.Model): m.Model = {
        model.copy(tables = model.tables.map(fixTable))
    }

    def run(slickDriver: String, jdbcDriver: String, url: String, outputDir: String, pkg: String, user: Option[String], password: Option[String]): Unit = {
        val driver: JdbcProfile =
            Class.forName(slickDriver + "$").getField("MODULE$").get(null).asInstanceOf[JdbcProfile]
        val dbFactory = driver.api.Database
        val db = dbFactory.forURL(url, driver = jdbcDriver,
            user = user.getOrElse(null), password = password.getOrElse(null), keepAliveConnection = true)
        try {
            val m = Await.result(db.run(driver.createModel(None, false)(ExecutionContext.global).withPinnedSession), Duration.Inf)
            new SourceCodeGenerator(fixSchema(m)).writeToFile(slickDriver,outputDir,pkg)
        } finally db.close
    }

    def run(uri: URI, outputDir: Option[String]): Unit = {
        val dc = DatabaseConfig.forURI[JdbcProfile](uri)
        val pkg = dc.config.getString("codegen.package")
        val out = outputDir.getOrElse(dc.config.getStringOr("codegen.outputDir", "."))
        val slickDriver = if(dc.driverIsObject) dc.driverName else "new " + dc.driverName
        try {
            val m = Await.result(dc.db.run(dc.driver.createModel(None, false)(ExecutionContext.global).withPinnedSession), Duration.Inf)
            new SourceCodeGenerator(fixSchema(m)).writeToFile(slickDriver, out, pkg)
        } finally dc.db.close
    }

    def main(args: Array[String]): Unit = {
        args.toList match {
            case uri :: Nil =>
                run(new URI(uri), None)
            case uri :: outputDir :: Nil =>
                run(new URI(uri), Some(outputDir))
            case slickDriver :: jdbcDriver :: url :: outputDir :: pkg :: Nil =>
                run(slickDriver, jdbcDriver, url, outputDir, pkg, None, None)
            case slickDriver :: jdbcDriver :: url :: outputDir :: pkg :: user :: password :: Nil =>
                run(slickDriver, jdbcDriver, url, outputDir, pkg, Some(user), Some(password))
            case _ => {
                println("""
                          |Usage:
                          |  SourceCodeGenerator configURI [outputDir]
                          |  SourceCodeGenerator slickDriver jdbcDriver url outputDir pkg [user password]
                          |
                          |Options:
                          |  configURI: A URL pointing to a standard database config file (a fragment is
                          |    resolved as a path in the config), or just a fragment used as a path in
                          |    application.conf on the class path
                          |  slickDriver: Fully qualified name of Slick driver class, e.g. "slick.driver.H2Driver"
                          |  jdbcDriver: Fully qualified name of jdbc driver class, e.g. "org.h2.Driver"
                          |  url: JDBC URL, e.g. "jdbc:postgresql://localhost/test"
                          |  outputDir: Place where the package folder structure should be put
                          |  pkg: Scala package the generated code should be places in
                          |  user: database connection user name
                          |  password: database connection password
                          |
                          |When using a config file, in addition to the standard config parameters from
                          |slick.backend.DatabaseConfig you can set "codegen.package" and
                          |"codegen.outputDir". The latter can be overridden on the command line.
                        """.stripMargin.trim)
                System.exit(1)
            }
        }
    }
}
