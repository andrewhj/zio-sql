package zio.sql.mysql

import zio.sql.{ Jdbc, Renderer }

trait MysqlModule extends Jdbc {
  self =>

  object MysqlFunctionDef {
    val Sind = FunctionDef[Double, Double](FunctionName("sind"))
  }

  override def renderRead(read: self.Read[_]): String = {
    val builder = new StringBuilder

    def buildExpr[A, B](expr: self.Expr[_, A, B]): Unit = expr match {
      case Expr.Source(tableName, column)                                            =>
        val _ = builder.append(tableName).append(".").append(column.name)
      case Expr.Unary(base, op)                                                      =>
        val _ = builder.append(" ").append(op.symbol)
        buildExpr(base)
      case Expr.Property(base, op)                                                   =>
        buildExpr(base)
        val _ = builder.append(" ").append(op.symbol)
      case Expr.Binary(left, right, op)                                              =>
        buildExpr(left)
        builder.append(" ").append(op.symbol).append(" ")
        buildExpr(right)
      case Expr.Relational(left, right, op)                                          =>
        buildExpr(left)
        builder.append(" ").append(op.symbol).append(" ")
        buildExpr(right)
      case Expr.In(value, set)                                                       =>
        buildExpr(value)
        buildReadString(set)
      case Expr.Literal(value)                                                       =>
        val _ = builder.append(value.toString) //todo fix escaping
      case Expr.AggregationCall(param, aggregation)                                  =>
        builder.append(aggregation.name.name)
        builder.append("(")
        buildExpr(param)
        val _ = builder.append(")")
      case Expr.FunctionCall0(function) if function.name.name == "localtime"         =>
        val _ = builder.append(function.name.name)
      case Expr.FunctionCall0(function) if function.name.name == "localtimestamp"    =>
        val _ = builder.append(function.name.name)
      case Expr.FunctionCall0(function) if function.name.name == "current_date"      =>
        val _ = builder.append(function.name.name)
      case Expr.FunctionCall0(function) if function.name.name == "current_timestamp" =>
        val _ = builder.append(function.name.name)
      case Expr.FunctionCall0(function)                                              =>
        builder.append(function.name.name)
        builder.append("(")
        val _ = builder.append(")")
      case Expr.FunctionCall1(param, function)                                       =>
        builder.append(function.name.name)
        builder.append("(")
        buildExpr(param)
        val _ = builder.append(")")
      case Expr.FunctionCall2(param1, param2, function)                              =>
        builder.append(function.name.name)
        builder.append("(")
        buildExpr(param1)
        builder.append(",")
        buildExpr(param2)
        val _ = builder.append(")")
      case Expr.FunctionCall3(param1, param2, param3, function)                      =>
        builder.append(function.name.name)
        builder.append("(")
        buildExpr(param1)
        builder.append(",")
        buildExpr(param2)
        builder.append(",")
        buildExpr(param3)
        val _ = builder.append(")")
      case Expr.FunctionCall4(param1, param2, param3, param4, function)              =>
        builder.append(function.name.name)
        builder.append("(")
        buildExpr(param1)
        builder.append(",")
        buildExpr(param2)
        builder.append(",")
        buildExpr(param3)
        builder.append(",")
        buildExpr(param4)
        val _ = builder.append(")")
    }

    def buildReadString[A <: SelectionSet[_]](read: self.Read[_]): Unit =
      read match {
        case read0 @ Read.Select(_, _, _, _, _, _, _, _) =>
          object Dummy {
            type F
            type A
            type B <: SelectionSet[A]
          }
          val read = read0.asInstanceOf[Read.Select[Dummy.F, Dummy.A, Dummy.B]]
          import read._

          builder.append("SELECT ")
          buildSelection(selection.value)
          builder.append(" FROM ")
          buildTable(table)
          whereExpr match {
            case Expr.Literal(true) => ()
            case _                  =>
              builder.append(" WHERE ")
              buildExpr(whereExpr)
          }
          groupBy match {
            case _ :: _ =>
              builder.append(" GROUP BY ")
              buildExprList(groupBy)

              havingExpr match {
                case Expr.Literal(true) => ()
                case _                  =>
                  builder.append(" HAVING ")
                  buildExpr(havingExpr)
              }
            case Nil    => ()
          }
          orderBy match {
            case _ :: _ =>
              builder.append(" ORDER BY ")
              buildOrderingList(orderBy)
            case Nil    => ()
          }
          limit match {
            case Some(limit) =>
              builder.append(" LIMIT ").append(limit)
            case None        => ()
          }
          offset match {
            case Some(offset) =>
              val _ = builder.append(" OFFSET ").append(offset)
            case None         => ()
          }

        case Read.Union(left, right, distinct) =>
          buildReadString(left)
          builder.append(" UNION ")
          if (!distinct) builder.append("ALL ")
          buildReadString(right)

        case Read.Literal(values) =>
          val _ = builder.append(" (").append(values.mkString(",")).append(") ") //todo fix needs escaping
      }

    def buildExprList(expr: List[Expr[_, _, _]]): Unit =
      expr match {
        case head :: tail =>
          buildExpr(head)
          tail match {
            case _ :: _ =>
              builder.append(", ")
              buildExprList(tail)
            case Nil    => ()
          }
        case Nil          => ()
      }

    def buildOrderingList(expr: List[Ordering[Expr[_, _, _]]]): Unit =
      expr match {
        case head :: tail =>
          head match {
            case Ordering.Asc(value)  => buildExpr(value)
            case Ordering.Desc(value) =>
              buildExpr(value)
              builder.append(" DESC")
          }
          tail match {
            case _ :: _ =>
              builder.append(", ")
              buildOrderingList(tail)
            case Nil    => ()
          }
        case Nil          => ()
      }

    def buildSelection[A](selectionSet: SelectionSet[A]): Unit =
      selectionSet match {
        case cons0 @ SelectionSet.Cons(_, _) =>
          object Dummy {
            type Source
            type A
            type B <: SelectionSet[Source]
          }
          val cons = cons0.asInstanceOf[SelectionSet.Cons[Dummy.Source, Dummy.A, Dummy.B]]
          import cons._
          buildColumnSelection(head)
          if (tail != SelectionSet.Empty) {
            builder.append(", ")
            buildSelection(tail)
          }
        case SelectionSet.Empty              => ()
      }

    def buildColumnSelection[A, B](columnSelection: ColumnSelection[A, B]): Unit =
      columnSelection match {
        case ColumnSelection.Constant(value, name) =>
          builder.append(value.toString()) //todo fix escaping
          name match {
            case Some(name) =>
              val _ = builder.append(" AS ").append(name)
            case None       => ()
          }
        case ColumnSelection.Computed(expr, name)  =>
          buildExpr(expr)
          name match {
            case Some(name) =>
              Expr.exprName(expr) match {
                case Some(sourceName) if name != sourceName =>
                  val _ = builder.append(" AS ").append(name)
                case _                                      => ()
              }
            case _          => () //todo what do we do if we don't have a name?
          }
      }

    def buildTable(table: Table): Unit =
      table match {
        //The outer reference in this type test cannot be checked at run time?!
        case sourceTable: self.Table.Source          =>
          val _ = builder.append(sourceTable.name)
        case Table.Joined(joinType, left, right, on) =>
          buildTable(left)
          builder.append(joinType match {
            case JoinType.Inner      => " INNER JOIN "
            case JoinType.LeftOuter  => " LEFT JOIN "
            case JoinType.RightOuter => " RIGHT JOIN "
            case JoinType.FullOuter  => " OUTER JOIN "
          })
          buildTable(right)
          builder.append(" ON ")
          buildExpr(on)
          val _ = builder.append(" ")
      }

    buildReadString(read)
    builder.toString()
  }

  override def renderDelete(delete: self.Delete[_]): String = {
    implicit val render: Renderer = Renderer()
    MysqlRenderModule.renderDeleteImpl(delete)
    println(render.toString)
    render.toString
  }

  override def renderUpdate(update: self.Update[_]): String = ???

  object MysqlRenderModule {
    def renderDeleteImpl(delete: Delete[_])(implicit render: Renderer) = {
      render("DELETE FROM ")
      renderTable(delete.table)
      delete.whereExpr match {
        case Expr.Literal(true) => ()
        case _                  =>
          render(" WHERE ")
          renderExpr(delete.whereExpr)
      }
    }

    private[zio] def renderList[A, B](lit: self.Expr.Literal[_])(implicit render: Renderer): Unit = {
      import TypeTag._
      lit.typeTag match {
        case tt @ TByteArray      => render(tt.cast(lit.value))                     // todo still broken
        //something like? render(tt.cast(lit.value).map("\\\\%03o" format _).mkString("E\'", "", "\'"))
        case tt @ TChar           =>
          render("'", tt.cast(lit.value), "'") //todo is this the same as a string? fix escaping
        case tt @ TInstant        => render("TIMESTAMP '", tt.cast(lit.value), "'") //todo test
        case tt @ TLocalDate      => render(tt.cast(lit.value))                     // todo still broken
        case tt @ TLocalDateTime  => render(tt.cast(lit.value))                     // todo still broken
        case tt @ TLocalTime      => render(tt.cast(lit.value))                     // todo still broken
        case tt @ TOffsetDateTime => render(tt.cast(lit.value))                     // todo still broken
        case tt @ TOffsetTime     => render(tt.cast(lit.value))                     // todo still broken
        case tt @ TUUID           => render(tt.cast(lit.value))                     // todo still broken
        case tt @ TZonedDateTime  => render(tt.cast(lit.value))                     // todo still broken

        case TByte       => render(lit.value)           //default toString is probably ok
        case TBigDecimal => render(lit.value)           //default toString is probably ok
        case TBoolean    => render(lit.value)           //default toString is probably ok
        case TDouble     => render(lit.value)           //default toString is probably ok
        case TFloat      => render(lit.value)           //default toString is probably ok
        case TInt        => render(lit.value)           //default toString is probably ok
        case TLong       => render(lit.value)           //default toString is probably ok
        case TShort      => render(lit.value)           //default toString is probably ok
        case TString     => render("'", lit.value, "'") //todo fix escaping

        case _ => render(lit.value) //todo fix add TypeTag.Nullable[_] =>
      }
    }

    private[zio] def renderExpr[A, B](expr: self.Expr[_, A, B])(implicit render: Renderer): Unit = expr match {
      case Expr.Source(tableName, column)         => render(tableName, ".", column.name)
      case Expr.Unary(base, op)                   =>
        render(" ", op.symbol)
        renderExpr(base)
      case Expr.Property(base, op)                =>
        renderExpr(base)
        render(" ", op.symbol)
      case Expr.Binary(left, right, op)           =>
        renderExpr(left)
        render(" ", op.symbol, " ")
        renderExpr(right)
      case Expr.Relational(left, right, op)       =>
        renderExpr(left)
        render(" ", op.symbol, " ")
        renderExpr(right)
      case Expr.In(value, set)                    =>
        renderExpr(value)
        renderReadImpl(set)
      case lit: Expr.Literal[_]                   => renderList(lit)
      case Expr.AggregationCall(p, aggregation)   =>
        render(aggregation.name.name, "(")
        renderExpr(p)
        render(")")
      case Expr.ParenlessFunctionCall0(fn)        =>
        val _ = render(fn.name)
      case Expr.FunctionCall0(fn)                 =>
        render(fn.name.name)
        render("(")
        val _ = render(")")
      case Expr.FunctionCall1(p, fn)              =>
        render(fn.name.name, "(")
        renderExpr(p)
        render(")")
      case Expr.FunctionCall2(p1, p2, fn)         =>
        render(fn.name.name, "(")
        renderExpr(p1)
        render(",")
        renderExpr(p2)
        render(")")
      case Expr.FunctionCall3(p1, p2, p3, fn)     =>
        render(fn.name.name, "(")
        renderExpr(p1)
        render(",")
        renderExpr(p2)
        render(",")
        renderExpr(p3)
        render(")")
      case Expr.FunctionCall4(p1, p2, p3, p4, fn) =>
        render(fn.name.name, "(")
        renderExpr(p1)
        render(",")
        renderExpr(p2)
        render(",")
        renderExpr(p3)
        render(",")
        renderExpr(p4)
        render(")")
    }

    private[zio] def renderReadImpl[A <: SelectionSet[_]](read: self.Read[_])(implicit render: Renderer): Unit =
      read match {
        case read0 @ Read.Select(_, _, _, _, _, _, _, _) =>
          object Dummy { type F; type A; type B <: SelectionSet[A] }
          val read = read0.asInstanceOf[Read.Select[Dummy.F, Dummy.A, Dummy.B]]
          import read._

          render("SELECT ")
          renderSelection(selection.value)
          render(" FROM ")
          renderTable(table)
          whereExpr match {
            case Expr.Literal(true) => ()
            case _                  =>
              render(" WHERE ")
              renderExpr(whereExpr)
          }
          groupBy match {
            case _ :: _ =>
              render(" GROUP BY ")
              renderExprList(groupBy)

              havingExpr match {
                case Expr.Literal(true) => ()
                case _                  =>
                  render(" HAVING ")
                  renderExpr(havingExpr)
              }
            case Nil    => ()
          }
          orderBy match {
            case _ :: _ =>
              render(" ORDER BY ")
              renderOrderingList(orderBy)
            case Nil    => ()
          }
          limit match {
            case Some(limit) => render(" LIMIT ", limit)
            case None        => ()
          }
          offset match {
            case Some(offset) => render(" OFFSET ", offset)
            case None         => ()
          }

        case Read.Union(left, right, distinct) =>
          renderReadImpl(left)
          render(" UNION ")
          if (!distinct) render("ALL ")
          renderReadImpl(right)

        case Read.Literal(values) =>
          render(" (", values.mkString(","), ") ") // todo fix needs escaping
      }

    def renderExprList(expr: List[Expr[_, _, _]])(implicit render: Renderer): Unit =
      expr match {
        case head :: tail =>
          renderExpr(head)
          tail match {
            case _ :: _ =>
              render(", ")
              renderExprList(tail)
            case Nil    => ()
          }
        case Nil          => ()
      }

    def renderOrderingList(expr: List[Ordering[Expr[_, _, _]]])(implicit render: Renderer): Unit =
      expr match {
        case head :: tail =>
          head match {
            case Ordering.Asc(value)  => renderExpr(value)
            case Ordering.Desc(value) =>
              renderExpr(value)
              render(" DESC")
          }
          tail match {
            case _ :: _ =>
              render(", ")
              renderOrderingList(tail)
            case Nil    => ()
          }
        case Nil          => ()
      }

    def renderSelection[A](selectionSet: SelectionSet[A])(implicit render: Renderer): Unit =
      selectionSet match {
        case cons0 @ SelectionSet.Cons(_, _) =>
          object Dummy {
            type Source
            type A
            type B <: SelectionSet[Source]
          }
          val cons = cons0.asInstanceOf[SelectionSet.Cons[Dummy.Source, Dummy.A, Dummy.B]]
          import cons._
          renderColumnSelection(head)
          if (tail != SelectionSet.Empty) {
            render(", ")
            renderSelection(tail)
          }
        case SelectionSet.Empty              => ()
      }

    def renderColumnSelection[A, B](columnSelection: ColumnSelection[A, B])(implicit render: Renderer): Unit =
      columnSelection match {
        case ColumnSelection.Constant(value, name) =>
          render(value) // todo fix escaping
          name match {
            case Some(name) => render(" AS ", name)
            case None       => ()
          }
        case ColumnSelection.Computed(expr, name)  =>
          renderExpr(expr)
          name match {
            case Some(name) =>
              Expr.exprName(expr) match {
                case Some(sourceName) if name != sourceName => render(" AS ", name)
                case _                                      => ()
              }
            case _          => ()
          }
      }

    def renderTable(table: Table)(implicit render: Renderer): Unit =
      table match {
        case sourceTable: self.Table.Source          => render(sourceTable.name)
        case Table.Joined(joinType, left, right, on) =>
          renderTable(left)
          render(joinType match {
            case JoinType.Inner      => " INNER JOIN "
            case JoinType.LeftOuter  => " LEFT JOIN "
            case JoinType.RightOuter => " RIGHT JOIN "
            case JoinType.FullOuter  => " OUTER JOIN "
          })
          renderTable(right)
          render(" ON ")
          renderExpr(on)
          render(" ")
      }
  }

}
