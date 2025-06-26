package nuodb

import (
	"database/sql"
	"fmt"

	_ "github.com/tilinna/go-nuodb"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/schema"
)

// Dialector implements gorm.Dialector interface for NuoDB
// using github.com/tilinna/go-nuodb driver.
type Dialector struct {
	DSN        string
	DriverName string
	Conn       gorm.ConnPool
}

// Open creates a new NuoDB dialector with given DSN.
func Open(dsn string) gorm.Dialector {
	return &Dialector{DSN: dsn, DriverName: "nuodb"}
}

// Name returns the dialect name.
func (d Dialector) Name() string {
	return "nuodb"
}

// Initialize sets up the database connection and config for GORM.
func (d Dialector) Initialize(db *gorm.DB) error {
	if d.DriverName == "" {
		d.DriverName = "nuodb"
	}

	if d.Conn != nil {
		db.ConnPool = d.Conn
	} else {
		conn, err := sql.Open(d.DriverName, d.DSN)
		if err != nil {
			return err
		}
		db.ConnPool = conn
	}

	// Setup common clauses
	db.Config.NamingStrategy = schema.NamingStrategy{}
	return nil
}

// Migrator returns the migrator for NuoDB.
func (d Dialector) Migrator(db *gorm.DB) gorm.Migrator {
	return &Migrator{db}
}

// DataTypeOf maps Go types to NuoDB data types.
func (d Dialector) DataTypeOf(field *schema.Field) string {
	switch field.DataType {
	case schema.Bool:
		return "boolean"
	case schema.Int, schema.Uint, schema.Int32, schema.Uint32:
		return "integer"
	case schema.Int64, schema.Uint64:
		return "bigint"
	case schema.Float32, schema.Float64:
		return "double"
	case schema.String:
		if field.Size > 0 && field.Size <= 4000 {
			return fmt.Sprintf("varchar(%d)", field.Size)
		}
		return "text"
	case schema.Time:
		return "timestamp"
	case schema.Bytes:
		return "blob"
	}
	return string(field.DataType)
}

// DefaultValueOf builds default value clause.
func (d Dialector) DefaultValueOf(field *schema.Field) clause.Expression {
	if field.DefaultValueInterface != nil {
		return clause.Expr{SQL: "?", Vars: []interface{}{field.DefaultValueInterface}}
	}
	return clause.Expr{SQL: ""}
}

// BindVarTo writes the variable placeholder to writer.
func (d Dialector) BindVarTo(writer clause.Writer, stmt *gorm.Statement, v interface{}) {
	writer.WriteByte('?')
}

// QuoteTo quotes identifiers for NuoDB.
func (d Dialector) QuoteTo(writer clause.Writer, str string) {
	writer.WriteByte('"')
	writer.WriteString(str)
	writer.WriteByte('"')
}

// Explain prints sql with variables.
func (d Dialector) Explain(sql string, vars ...interface{}) string {
	return gorm.Expr(sql, vars...).SQL
}

// SavePoint creates a savepoint with the given name.
func (d Dialector) SavePoint(tx *gorm.DB, name string) error {
	return tx.Exec("SAVEPOINT " + name).Error
}

// RollbackTo rolls back to the specified savepoint.
func (d Dialector) RollbackTo(tx *gorm.DB, name string) error {
	return tx.Exec("ROLLBACK TO SAVEPOINT " + name).Error
}
