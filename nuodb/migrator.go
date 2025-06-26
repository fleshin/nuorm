package nuodb

import (
	"gorm.io/gorm"
	"gorm.io/gorm/migrator"
)

// Migrator implements gorm.Migrator interface for NuoDB.
type Migrator struct {
	DB *gorm.DB
	migrator.Migrator
}

// CurrentDatabase returns the current database name.
func (m Migrator) CurrentDatabase() string {
	var name string
	m.DB.Raw("select current_schema").Scan(&name)
	return name
}
