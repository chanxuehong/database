package lru

import (
	"database/sql"
)

type Stmt struct {
	*sql.Stmt
}
