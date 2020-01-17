package schema

// Column represents a column in table
type Column struct {
	Name string
	Type string
}

// Table represents a table in database
type Table struct {
	Name    string
	Columns map[string]Column
}

type Db struct {
	Tables map[string]Table
}

func (s *Db) Load(schemaPath string) error {
	return s.LoadPostgres(schemaPath)
}

func NewDbSchema(schemaPath string) (*Db, error) {
	s := &Db{}
	s.Tables = make(map[string]Table)
	err := s.Load(schemaPath)
	if err != nil {
		return nil, err
	}
	return s, nil
}
