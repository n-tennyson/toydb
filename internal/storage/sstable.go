package storage

type SSTable struct {
	path string
}

func OpenSSTable(path string) (*SSTable, error) {
	return &SSTable{
		path: path,
	}, nil
}
