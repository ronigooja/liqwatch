package pack

import (
	"fmt"
	"os"
	"path/filepath"
)

// RecheckDeletion checks stored-byte metadata without rereading data. During
// recovery, missing members are allowed: a prior removal may have unlinked them.
// Extra or changed members always block, including during partial-removal recovery.
func RecheckDeletion(dir string, members []Member, recovering bool) error {
	st, err := os.Lstat(dir)
	if err != nil {
		return err
	}
	if !st.IsDir() || st.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("unsafe deletion directory %s", dir)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	want := make(map[string]Member, len(members))
	for _, m := range members {
		if !safe(m.Name) || m.MtimeNS == 0 {
			return fmt.Errorf("missing deletion evidence")
		}
		want[m.Name] = m
	}
	if len(want) == 0 {
		return fmt.Errorf("missing deletion evidence")
	}
	if !recovering && len(entries) != len(want) {
		return fmt.Errorf("source changed after verification: member set")
	}
	for _, e := range entries {
		m, ok := want[e.Name()]
		st, err := os.Lstat(filepath.Join(dir, e.Name()))
		if err != nil {
			return err
		}
		if !ok || !st.Mode().IsRegular() || st.Size() != m.Bytes || st.ModTime().UnixNano() != m.MtimeNS {
			return fmt.Errorf("source changed after verification: %s", e.Name())
		}
	}
	return nil
}
