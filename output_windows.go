package mmdbconvert

import "os"

func outputFileMode(os.FileInfo) os.FileMode {
	return 0o600
}

func restoreOutputPermissions(*os.File, os.FileMode) error {
	return nil
}
