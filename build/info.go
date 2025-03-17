package build

import "runtime/debug"

type Info struct {
	Path       string `json:"path,omitempty"`
	Checksum   string `json:"checksum,omitempty"`
	CommitHash string `json:"commitHash,omitempty"`
	CommitTime string `json:"commitTime,omitempty"`
}

func GetBuildInfo() *Info {
	result := &Info{}

	if bi, ok := debug.ReadBuildInfo(); ok {
		result.Path = bi.Main.Path
		result.Checksum = bi.Main.Sum

		for _, s := range bi.Settings {
			switch s.Key {
			case "vcs.revision":
				result.CommitHash = s.Value
			case "vcs.time":
				result.CommitTime = s.Value
			}
		}
	}
	return result
}
