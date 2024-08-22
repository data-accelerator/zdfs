package zdfs

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/containerd/accelerated-container-image/pkg/types"
	"github.com/containerd/containerd/v2/core/snapshots"
	"github.com/stretchr/testify/assert"
)

func TestParseRef(t *testing.T) {
	ref := "dadi-test-registry.cn-hangzhou.cr.aliyuncs.com/tuji/wordpress:20240303_containerd_accelerated"
	fmt.Println(constructImageBlobURL(ref))
}

func TestPrepareOverlayBDSpec(t *testing.T) {
	ctx := context.Background()

	testdir := "/tmp/zdfs-test/snapshot/"
	snPath := func(id string) string {
		return filepath.Join(testdir, id)
	}
	dgst := "sha256:0000000000000000000000000000000000000000000000000000000000000000"
	domain := "registry-1.docker.io"
	domainNew := "registry-2.docker.io"

	expectedConfig := func(repoBlobUrl string) types.OverlayBDBSConfig {
		return types.OverlayBDBSConfig{
			RepoBlobURL: repoBlobUrl,
			Lowers: []types.OverlayBDBSConfigLower{
				{
					File: overlaybdBaseLayer,
				},
				{
					Digest: dgst,
					Dir:    filepath.Join(snPath("0"), "block"),
				},
			},
			ResultFile: filepath.Join(overlaybdInitDebuglogPath(snPath("0"))),
		}
	}

	testcases := []struct {
		name     string
		imageRef string
		expected types.OverlayBDBSConfig
	}{
		{
			name:     "without image ref",
			imageRef: "",
			expected: expectedConfig(fmt.Sprintf("https://%s/v2/test/blobs", domain)),
		},
		{
			name:     "with image ref",
			imageRef: domainNew + "/test:latest",
			expected: expectedConfig(fmt.Sprintf("https://%s/v2/test/blobs", domainNew)),
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			os.RemoveAll(testdir)
			os.MkdirAll(filepath.Join(snPath("0"), "fs"), 0755)
			os.MkdirAll(filepath.Join(snPath("0"), "block"), 0755)

			// prepare snapshot dir (image_ref, .oss_url ...)
			files := []string{iNewFormat, zdfsChecksumFile, zdfsOssurlFile, zdfsOssDataSizeFile, zdfsOssTypeFile}
			for _, file := range files {
				if err := os.WriteFile(filepath.Join(snPath("0"), "fs", file), nil, 0644); err != nil {
					t.Fatal(err)
				}
			}
			ossURL := fmt.Sprintf("https://%s/v2/test/blobs/%s", domain, dgst)
			if err := os.WriteFile(filepath.Join(snPath("0"), "fs", zdfsOssurlFile), []byte(ossURL), 0644); err != nil {
				t.Fatal(err)
			}
			if tc.imageRef != "" {
				t.Logf("using image ref: %s", tc.imageRef)
				if err := os.WriteFile(filepath.Join(snPath("0"), "image_ref"), []byte(tc.imageRef), 0644); err != nil {
					t.Fatal(err)
				}
			}

			ok, err := PrepareOverlayBDSpec(ctx, "test-key", "0", snPath("0"), snapshots.Info{}, snPath)
			if err != nil {
				t.Fatal(err)
			}

			assert.True(t, ok)
			var config types.OverlayBDBSConfig
			b, err := os.ReadFile(overlaybdConfPath(snPath("0")))
			if err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(b, &config); err != nil {
				t.Fatal(err)
			}
			assert.Equal(t, tc.expected, config)

			out, _ := json.MarshalIndent(config, "", "  ")
			t.Logf("construct config: \n%s", out)
		})
	}
}
